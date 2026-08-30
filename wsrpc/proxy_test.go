package wsrpc_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestProxy_UnknownHandlerRawRelay pins the proto-agnostic proxy building
// blocks: WithUnknownHandler + Stream.RecvRaw/SendRaw. A front server with NO
// registered handlers relays every stream verbatim to a backend server over a
// second pipe, and a client talking to the front must observe backend semantics
// exactly: unary responses, bidi ordering, error status with details, trailers.
func TestProxy_UnknownHandlerRawRelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Backend: real handlers.
	backend := wsrpc.NewServer()
	backend.Register("/t/Echo", func(ctx context.Context, s *wsrpc.Stream) error {
		var req wrapperspb.StringValue
		if err := s.Recv(&req); err != nil {
			return err
		}
		return s.Send(&wrapperspb.StringValue{Value: "echo:" + req.Value})
	})
	backend.Register("/t/Bidi", func(ctx context.Context, s *wsrpc.Stream) error {
		defer s.SetTrailer(map[string]string{"x-bidi": "done"})
		for {
			var req wrapperspb.Int32Value
			if err := s.Recv(&req); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			if err := s.Send(&wrapperspb.Int32Value{Value: req.Value * 2}); err != nil {
				return err
			}
		}
	})
	backend.Register("/t/Fail", func(ctx context.Context, s *wsrpc.Stream) error {
		var req wrapperspb.StringValue
		if err := s.Recv(&req); err != nil {
			return err
		}
		detail, err := anypb.New(&wrapperspb.StringValue{Value: "detail:" + req.Value})
		if err != nil {
			return err
		}
		raw, err := proto.Marshal(detail)
		if err != nil {
			return err
		}
		return &wsrpc.Status{Code: codes.FailedPrecondition, Message: "nope", Details: [][]byte{raw}}
	})

	backendSrvEnd, backendCliEnd := wsrpc.NewPipe()
	t.Cleanup(func() { _ = backendSrvEnd.Close(); _ = backendCliEnd.Close() })
	go backend.ServeConn(ctx, backendSrvEnd)

	upstream, err := wsrpc.DialConn(ctx, backendCliEnd)
	require.NoError(t, err)
	t.Cleanup(func() { _ = upstream.Close() })

	// Front: zero handlers, relays raw frames for any method.
	front := wsrpc.NewServer(wsrpc.WithUnknownHandler(rawRelay(upstream)))

	frontSrvEnd, frontCliEnd := wsrpc.NewPipe()
	t.Cleanup(func() { _ = frontSrvEnd.Close(); _ = frontCliEnd.Close() })
	go front.ServeConn(ctx, frontSrvEnd)

	cc, err := wsrpc.DialConn(ctx, frontCliEnd)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	t.Run("unary", func(t *testing.T) {
		s, err := cc.NewStream(ctx, "/t/Echo", nil)
		require.NoError(t, err)
		require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "hi"}))
		require.NoError(t, s.CloseSend())

		var res wrapperspb.StringValue
		require.NoError(t, s.Recv(&res))
		require.Equal(t, "echo:hi", res.Value)
		require.ErrorIs(t, s.Recv(&res), io.EOF)
	})

	t.Run("bidi", func(t *testing.T) {
		s, err := cc.NewStream(ctx, "/t/Bidi", nil)
		require.NoError(t, err)
		for i := int32(1); i <= 3; i++ {
			require.NoError(t, s.Send(&wrapperspb.Int32Value{Value: i}))
			var res wrapperspb.Int32Value
			require.NoError(t, s.Recv(&res))
			require.Equal(t, i*2, res.Value)
		}
		require.NoError(t, s.CloseSend())
		var res wrapperspb.Int32Value
		require.ErrorIs(t, s.Recv(&res), io.EOF)
		require.Equal(t, "done", s.Trailer()["x-bidi"])
	})

	t.Run("error status with details", func(t *testing.T) {
		s, err := cc.NewStream(ctx, "/t/Fail", nil)
		require.NoError(t, err)
		require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "x"}))
		require.NoError(t, s.CloseSend())

		var res wrapperspb.StringValue
		recvErr := s.Recv(&res)
		require.Error(t, recvErr)

		st := wsrpc.FromError(recvErr)
		require.Equal(t, codes.FailedPrecondition, st.Code)
		require.Equal(t, "nope", st.Message)
		require.Len(t, st.Details, 1)

		var a anypb.Any
		require.NoError(t, proto.Unmarshal(st.Details[0], &a))
		var detail wrapperspb.StringValue
		require.NoError(t, a.UnmarshalTo(&detail))
		require.Equal(t, "detail:x", detail.Value)
	})

	t.Run("unregistered on backend too", func(t *testing.T) {
		s, err := cc.NewStream(ctx, "/t/Nowhere", nil)
		require.NoError(t, err)
		require.NoError(t, s.CloseSend())

		var res wrapperspb.StringValue
		st := wsrpc.FromError(s.Recv(&res))
		require.Equal(t, codes.Unimplemented, st.Code)
	})
}

// rawRelay proxies one downstream stream onto a fresh upstream stream of the
// same method, copying payloads verbatim in both directions — the shape a
// gateway uses in production (there the upstream side is gRPC).
func rawRelay(upstream *wsrpc.ClientConn) wsrpc.Handler {
	return func(ctx context.Context, down *wsrpc.Stream) error {
		up, err := upstream.NewStream(ctx, down.Method(), down.Header())
		if err != nil {
			return err
		}

		clientDone := make(chan error, 1)
		go func() {
			for {
				b, err := down.RecvRaw()
				if errors.Is(err, io.EOF) {
					clientDone <- up.CloseSend()
					return
				}
				if err != nil {
					clientDone <- err
					return
				}
				if err := up.SendRaw(b); err != nil {
					clientDone <- err
					return
				}
			}
		}()

		for {
			b, err := up.RecvRaw()
			if errors.Is(err, io.EOF) {
				down.SetTrailer(up.Trailer())
				return nil
			}
			if err != nil {
				return err
			}
			if err := down.SendRaw(b); err != nil {
				return err
			}
		}
	}
}
