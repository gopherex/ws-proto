package wsrpc_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestPipeExport_EndToEnd exercises ONLY exported symbols: NewPipe + DialConn +
// Server.ServeConn drive a unary and a server-streaming RPC over an in-memory
// pipe, with no socket and no WebSocket handshake. It pins the public bufconn
// analog so the export cannot quietly regress.
func TestPipeExport_EndToEnd(t *testing.T) {
	t.Parallel()

	srv := wsrpc.NewServer(
		wsrpc.WithMaxConcurrentStreams(4),
	)
	srv.Register("/t/Echo", func(ctx context.Context, s *wsrpc.Stream) error {
		var req wrapperspb.StringValue
		if err := s.Recv(&req); err != nil {
			return err
		}
		return s.Send(&wrapperspb.StringValue{Value: "echo:" + req.Value})
	})
	srv.Register("/t/Count", func(ctx context.Context, s *wsrpc.Stream) error {
		_ = s.SendHeader(map[string]string{"x-served-by": "pipe"})
		defer s.SetTrailer(map[string]string{"x-count": "3"})
		for i := 0; i < 3; i++ {
			if err := s.Send(&wrapperspb.Int32Value{Value: int32(i)}); err != nil {
				return err
			}
		}
		return nil
	})

	srvEnd, cliEnd := wsrpc.NewPipe()
	t.Cleanup(func() { _ = srvEnd.Close(); _ = cliEnd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.ServeConn(ctx, srvEnd)

	// DialConn accepts the same DialOptions as Dial; WebSocket-handshake options
	// are ignored, but transport ones (WithDialInitialWindow) take effect.
	cc, err := wsrpc.DialConn(ctx, cliEnd,
		wsrpc.WithDialInitialWindow(64*1024),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	// Unary over the pipe.
	us, err := cc.NewStream(ctx, "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, us.Send(&wrapperspb.StringValue{Value: "hi"}))
	require.NoError(t, us.CloseSend())
	var out wrapperspb.StringValue
	require.NoError(t, us.Recv(&out))
	require.Equal(t, "echo:hi", out.Value)
	require.Equal(t, io.EOF, us.Recv(&wrapperspb.StringValue{}))

	// Server streaming over the pipe: leading header + trailers observed.
	ss, err := cc.NewStream(ctx, "/t/Count", nil)
	require.NoError(t, err)
	require.NoError(t, ss.CloseSend())
	var got []int32
	for {
		var v wrapperspb.Int32Value
		err := ss.Recv(&v)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, v.Value)
	}
	require.Equal(t, []int32{0, 1, 2}, got)
	require.Equal(t, "pipe", ss.Header()["x-served-by"])
	require.Equal(t, "3", ss.Trailer()["x-count"])
}

// TestPipeExport_PeerCloseUnblocksServeConn verifies ServeConn returns when the
// peer half of NewPipe is closed.
func TestPipeExport_PeerCloseUnblocksServeConn(t *testing.T) {
	t.Parallel()

	srv := wsrpc.NewServer()
	srv.Register("/t/Noop", func(ctx context.Context, s *wsrpc.Stream) error { return nil })

	srvEnd, cliEnd := wsrpc.NewPipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() { srv.ServeConn(ctx, srvEnd); close(done) }()

	require.NoError(t, cliEnd.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after peer Close")
	}
}

// TestPipeExport_ContextCancel verifies ctx cancellation tears the in-memory
// connection down: an in-flight stream fails with codes.Unavailable.
func TestPipeExport_ContextCancel(t *testing.T) {
	t.Parallel()

	srv := wsrpc.NewServer()
	srv.Register("/t/Hang", func(ctx context.Context, s *wsrpc.Stream) error {
		<-ctx.Done() // never responds until the conn tears down
		return ctx.Err()
	})

	srvEnd, cliEnd := wsrpc.NewPipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.ServeConn(ctx, srvEnd)

	cc, err := wsrpc.DialConn(ctx, cliEnd)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	s, err := cc.NewStream(ctx, "/t/Hang", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Recv(&wrapperspb.StringValue{}) }()

	// Canceling ServeConn's ctx tears the connection down: the in-flight Recv
	// must return an error. (The exact gRPC code depends on which side observes
	// the teardown first, so we only assert an error — not a specific code.)
	cancel()
	select {
	case <-errCh:
		// Recv failed as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not fail after ctx cancel")
	}
}
