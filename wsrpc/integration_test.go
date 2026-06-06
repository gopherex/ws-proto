package wsrpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// harness wires a server mux + client conn over a pipe. Handlers keyed by method.
func harness(t *testing.T, handlers map[string]Handler) *ClientConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	srvEnd, cliEnd := newPipe()

	_ = newMux(ctx, srvEnd, func(s *Stream) {
		go func() {
			h := handlers[s.method]
			if h == nil {
				_ = s.end(&Status{Code: codes.Unimplemented}, nil)
				return
			}
			err := h(s.ctx, s)
			st := FromError(err)
			if st == nil {
				st = &Status{Code: codes.OK}
			}
			_ = s.end(st, nil)
		}()
	})

	return newClientConn(ctx, cliEnd)
}

func TestServerStream(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/SS": func(ctx context.Context, s *Stream) error {
			for i := 0; i < 3; i++ {
				if err := s.Send(&wrapperspb.Int32Value{Value: int32(i)}); err != nil {
					return err
				}
			}
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/SS", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	var got []int32
	for {
		var v wrapperspb.Int32Value
		err := s.Recv(&v)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, v.Value)
	}
	require.Equal(t, []int32{0, 1, 2}, got)
}

func TestClientStream(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/CS": func(ctx context.Context, s *Stream) error {
			var sum int32
			for {
				var v wrapperspb.Int32Value
				err := s.Recv(&v)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				sum += v.Value
			}
			return s.Send(&wrapperspb.Int32Value{Value: sum})
		},
	})

	s, err := cc.NewStream(ctx, "/t/CS", nil)
	require.NoError(t, err)
	for i := 1; i <= 4; i++ {
		require.NoError(t, s.Send(&wrapperspb.Int32Value{Value: int32(i)}))
	}
	require.NoError(t, s.CloseSend())

	var res wrapperspb.Int32Value
	require.NoError(t, s.Recv(&res))
	require.Equal(t, int32(10), res.Value)
}

func TestUnary(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/Unary": func(ctx context.Context, s *Stream) error {
			var req wrapperspb.StringValue
			if err := s.Recv(&req); err != nil {
				return err
			}
			return s.Send(&wrapperspb.StringValue{Value: "hi " + req.Value})
		},
	})

	s, err := cc.NewStream(ctx, "/t/Unary", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "bob"}))
	require.NoError(t, s.CloseSend())

	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "hi bob", res.Value)
	require.Equal(t, io.EOF, s.Recv(&wrapperspb.StringValue{}))
}

func TestBidi(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/BD": func(ctx context.Context, s *Stream) error {
			for {
				var v wrapperspb.StringValue
				err := s.Recv(&v)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if err := s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value}); err != nil {
					return err
				}
			}
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/BD", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "a"}))
	var r1 wrapperspb.StringValue
	require.NoError(t, s.Recv(&r1))
	require.Equal(t, "echo:a", r1.Value)
	require.NoError(t, s.CloseSend())
	require.Equal(t, io.EOF, s.Recv(&wrapperspb.StringValue{}))
}

func TestErrorStatus(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/Err": func(ctx context.Context, s *Stream) error {
			return Errorf(codes.PermissionDenied, "nope")
		},
	})
	s, err := cc.NewStream(ctx, "/t/Err", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	st := FromError(err)
	require.Equal(t, codes.PermissionDenied, st.Code)
	require.Equal(t, "nope", st.Message)
}

func TestUnknownMethod(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{})
	s, err := cc.NewStream(ctx, "/t/Missing", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	require.Equal(t, codes.Unimplemented, FromError(err).Code)
}

func TestDeadlineCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cc := harness(t, map[string]Handler{
		"/t/Hang": func(hctx context.Context, s *Stream) error {
			<-hctx.Done()
			return Errorf(codes.Canceled, "cancelled")
		},
	})
	s, err := cc.NewStream(ctx, "/t/Hang", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	require.Error(t, err)
}
