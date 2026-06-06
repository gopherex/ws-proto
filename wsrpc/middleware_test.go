package wsrpc

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestChainOrder(t *testing.T) {
	var log []string
	tag := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, s *Stream) error {
				log = append(log, "enter:"+name)
				err := next(ctx, s)
				log = append(log, "exit:"+name)
				return err
			}
		}
	}
	final := Handler(func(ctx context.Context, s *Stream) error {
		log = append(log, "handler")
		return nil
	})
	// First listed runs outermost.
	require.NoError(t, chain(final, []Middleware{tag("a"), tag("b")})(context.Background(), nil))
	require.Equal(t, []string{"enter:a", "enter:b", "handler", "exit:b", "exit:a"}, log)
}

// dialWithServer spins up a real WS server with the given options and returns a client conn.
func dialWithServer(t *testing.T, register func(*Server), opts ...ServerOption) *ClientConn {
	t.Helper()
	srv := NewServer(opts...)
	register(srv)
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	cc, err := Dial(context.Background(), "ws"+strings.TrimPrefix(hs.URL, "http"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func TestMiddlewareRunsAndSeesMethod(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var seenMethod string

	tag := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, s *Stream) error {
				mu.Lock()
				order = append(order, "enter:"+name)
				seenMethod = s.Method()
				mu.Unlock()
				err := next(ctx, s)
				mu.Lock()
				order = append(order, "exit:"+name)
				mu.Unlock()
				return err
			}
		}
	}

	cc := dialWithServer(t, func(srv *Server) {
		srv.Register("/t/Echo", func(ctx context.Context, s *Stream) error {
			var v wrapperspb.StringValue
			if err := s.Recv(&v); err != nil && err != io.EOF {
				return err
			}
			mu.Lock()
			order = append(order, "handler")
			mu.Unlock()
			return s.Send(&wrapperspb.StringValue{Value: "ok"})
		})
	}, WithMiddleware(tag("a"), tag("b")))

	s, err := cc.NewStream(context.Background(), "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "hi"}))
	require.NoError(t, s.CloseSend())
	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, io.EOF, s.Recv(&wrapperspb.StringValue{}))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"enter:a", "enter:b", "handler", "exit:b", "exit:a"}, order)
	require.Equal(t, "/t/Echo", seenMethod)
}

func TestPanicInHandlerRecovered(t *testing.T) {
	cc := dialWithServer(t, func(srv *Server) {
		srv.Register("/t/Boom", func(ctx context.Context, s *Stream) error {
			panic("kaboom")
		})
	})
	s, err := cc.NewStream(context.Background(), "/t/Boom", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	st := FromError(err)
	require.Equal(t, codes.Internal, st.Code)
	require.Contains(t, st.Message, "panic")
}

func TestMiddlewareShortCircuits(t *testing.T) {
	var handlerRan bool
	deny := func(next Handler) Handler {
		return func(ctx context.Context, s *Stream) error {
			return Errorf(codes.PermissionDenied, "blocked")
		}
	}

	cc := dialWithServer(t, func(srv *Server) {
		srv.Register("/t/Guard", func(ctx context.Context, s *Stream) error {
			handlerRan = true
			return nil
		})
	}, WithMiddleware(deny))

	s, err := cc.NewStream(context.Background(), "/t/Guard", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	st := FromError(err)
	require.Equal(t, codes.PermissionDenied, st.Code)
	require.Equal(t, "blocked", st.Message)
	require.False(t, handlerRan)
}
