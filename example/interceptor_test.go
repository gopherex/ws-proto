package example_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestClientUnaryInterceptorEndToEnd drives a unary call through the generated
// client with a client interceptor that injects an auth header; the server
// observes it via middleware.
func TestClientUnaryInterceptorEndToEnd(t *testing.T) {
	gotAuth := make(chan string, 1)
	mw := func(next wsrpc.Handler) wsrpc.Handler {
		return func(ctx context.Context, stream *wsrpc.Stream) error {
			gotAuth <- stream.Header()["authorization"]
			return next(ctx, stream)
		}
	}
	srv := wsrpc.NewServer(wsrpc.WithInsecureSkipOriginCheck(), wsrpc.WithMiddleware(mw))
	echov1.RegisterEchoServiceHandler(srv, impl{})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ran := false
	icept := wsrpc.UnaryInterceptorFunc(func(next wsrpc.UnaryFunc) wsrpc.UnaryFunc {
		return func(ctx context.Context, req *wsrpc.AnyRequest) (*wsrpc.AnyResponse, error) {
			ran = true
			req.Header["authorization"] = "Bearer xyz"
			return next(ctx, req)
		}
	})

	cc, err := wsrpc.Dial(context.Background(), wsURL, wsrpc.WithClientInterceptor(icept))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	client := echov1.NewEchoServiceWSClient(cc)

	res, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "bob"})
	require.NoError(t, err)
	require.True(t, ran, "interceptor did not run")
	require.Equal(t, "hello bob", res.Greeting)

	select {
	case got := <-gotAuth:
		require.Equal(t, "Bearer xyz", got)
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the interceptor-injected header")
	}
}

// TestClientStreamingInterceptorEndToEnd drives a server-streaming call through
// the generated client with a streaming interceptor that observes each received
// message and injects a request header.
func TestClientStreamingInterceptorEndToEnd(t *testing.T) {
	gotAuth := make(chan string, 1)
	mw := func(next wsrpc.Handler) wsrpc.Handler {
		return func(ctx context.Context, stream *wsrpc.Stream) error {
			select {
			case gotAuth <- stream.Header()["authorization"]:
			default:
			}
			return next(ctx, stream)
		}
	}
	srv := wsrpc.NewServer(wsrpc.WithInsecureSkipOriginCheck(), wsrpc.WithMiddleware(mw))
	echov1.RegisterEchoServiceHandler(srv, impl{})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	received := 0
	icept := wsrpc.StreamInterceptorFunc(func(next wsrpc.StreamingClientFunc) wsrpc.StreamingClientFunc {
		return func(ctx context.Context, spec wsrpc.MethodSpec, header map[string]string) (wsrpc.StreamingClientConn, error) {
			header["authorization"] = "Bearer stream"
			conn, err := next(ctx, spec, header)
			if err != nil {
				return nil, err
			}
			return &countingExampleConn{StreamingClientConn: conn, received: &received}, nil
		}
	})

	cc, err := wsrpc.Dial(context.Background(), wsURL, wsrpc.WithClientInterceptor(icept))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	client := echov1.NewEchoServiceWSClient(cc)

	stream, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 3})
	require.NoError(t, err)
	n := 0
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
		n++
	}
	require.Equal(t, 3, n)
	require.Equal(t, 3, received, "streaming interceptor did not observe each message")

	select {
	case got := <-gotAuth:
		require.Equal(t, "Bearer stream", got)
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the interceptor-injected header")
	}
}

// countingExampleConn counts received messages through a wrapped streaming conn.
type countingExampleConn struct {
	wsrpc.StreamingClientConn
	received *int
}

func (c *countingExampleConn) Receive(m proto.Message) error {
	err := c.StreamingClientConn.Receive(m)
	if err == nil {
		*c.received++
	}
	return err
}
