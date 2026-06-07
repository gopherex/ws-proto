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
)

// TestCallHeaderCrossesWire verifies a per-call header set via
// wsrpc.WithCallHeader reaches the server-side stream (read via middleware).
func TestCallHeaderCrossesWire(t *testing.T) {
	gotCh := make(chan string, 1)
	mw := func(next wsrpc.Handler) wsrpc.Handler {
		return func(ctx context.Context, stream *wsrpc.Stream) error {
			gotCh <- stream.Header()["x-tenant"]
			return next(ctx, stream)
		}
	}
	srv := wsrpc.NewServer(wsrpc.WithMiddleware(mw))
	echov1.RegisterEchoServiceHandler(srv, impl{})

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	client := echov1.NewEchoServiceWSClient(cc)

	res, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "bob"},
		wsrpc.WithCallHeader("x-tenant", "acme"))
	require.NoError(t, err)
	require.Equal(t, "hello bob", res.Greeting)

	select {
	case got := <-gotCh:
		require.Equal(t, "acme", got)
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the call header")
	}
}

// TestDeadlineCrossesWire verifies the client's ctx deadline is propagated over
// the wire (ws-timeout-ms) and applied to the server handler's context.
func TestDeadlineCrossesWire(t *testing.T) {
	type result struct {
		ok   bool
		left time.Duration
	}
	resCh := make(chan result, 1)
	mw := func(next wsrpc.Handler) wsrpc.Handler {
		return func(ctx context.Context, stream *wsrpc.Stream) error {
			d, ok := ctx.Deadline()
			resCh <- result{ok: ok, left: time.Until(d)}
			return next(ctx, stream)
		}
	}
	srv := wsrpc.NewServer(wsrpc.WithMiddleware(mw))
	echov1.RegisterEchoServiceHandler(srv, impl{})

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	client := echov1.NewEchoServiceWSClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = client.Unary(ctx, &echov1.UnaryRequest{Name: "bob"})
	require.NoError(t, err)

	select {
	case got := <-resCh:
		require.True(t, got.ok, "server handler must have a deadline")
		require.Greater(t, got.left, 10*time.Millisecond)
		require.LessOrEqual(t, got.left, 100*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("server never reported its deadline")
	}
}
