package wsrpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestSubprotocolAndOptions verifies a server configured with WithOriginPatterns
// and a Dial with WithHeader complete a unary echo (subprotocol negotiated,
// proxy-visible upgrade options applied).
func TestSubprotocolAndOptions(t *testing.T) {
	srv := NewServer(WithOriginPatterns("*"))
	srv.Register("/t/Echo", func(ctx context.Context, s *Stream) error {
		var v wrapperspb.StringValue
		if err := s.Recv(&v); err != nil {
			return err
		}
		return s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value})
	})

	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx := context.Background()
	cc, err := Dial(ctx, wsURL, WithHeader(http.Header{"X-Test": {"1"}}))
	require.NoError(t, err)
	defer cc.Close()

	s, err := cc.NewStream(ctx, "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "hi"}))
	require.NoError(t, s.CloseSend())

	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "echo:hi", res.Value)
}

// TestReadLimitRejectsOversized verifies a tiny read limit fails the stream
// rather than buffering an arbitrarily large frame.
func TestReadLimitRejectsOversized(t *testing.T) {
	srv := NewServer(WithReadLimit(64))
	srv.Register("/t/Big", func(ctx context.Context, s *Stream) error {
		var v wrapperspb.StringValue
		// Reading the oversized frame should error (conn closed by read limit).
		return s.Recv(&v)
	})

	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx := context.Background()
	cc, err := Dial(ctx, wsURL)
	require.NoError(t, err)
	defer cc.Close()

	s, err := cc.NewStream(ctx, "/t/Big", nil)
	require.NoError(t, err)
	// Payload well above the 64-byte server read limit.
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: strings.Repeat("x", 4096)}))

	// The server-side read fails and the connection is torn down; the client's
	// Recv must therefore return an error rather than a valid response.
	err = s.Recv(&wrapperspb.StringValue{})
	require.Error(t, err)
}

// countingConn wraps a frameConn and counts Ping invocations.
type countingConn struct {
	frameConn
	pings int64
}

func (c *countingConn) Ping(ctx context.Context) error {
	atomic.AddInt64(&c.pings, 1)
	return c.frameConn.Ping(ctx)
}

// TestKeepalivePings verifies startKeepalive actually pings the conn.
func TestKeepalivePings(t *testing.T) {
	a, _ := newPipe()
	cc := &countingConn{frameConn: a}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := newMux(ctx, cc, nil)
	m.startKeepalive(20*time.Millisecond, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&cc.pings) >= 1
	}, 200*time.Millisecond, 5*time.Millisecond)

	m.cancel()
}

// TestConnContextAuthHeader proves the Upgrade request's headers reach the
// handler via WithConnContext + Stream.Context().
func TestConnContextAuthHeader(t *testing.T) {
	type authKey struct{}

	srv := NewServer(WithConnContext(func(ctx context.Context, r *http.Request) context.Context {
		return context.WithValue(ctx, authKey{}, r.Header.Get("Authorization"))
	}))
	srv.Register("/t/Auth", func(ctx context.Context, s *Stream) error {
		tok, _ := ctx.Value(authKey{}).(string)
		return s.Send(&wrapperspb.StringValue{Value: tok})
	})

	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx := context.Background()
	cc, err := Dial(ctx, wsURL, WithHeader(http.Header{"Authorization": {"Bearer t"}}))
	require.NoError(t, err)
	defer cc.Close()

	s, err := cc.NewStream(ctx, "/t/Auth", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "Bearer t", res.Value)
}
