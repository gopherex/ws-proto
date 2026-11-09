package wsrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// revokedStatus is a terminal connection status carrying a protobuf detail, as
// an application ends a session.
func revokedStatus(t *testing.T) *Status {
	t.Helper()
	a, err := anypb.New(&wrapperspb.StringValue{Value: "TOKEN_REVOKED"})
	require.NoError(t, err)
	raw, err := proto.Marshal(a)
	require.NoError(t, err)
	return &Status{Code: codes.Unauthenticated, Message: "session ended", Details: [][]byte{raw}}
}

// requireRevoked asserts err is the revoked status with its detail intact.
func requireRevoked(t *testing.T, err error) {
	t.Helper()
	st := FromError(err)
	require.NotNil(t, st, "want a status, got nil")
	require.Equal(t, codes.Unauthenticated, st.Code, "status: %v", err)
	require.Equal(t, "session ended", st.Message)
	require.Len(t, st.Details, 1)
	var a anypb.Any
	require.NoError(t, proto.Unmarshal(st.Details[0], &a))
	v, err := a.UnmarshalNew()
	require.NoError(t, err)
	require.Equal(t, "TOKEN_REVOKED", v.(*wrapperspb.StringValue).GetValue())
}

// closingServer starts a WebSocket server whose handler /t/Hang holds its
// stream until the connection ends and then returns a late status that must
// never reach the client. Each accepted connection's handle is published on
// conns; accepted counts upgrades.
func closingServer(t *testing.T, started chan<- struct{}) (url string, conns <-chan *ServerConn, accepted *atomic.Int32) {
	t.Helper()
	handles := make(chan *ServerConn, 4)
	accepted = &atomic.Int32{}
	srv := NewServer(WithInsecureSkipOriginCheck(), WithKeepalive(0, 0),
		WithConnContext(func(ctx context.Context, _ *http.Request) context.Context {
			accepted.Add(1)
			sc, ok := ServerConnFromContext(ctx)
			require.True(t, ok, "connContext must see the connection handle")
			handles <- sc
			return ctx
		}))
	srv.Register("/t/Hang", func(ctx context.Context, _ *Stream) error {
		started <- struct{}{}
		<-ctx.Done()
		return Errorf(codes.Internal, "late handler status")
	})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return "ws" + strings.TrimPrefix(hs.URL, "http"), handles, accepted
}

func TestCloseWithStatusReachesStreamsAndStopsReconnect(t *testing.T) {
	t.Parallel()
	started := make(chan struct{}, 2)
	url, conns, accepted := closingServer(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cc, err := Dial(ctx, url, WithReconnect(WithBackoff(5*time.Millisecond, 20*time.Millisecond)))
	require.NoError(t, err)
	defer cc.Close()
	var streams []*Stream
	for range 2 {
		s, err := cc.NewStream(ctx, "/t/Hang", nil)
		require.NoError(t, err)
		streams = append(streams, s)
		<-started
	}

	require.NoError(t, (<-conns).CloseWithStatus(ctx, revokedStatus(t)))

	for _, s := range streams {
		requireRevoked(t, s.Recv(&wrapperspb.StringValue{}))
	}
	// A call after the end fails with the same status instead of redialing.
	require.Eventually(t, func() bool {
		s, err := cc.NewStream(ctx, "/t/Hang", nil)
		if err == nil {
			err = s.Recv(&wrapperspb.StringValue{})
		}
		st := FromError(err)
		return st != nil && st.Code == codes.Unauthenticated && len(st.Details) == 1
	}, 5*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // several backoff periods
	require.Equal(t, int32(1), accepted.Load(), "client redialed after the server ended the connection")
}

// A client that does not know CONN_END (or reads raw frames) still gets the
// status on each stream exactly once, then CONN_END, then close code 1008.
func TestCloseWithStatusWireOrder(t *testing.T) {
	t.Parallel()
	started := make(chan struct{}, 1)
	url, conns, _ := closingServer(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{Subprotocol}})
	require.NoError(t, err)
	defer c.CloseNow()
	conn := newWSConn(c, 1<<20)
	require.NoError(t, conn.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_OPEN, Method: "/t/Hang"}))
	<-started
	sc := <-conns
	closed := make(chan error, 1)
	go func() { closed <- sc.CloseWithStatus(ctx, revokedStatus(t)) }()

	var kinds []transport.Kind
	var readErr error
	for {
		f, err := conn.ReadFrame(ctx)
		if err != nil {
			readErr = err
			break
		}
		kinds = append(kinds, f.GetKind())
		requireRevoked(t, statusFromProto(f.GetStatus()))
	}
	require.Equal(t, []transport.Kind{transport.Kind_KIND_END, transport.Kind_KIND_CONN_END}, kinds)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(readErr), "close: %v", readErr)
	require.NoError(t, <-closed)
}

// ServeConn hands the same handle to handlers; a handler may end its own
// connection, and its own stream gets the status too.
func TestCloseWithStatusFromHandlerOverServeConn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := make(chan struct{}, 1)
	srv := NewServer()
	srv.Register("/t/Hang", func(ctx context.Context, _ *Stream) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	srv.Register("/t/End", func(ctx context.Context, _ *Stream) error {
		sc, ok := ServerConnFromContext(ctx)
		if !ok {
			return errors.New("no connection handle in the stream context")
		}
		return sc.CloseWithStatus(ctx, revokedStatus(t))
	})
	srvEnd, cliEnd := NewPipe()
	go srv.ServeConn(ctx, srvEnd)
	cc, err := DialConn(ctx, cliEnd)
	require.NoError(t, err)

	hang, err := cc.NewStream(ctx, "/t/Hang", nil)
	require.NoError(t, err)
	<-started
	end, err := cc.NewStream(ctx, "/t/End", nil)
	require.NoError(t, err)

	requireRevoked(t, hang.Recv(&wrapperspb.StringValue{}))
	requireRevoked(t, end.Recv(&wrapperspb.StringValue{}))
	_, err = cc.NewStream(ctx, "/t/Hang", nil)
	requireRevoked(t, err)
}

// An OPEN that races the close is refused with the connection's status.
func TestOpenWhileClosingGetsStatus(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srvEnd, cliEnd := newPipe()
	m := newMux(ctx, srvEnd, func(*Stream) { t.Error("a stream must not open on a closing connection") })
	m.mu.Lock()
	m.ending = revokedStatus(t)
	m.mu.Unlock()

	require.NoError(t, cliEnd.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_OPEN, Method: "/t/Hang"}))
	f, err := cliEnd.ReadFrame(ctx)
	require.NoError(t, err)
	require.Equal(t, transport.Kind_KIND_RST, f.GetKind())
	requireRevoked(t, statusFromProto(f.GetStatus()))
}

func TestCloseReason(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, in, want string }{
		{name: "short", in: "session ended", want: "session ended"},
		{name: "ascii over limit", in: strings.Repeat("a", 200), want: strings.Repeat("a", 123)},
		{name: "cut inside a rune", in: strings.Repeat("a", 122) + "ж", want: strings.Repeat("a", 122)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, closeReason(tc.in))
		})
	}
}

func BenchmarkCloseReason(b *testing.B) {
	msg := strings.Repeat("сессия завершена ", 20)
	for b.Loop() {
		_ = closeReason(msg)
	}
}
