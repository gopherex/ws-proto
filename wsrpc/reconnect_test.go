package wsrpc

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// trackingListener wraps a net.Listener and records accepted conns so a test can
// forcibly drop the underlying TCP sockets — including hijacked WebSocket conns,
// which httptest's CloseClientConnections does NOT close (they're StateHijacked).
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	l.mu.Unlock()
	return c, nil
}

func (l *trackingListener) dropAll() {
	l.mu.Lock()
	conns := l.conns
	l.conns = nil
	l.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// TestInFlightFailsUnavailableOnDisconnect verifies that when the underlying
// socket is dropped mid-RPC, an in-flight Recv returns codes.Unavailable (not
// Canceled / Unknown). This holds without reconnect enabled.
func TestInFlightFailsUnavailableOnDisconnect(t *testing.T) {
	started := make(chan struct{})
	srv := NewServer(WithInsecureSkipOriginCheck(), WithKeepalive(0, 0))
	srv.Register("/t/Hang", func(ctx context.Context, s *Stream) error {
		close(started)
		<-ctx.Done() // never reply; hold the stream open
		return ctx.Err()
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tl := &trackingListener{Listener: ln}
	hs := &httptest.Server{
		Listener: tl,
		Config:   &http.Server{Handler: srv},
	}
	hs.Start()
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx := context.Background()
	cc, err := Dial(ctx, wsURL, WithDialKeepalive(0, 0))
	require.NoError(t, err)
	defer cc.Close()

	s, err := cc.NewStream(ctx, "/t/Hang", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "hi"}))

	<-started
	// Forcibly drop the underlying TCP socket(s).
	tl.dropAll()

	var res wrapperspb.StringValue
	err = s.Recv(&res)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, FromError(err).Code, "in-flight Recv should fail Unavailable on disconnect, got %v", err)
}

// TestUserCloseMapsCanceled verifies a deliberate Close surfaces codes.Canceled
// on in-flight streams (distinguishable from a transport drop).
func TestUserCloseMapsCanceled(t *testing.T) {
	started := make(chan struct{})
	srv := NewServer(WithInsecureSkipOriginCheck())
	srv.Register("/t/Hang", func(ctx context.Context, s *Stream) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx := context.Background()
	cc, err := Dial(ctx, wsURL)
	require.NoError(t, err)

	s, err := cc.NewStream(ctx, "/t/Hang", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "hi"}))
	<-started

	require.NoError(t, cc.Close())

	var res wrapperspb.StringValue
	err = s.Recv(&res)
	require.Error(t, err)
	require.Equal(t, codes.Canceled, FromError(err).Code, "in-flight Recv should fail Canceled on user Close, got %v", err)
}

// restartableServer wraps an http.Server bound to a fixed listener address that
// can be stopped and restarted on the same port, so reconnect can land on a
// fresh socket at the original URL.
type restartableServer struct {
	mu      sync.Mutex
	ln      net.Listener
	addr    string
	srv     *http.Server
	handler http.Handler
	wg      sync.WaitGroup
}

func newRestartableServer(t *testing.T, h http.Handler) *restartableServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	rs := &restartableServer{ln: ln, addr: ln.Addr().String(), handler: h}
	rs.serve(ln)
	return rs
}

func (rs *restartableServer) serve(ln net.Listener) {
	srv := &http.Server{Handler: rs.handler}
	rs.mu.Lock()
	rs.srv = srv
	rs.ln = ln
	rs.mu.Unlock()
	rs.wg.Add(1)
	go func() {
		defer rs.wg.Done()
		_ = srv.Serve(ln)
	}()
}

func (rs *restartableServer) url() string {
	return "ws://" + rs.addr
}

// stop closes the server and all its connections, freeing the port.
func (rs *restartableServer) stop() {
	rs.mu.Lock()
	srv := rs.srv
	rs.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	srv.Close()
	rs.wg.Wait()
}

// restart re-binds a listener on the original address and serves again.
func (rs *restartableServer) restart(t *testing.T) {
	t.Helper()
	var ln net.Listener
	var err error
	// The port may take a moment to free after Shutdown; retry briefly.
	deadline := time.Now().Add(2 * time.Second)
	for {
		ln, err = net.Listen("tcp", rs.addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "could not rebind %s", rs.addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rs.serve(ln)
}

func (rs *restartableServer) close() {
	rs.mu.Lock()
	srv := rs.srv
	rs.mu.Unlock()
	if srv != nil {
		srv.Close()
	}
	rs.wg.Wait()
}

// TestReconnectResumesService starts a server, runs an RPC, stops and restarts
// the server on the same port, then asserts a NEW RPC succeeds on the fresh
// connection installed by the reconnect controller.
func TestReconnectResumesService(t *testing.T) {
	echo := func() http.Handler {
		srv := NewServer(WithInsecureSkipOriginCheck(), WithKeepalive(0, 0))
		srv.Register("/t/Echo", func(ctx context.Context, s *Stream) error {
			var v wrapperspb.StringValue
			if err := s.Recv(&v); err != nil && err.Error() != "EOF" {
				// io.EOF is fine (client half-closed); other errors propagate.
			}
			return s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value})
		})
		return srv
	}
	rs := newRestartableServer(t, echo())
	defer rs.close()

	ctx := context.Background()
	cc, err := Dial(ctx, rs.url(),
		WithReconnect(WithBackoff(20*time.Millisecond, 100*time.Millisecond)),
		WithDialKeepalive(0, 0),
	)
	require.NoError(t, err)
	defer cc.Close()

	// First RPC on the original connection.
	s, err := cc.NewStream(ctx, "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "one"}))
	require.NoError(t, s.CloseSend())
	var r1 wrapperspb.StringValue
	require.NoError(t, s.Recv(&r1))
	require.Equal(t, "echo:one", r1.Value)

	// Drop and restart the server on the same address.
	rs.stop()
	rs.restart(t)

	// A new RPC must eventually succeed once reconnect installs a fresh mux.
	// NewStream blocks during the gap (or we retry until the socket is back).
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		s2, err := cc.NewStream(rctx, "/t/Echo", nil)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		_ = s2.Send(&wrapperspb.StringValue{Value: "two"})
		_ = s2.CloseSend()
		var r2 wrapperspb.StringValue
		err = s2.Recv(&r2)
		cancel()
		if err == nil && r2.Value == "echo:two" {
			return // success
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("reconnect did not resume service: lastErr=%v", lastErr)
}

// TestCloseStopsReconnect verifies Close during an active reconnect loop returns
// promptly and does not leak the controller goroutine. The server is down so the
// controller is stuck in its backoff/redial loop when Close fires.
func TestCloseStopsReconnect(t *testing.T) {
	srv := NewServer(WithInsecureSkipOriginCheck(), WithKeepalive(0, 0))
	srv.Register("/t/Echo", func(ctx context.Context, s *Stream) error { return nil })
	rs := newRestartableServer(t, srv)

	ctx := context.Background()
	cc, err := Dial(ctx, rs.url(),
		WithReconnect(WithBackoff(50*time.Millisecond, 200*time.Millisecond)),
		WithDialKeepalive(0, 0),
	)
	require.NoError(t, err)

	// Take the server down so the controller enters its retry loop.
	rs.stop()

	// Give the controller a moment to notice the drop and begin retrying.
	time.Sleep(80 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = cc.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return promptly during reconnect loop")
	}

	// After Close, NewStream must fail fast with Unavailable.
	_, err = cc.NewStream(ctx, "/t/Echo", nil)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, FromError(err).Code)
}
