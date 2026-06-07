package wsrpc

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ClientConn is a multiplexed client connection.
//
// By default it wraps a single *Mux: on connection loss every in-flight stream
// fails (codes.Unavailable) and the conn stays down. When opened with
// WithReconnect, a background controller redials with backoff and swaps in a
// fresh mux; the live mux is then guarded by mu and NewStream callers made
// during the reconnect gap block until a new socket is installed. There is no
// stream resume — callers must retry RPCs that failed on the dropped socket.
type ClientConn struct {
	// mu guards mux/closed and the reconnect wait machinery. When reconnect is
	// disabled the controller is nil; mux is set once at Dial and never swapped,
	// so the lock is uncontended on the hot path.
	mu     sync.Mutex
	mux    *Mux
	closed bool

	reconn  *reconnector  // nil unless WithReconnect
	waitCh  chan struct{} // broadcast: replaced (closed+remade) on each mux install
	closeCh chan struct{} // closed once on Close to unblock the controller/waiters
}

// Dial opens a WebSocket to url (ws:// or wss://) and returns a ClientConn.
func Dial(ctx context.Context, url string, opts ...DialOption) (*ClientConn, error) {
	cfg := defaultDialConfig()
	for _, o := range opts {
		o(&cfg)
	}
	mux, err := dialMux(ctx, url, cfg)
	if err != nil {
		return nil, err
	}
	cc := &ClientConn{
		mux:     mux,
		waitCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
	}
	if cfg.reconnect.enabled {
		cc.reconn = &reconnector{
			cc:  cc,
			url: url,
			cfg: cfg,
			rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		go cc.reconn.run()
	}
	return cc, nil
}

// dialMux performs one websocket dial and wraps it in a started mux.
func dialMux(ctx context.Context, url string, cfg dialConfig) (*Mux, error) {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols:    []string{Subprotocol},
		HTTPHeader:      cfg.header,
		CompressionMode: cfg.compression,
	})
	if err != nil {
		return nil, err
	}
	conn := newWSConn(c, cfg.readLimit)
	mux := newMuxBuffered(ctx, conn, nil, cfg.receiveBuffer)
	mux.startKeepalive(cfg.keepalive, cfg.keepaliveTimeout)
	return mux, nil
}

// newClientConn wraps an existing frameConn (used by tests over a pipe).
func newClientConn(ctx context.Context, conn frameConn) *ClientConn {
	return &ClientConn{
		mux:     newMux(ctx, conn, nil),
		waitCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
	}
}

// NewStream opens a new client stream for method with request headers. With
// reconnect enabled, if the connection is mid-reconnect (no live mux), it blocks
// until a fresh socket is installed, ctx is done, or the conn is closed.
func (cc *ClientConn) NewStream(ctx context.Context, method string, headers map[string]string) (*Stream, error) {
	cc.mu.Lock()
	mux := cc.mux
	if mux == nil {
		// Only reachable when reconnect is enabled and a redial is in progress.
		var err error
		mux, err = cc.waitForMux(ctx)
		if err != nil {
			cc.mu.Unlock()
			return nil, err
		}
	}
	cc.mu.Unlock()
	return mux.newClientStream(ctx, method, headers)
}

// Close stops any reconnection and closes the active mux. After Close, NewStream
// returns codes.Unavailable ("connection closed").
func (cc *ClientConn) Close() error {
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		return nil
	}
	cc.closed = true
	mux := cc.mux
	cc.mux = nil
	close(cc.closeCh)
	cc.broadcastLocked()
	cc.mu.Unlock()
	if mux != nil {
		return mux.Close()
	}
	return nil
}
