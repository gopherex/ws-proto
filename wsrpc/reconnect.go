package wsrpc

import (
	"context"
	"math/rand"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
)

// Reconnect backoff defaults.
const (
	defaultReconnectInitial = 100 * time.Millisecond
	defaultReconnectMax     = 30 * time.Second
	reconnectFactor         = 2.0
)

// reconnectConfig holds the tunable backoff schedule for a reconnecting
// ClientConn. Enabled is set when WithReconnect is supplied.
type reconnectConfig struct {
	enabled bool
	initial time.Duration
	max     time.Duration
}

func defaultReconnectConfig() reconnectConfig {
	return reconnectConfig{
		initial: defaultReconnectInitial,
		max:     defaultReconnectMax,
	}
}

// ReconnectOption tunes the auto-reconnect backoff schedule.
type ReconnectOption func(*reconnectConfig)

// WithBackoff sets the initial and maximum backoff intervals for reconnect
// attempts. Non-positive values keep the defaults (100ms initial, 30s max).
func WithBackoff(initial, max time.Duration) ReconnectOption {
	return func(c *reconnectConfig) {
		if initial > 0 {
			c.initial = initial
		}
		if max > 0 {
			c.max = max
		}
	}
}

// WithReconnect enables opt-in client auto-reconnect on the returned ClientConn.
// On a transport disconnect the connection is redialed with exponential backoff
// and full jitter; in-flight streams still fail with codes.Unavailable and must
// be retried by the caller (there is no stream resume). Without this option the
// connection behaves exactly as today: a single socket that, on loss, fails all
// streams and stays down.
func WithReconnect(opts ...ReconnectOption) DialOption {
	return func(c *dialConfig) {
		c.reconnect = defaultReconnectConfig()
		c.reconnect.enabled = true
		for _, o := range opts {
			o(&c.reconnect)
		}
	}
}

// nextBackoff computes the next sleep for attempt n (0-based) under exponential
// growth capped at max, then applies full jitter (uniform in [0, d]).
func (c reconnectConfig) nextBackoff(attempt int, rng *rand.Rand) time.Duration {
	d := float64(c.initial)
	for i := 0; i < attempt; i++ {
		d *= reconnectFactor
		if d >= float64(c.max) {
			d = float64(c.max)
			break
		}
	}
	if d > float64(c.max) {
		d = float64(c.max)
	}
	// Full jitter: sleep a random amount in [0, d].
	return time.Duration(rng.Int63n(int64(d) + 1))
}

// reconnector watches a ClientConn's live mux and, on disconnect, redials with
// backoff and installs a fresh mux. It is created only when WithReconnect is set.
type reconnector struct {
	cc  *ClientConn
	url string
	cfg dialConfig
	rng *rand.Rand
}

// run blocks until the conn is user-closed. Each iteration waits for the active
// mux to terminate (ctx.Done), then — if the conn is still open — redials with
// backoff and installs the new mux, broadcasting to any NewStream callers
// waiting during the gap.
func (r *reconnector) run() {
	for {
		cc := r.cc
		cc.mu.Lock()
		mux := cc.mux
		closed := cc.closed
		cc.mu.Unlock()
		if closed {
			return
		}
		if mux == nil {
			return
		}
		// Wait for this mux to die (disconnect) or the conn to close.
		select {
		case <-mux.ctx.Done():
		case <-cc.closeCh:
			return
		}

		cc.mu.Lock()
		if cc.closed {
			cc.mu.Unlock()
			return
		}
		// Drop the dead mux so NewStream callers block until a fresh one lands.
		cc.mux = nil
		cc.mu.Unlock()

		newMux := r.redial()
		if newMux == nil {
			// redial returns nil only when the conn was closed mid-loop.
			return
		}
		cc.mu.Lock()
		if cc.closed {
			cc.mu.Unlock()
			_ = newMux.Close()
			return
		}
		cc.mux = newMux
		cc.broadcastLocked()
		cc.mu.Unlock()
	}
}

// redial attempts to reconnect with backoff until it succeeds or the conn is
// closed (returns nil). Each failed attempt sleeps per the jittered schedule.
func (r *reconnector) redial() *Mux {
	for attempt := 0; ; attempt++ {
		select {
		case <-r.cc.closeCh:
			return nil
		default:
		}
		mux, err := r.dialOnce()
		if err == nil {
			return mux
		}
		sleep := r.cfg.reconnect.nextBackoff(attempt, r.rng)
		t := time.NewTimer(sleep)
		select {
		case <-t.C:
		case <-r.cc.closeCh:
			t.Stop()
			return nil
		}
	}
}

// dialOnce performs a single websocket dial and wraps it in a fresh mux.
func (r *reconnector) dialOnce() (*Mux, error) {
	// Bound the dial by the conn's lifetime so a hung dial unblocks on Close.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-r.cc.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	c, _, err := websocket.Dial(ctx, r.url, &websocket.DialOptions{
		Subprotocols:    []string{Subprotocol},
		HTTPHeader:      r.cfg.header,
		CompressionMode: r.cfg.compression,
	})
	if err != nil {
		return nil, err
	}
	conn := newWSConn(c, r.cfg.readLimit)
	mux := newMuxConfig(context.Background(), conn, nil, r.cfg.receiveBuffer, r.cfg.initialWindow)
	mux.startKeepalive(r.cfg.keepalive, r.cfg.keepaliveTimeout)
	return mux, nil
}

// waitForMux blocks until a live mux is installed, the conn is closed, or ctx is
// done. It is called by NewStream when no live mux is currently available
// (during the reconnect gap). The caller must hold cc.mu on entry; waitForMux
// releases and re-acquires it around the wait.
func (cc *ClientConn) waitForMux(ctx context.Context) (*Mux, error) {
	for {
		if cc.closed {
			return nil, Errorf(codes.Unavailable, "wsrpc: connection closed")
		}
		if cc.mux != nil {
			return cc.mux, nil
		}
		ch := cc.waitCh // captured under lock; replaced on each broadcast
		cc.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			cc.mu.Lock()
			return nil, ctx.Err()
		case <-cc.closeCh:
			cc.mu.Lock()
			return nil, Errorf(codes.Unavailable, "wsrpc: connection closed")
		}
		cc.mu.Lock()
	}
}

// broadcastLocked wakes all NewStream waiters. Caller holds cc.mu.
func (cc *ClientConn) broadcastLocked() {
	close(cc.waitCh)
	cc.waitCh = make(chan struct{})
}
