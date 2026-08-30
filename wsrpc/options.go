package wsrpc

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// CompressionMode controls per-message-deflate (RFC 7692) negotiation. It is
// re-exported from coder/websocket so callers need not import it directly.
// Default is CompressionDisabled — payloads are already protobuf (poorly
// compressible) and some proxies mishandle the deflate extension.
type CompressionMode = websocket.CompressionMode

const (
	CompressionDisabled          = websocket.CompressionDisabled
	CompressionContextTakeover   = websocket.CompressionContextTakeover
	CompressionNoContextTakeover = websocket.CompressionNoContextTakeover
)

const (
	defaultReadLimit        int64         = 16 << 20 // 16 MiB
	defaultKeepalive        time.Duration = 20 * time.Second
	defaultKeepaliveTimeout time.Duration = 10 * time.Second
	// defaultReceiveBuffer bounds the per-stream inbound MSG queue. A slow
	// consumer that fills this buffer is reset rather than blocking the shared
	// read loop (head-of-line blocking / memory safety valve).
	defaultReceiveBuffer int = 256
	// defaultInitialWindow is the per-stream credit window (bytes) each peer
	// assumes for both send and receive directions with no handshake. A sender's
	// Send blocks once it would exceed the available send window; the receiver
	// returns credit as it consumes MSGs. See "Flow control" in the README.
	defaultInitialWindow int = 256 * 1024 // 256 KiB
	// defaultMaxStreams bounds the number of concurrent server streams accepted on
	// one connection. Each OPEN beyond this is rejected with an RST
	// (codes.ResourceExhausted) instead of allocating a handler goroutine, so a
	// single client cannot exhaust server memory/goroutines. See
	// WithMaxConcurrentStreams to tune or disable it.
	defaultMaxStreams int = 1000
	// Subprotocol is the negotiated Sec-WebSocket-Protocol value.
	Subprotocol string = "wsrpc.v1"
)

type serverConfig struct {
	originPatterns     []string
	insecureSkipOrigin bool
	readLimit          int64
	keepalive          time.Duration
	keepaliveTimeout   time.Duration
	receiveBuffer      int
	initialWindow      int
	maxStreams         int
	compression        CompressionMode
	connContext        func(ctx context.Context, r *http.Request) context.Context
	middleware         []Middleware
	stats              *ServerStats
	unknownHandler     Handler
}

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

// WithOriginPatterns restricts accepted WebSocket Origin headers (CSRF defense).
// Configuring it satisfies the fail-closed origin gate (see NewServer). Pass "*"
// to match any origin (equivalent to disabling the browser Origin check).
func WithOriginPatterns(patterns ...string) ServerOption {
	return func(c *serverConfig) { c.originPatterns = patterns }
}

// WithInsecureSkipOriginCheck disables the WebSocket Origin check entirely,
// accepting upgrades from ANY origin. It is the explicit opt-out required to run
// without WithOriginPatterns (the server otherwise rejects every upgrade — see
// NewServer). Only safe when cross-origin access is intended OR auth is enforced
// on the Upgrade request (e.g. via WithConnContext); otherwise it exposes the
// server to cross-site WebSocket hijacking.
func WithInsecureSkipOriginCheck() ServerOption {
	return func(c *serverConfig) { c.insecureSkipOrigin = true }
}

// WithKeepalive sets the server ping interval and pong timeout. interval<=0 disables pinging.
func WithKeepalive(interval, timeout time.Duration) ServerOption {
	return func(c *serverConfig) {
		c.keepalive = interval
		c.keepaliveTimeout = timeout
	}
}

// WithReadLimit caps the size of an inbound message in bytes.
func WithReadLimit(n int64) ServerOption {
	return func(c *serverConfig) { c.readLimit = n }
}

// WithReceiveBuffer is DEPRECATED and has no effect. The per-stream inbound
// backlog is now bounded by BYTES, aligned with the flow-control window (see
// WithInitialWindow), rather than by a fixed frame count — so a peer that obeys
// the window is never falsely reset. Retained for API compatibility.
func WithReceiveBuffer(n int) ServerOption {
	return func(c *serverConfig) {
		if n > 0 {
			c.receiveBuffer = n
		}
	}
}

// WithInitialWindow sets the per-stream credit window (bytes) for flow control.
// A sender blocks in Send once a MSG would exceed the available send window and
// resumes as the peer returns credit (KIND_WINDOW_UPDATE) by consuming MSGs.
// Both peers must assume the same value (there is no handshake); n<=0 keeps the
// default (256 KiB).
func WithInitialWindow(n int) ServerOption {
	return func(c *serverConfig) {
		if n > 0 {
			c.initialWindow = n
		}
	}
}

// WithMaxConcurrentStreams caps the number of simultaneously-open server streams
// on a single connection. Each OPEN past the cap is answered with an RST
// (codes.ResourceExhausted) and never reaches a handler, bounding goroutine and
// memory use against a misbehaving or hostile client. The default is 1000; n<=0
// disables the cap (unlimited — not recommended for untrusted clients).
func WithMaxConcurrentStreams(n int) ServerOption {
	return func(c *serverConfig) { c.maxStreams = n }
}

// WithConnContext lets handlers read the Upgrade HTTP request (auth headers, X-Forwarded-For)
// by deriving the per-connection base context; values are visible via Stream.Context().
func WithConnContext(fn func(ctx context.Context, r *http.Request) context.Context) ServerOption {
	return func(c *serverConfig) { c.connContext = fn }
}

// WithCompression sets the permessage-deflate negotiation mode on the server.
// Default CompressionDisabled.
func WithCompression(m CompressionMode) ServerOption {
	return func(c *serverConfig) { c.compression = m }
}

// WithMiddleware appends server middleware applied to every RPC. The first
// middleware passed runs outermost. Multiple calls accumulate in order.
func WithMiddleware(mw ...Middleware) ServerOption {
	return func(c *serverConfig) { c.middleware = append(c.middleware, mw...) }
}

// WithUnknownHandler installs a fallback Handler invoked for any method that has
// no registered handler, instead of answering codes.Unimplemented. Middleware
// wraps it exactly as it wraps registered handlers. The handler sees the
// requested method via Stream.Method() and typically relays frames verbatim with
// Stream.RecvRaw/SendRaw — this is the building block for a proto-agnostic proxy.
func WithUnknownHandler(h Handler) ServerOption {
	return func(c *serverConfig) { c.unknownHandler = h }
}

// WithStats installs a ServerStats observer for transport-level events
// (connection lifecycle, rejections, per-message flow) that per-RPC middleware
// cannot see. Callbacks must be fast and non-blocking — see ServerStats.
func WithStats(stats *ServerStats) ServerOption {
	return func(c *serverConfig) { c.stats = stats }
}

func defaultServerConfig() serverConfig {
	return serverConfig{
		readLimit:        defaultReadLimit,
		keepalive:        defaultKeepalive,
		keepaliveTimeout: defaultKeepaliveTimeout,
		receiveBuffer:    defaultReceiveBuffer,
		initialWindow:    defaultInitialWindow,
		maxStreams:       defaultMaxStreams,
	}
}

type dialConfig struct {
	header           http.Header
	readLimit        int64
	keepalive        time.Duration
	keepaliveTimeout time.Duration
	receiveBuffer    int
	initialWindow    int
	compression      CompressionMode
	reconnect        reconnectConfig
	interceptors     []Interceptor
}

// DialOption configures a client Dial.
type DialOption func(*dialConfig)

// WithHeader sets HTTP headers on the WebSocket Upgrade request (auth, cookies) — visible to proxies.
func WithHeader(h http.Header) DialOption {
	return func(c *dialConfig) { c.header = h }
}

// WithDialKeepalive sets the client ping interval and pong timeout. interval<=0 disables pinging.
func WithDialKeepalive(interval, timeout time.Duration) DialOption {
	return func(c *dialConfig) {
		c.keepalive = interval
		c.keepaliveTimeout = timeout
	}
}

// WithDialReadLimit caps the size of an inbound message in bytes.
func WithDialReadLimit(n int64) DialOption {
	return func(c *dialConfig) { c.readLimit = n }
}

// WithDialReceiveBuffer is DEPRECATED and has no effect. See WithReceiveBuffer:
// the per-stream inbound backlog is now byte-bounded by the flow-control window.
func WithDialReceiveBuffer(n int) DialOption {
	return func(c *dialConfig) {
		if n > 0 {
			c.receiveBuffer = n
		}
	}
}

// WithDialInitialWindow sets the per-stream credit window (bytes) for flow
// control on the client. See WithInitialWindow; n<=0 keeps the default (256 KiB).
func WithDialInitialWindow(n int) DialOption {
	return func(c *dialConfig) {
		if n > 0 {
			c.initialWindow = n
		}
	}
}

// WithDialCompression sets the permessage-deflate negotiation mode on the client.
// Default CompressionDisabled.
func WithDialCompression(m CompressionMode) DialOption {
	return func(c *dialConfig) { c.compression = m }
}

// WithClientInterceptor installs client interceptors, applied to every typed
// call made through a generated client. The first interceptor is the outermost.
// Interceptors can read/modify request metadata and messages, observe/transform
// responses, short-circuit a call, and read trailers — across all method kinds.
func WithClientInterceptor(interceptors ...Interceptor) DialOption {
	return func(c *dialConfig) { c.interceptors = append(c.interceptors, interceptors...) }
}

func defaultDialConfig() dialConfig {
	return dialConfig{
		readLimit:        defaultReadLimit,
		keepalive:        defaultKeepalive,
		keepaliveTimeout: defaultKeepaliveTimeout,
		receiveBuffer:    defaultReceiveBuffer,
		initialWindow:    defaultInitialWindow,
	}
}
