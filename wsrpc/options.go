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
	// Subprotocol is the negotiated Sec-WebSocket-Protocol value.
	Subprotocol string = "wsrpc.v1"
)

type serverConfig struct {
	originPatterns   []string
	readLimit        int64
	keepalive        time.Duration
	keepaliveTimeout time.Duration
	receiveBuffer    int
	compression      CompressionMode
	connContext      func(ctx context.Context, r *http.Request) context.Context
	middleware       []Middleware
}

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

// WithOriginPatterns restricts accepted WebSocket Origin headers (CSRF defense).
func WithOriginPatterns(patterns ...string) ServerOption {
	return func(c *serverConfig) { c.originPatterns = patterns }
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

// WithReceiveBuffer bounds the per-stream inbound MSG queue. When a consumer is
// too slow and the buffer fills, that stream is reset (codes.ResourceExhausted)
// instead of blocking delivery to other streams on the connection. n<=0 keeps
// the default.
func WithReceiveBuffer(n int) ServerOption {
	return func(c *serverConfig) {
		if n > 0 {
			c.receiveBuffer = n
		}
	}
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

func defaultServerConfig() serverConfig {
	return serverConfig{
		readLimit:        defaultReadLimit,
		keepalive:        defaultKeepalive,
		keepaliveTimeout: defaultKeepaliveTimeout,
		receiveBuffer:    defaultReceiveBuffer,
	}
}

type dialConfig struct {
	header           http.Header
	readLimit        int64
	keepalive        time.Duration
	keepaliveTimeout time.Duration
	receiveBuffer    int
	compression      CompressionMode
	reconnect        reconnectConfig
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

// WithDialReceiveBuffer bounds the per-stream inbound MSG queue on the client.
// See WithReceiveBuffer. n<=0 keeps the default.
func WithDialReceiveBuffer(n int) DialOption {
	return func(c *dialConfig) {
		if n > 0 {
			c.receiveBuffer = n
		}
	}
}

// WithDialCompression sets the permessage-deflate negotiation mode on the client.
// Default CompressionDisabled.
func WithDialCompression(m CompressionMode) DialOption {
	return func(c *dialConfig) { c.compression = m }
}

func defaultDialConfig() dialConfig {
	return dialConfig{
		readLimit:        defaultReadLimit,
		keepalive:        defaultKeepalive,
		keepaliveTimeout: defaultKeepaliveTimeout,
		receiveBuffer:    defaultReceiveBuffer,
	}
}
