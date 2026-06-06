package wsrpc

import (
	"context"
	"net/http"
	"time"
)

const (
	defaultReadLimit        int64         = 16 << 20 // 16 MiB
	defaultKeepalive        time.Duration = 20 * time.Second
	defaultKeepaliveTimeout time.Duration = 10 * time.Second
	// Subprotocol is the negotiated Sec-WebSocket-Protocol value.
	Subprotocol string = "wsrpc.v1"
)

type serverConfig struct {
	originPatterns   []string
	readLimit        int64
	keepalive        time.Duration
	keepaliveTimeout time.Duration
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

// WithConnContext lets handlers read the Upgrade HTTP request (auth headers, X-Forwarded-For)
// by deriving the per-connection base context; values are visible via Stream.Context().
func WithConnContext(fn func(ctx context.Context, r *http.Request) context.Context) ServerOption {
	return func(c *serverConfig) { c.connContext = fn }
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
	}
}

type dialConfig struct {
	header           http.Header
	readLimit        int64
	keepalive        time.Duration
	keepaliveTimeout time.Duration
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

func defaultDialConfig() dialConfig {
	return dialConfig{
		readLimit:        defaultReadLimit,
		keepalive:        defaultKeepalive,
		keepaliveTimeout: defaultKeepaliveTimeout,
	}
}
