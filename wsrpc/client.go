package wsrpc

import (
	"context"

	"github.com/coder/websocket"
)

// ClientConn is a multiplexed client connection.
type ClientConn struct {
	mux *Mux
}

// Dial opens a WebSocket to url (ws:// or wss://) and returns a ClientConn.
func Dial(ctx context.Context, url string, opts ...DialOption) (*ClientConn, error) {
	cfg := defaultDialConfig()
	for _, o := range opts {
		o(&cfg)
	}
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
	return &ClientConn{mux: mux}, nil
}

// newClientConn wraps an existing frameConn (used by tests over a pipe).
func newClientConn(ctx context.Context, conn frameConn) *ClientConn {
	return &ClientConn{mux: newMux(ctx, conn, nil)}
}

// NewStream opens a new client stream for method with request headers.
func (cc *ClientConn) NewStream(ctx context.Context, method string, headers map[string]string) (*Stream, error) {
	return cc.mux.newClientStream(ctx, method, headers)
}

func (cc *ClientConn) Close() error { return cc.mux.Close() }
