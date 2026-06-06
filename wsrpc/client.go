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
func Dial(ctx context.Context, url string) (*ClientConn, error) {
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	conn := newWSConn(c)
	return &ClientConn{mux: newMux(ctx, conn, nil)}, nil
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
