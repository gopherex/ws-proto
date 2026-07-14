package wsrpc

import (
	"context"

	"github.com/coder/websocket"
	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/grpc/codes"
)

// verifySubprotocol fails the connection unless the server selected the wsrpc
// subprotocol during the handshake. A mismatch (empty or other value) means the
// peer — or an intermediary proxy that stripped Sec-WebSocket-Protocol — is not
// speaking this framing protocol, so every frame would be mis-interpreted.
func verifySubprotocol(c *websocket.Conn) error {
	if got := c.Subprotocol(); got != Subprotocol {
		_ = c.Close(websocket.StatusProtocolError, "wsrpc: subprotocol not negotiated")
		return Errorf(codes.Unavailable, "wsrpc: server did not negotiate subprotocol %q (got %q)", Subprotocol, got)
	}
	return nil
}

// wsConn adapts a coder/websocket connection to FrameConn.
type wsConn struct {
	c *websocket.Conn
}

func newWSConn(c *websocket.Conn, readLimit int64) *wsConn {
	c.SetReadLimit(readLimit)
	return &wsConn{c: c}
}

func (w *wsConn) WriteFrame(ctx context.Context, f *transport.Frame) error {
	b, err := marshalFrame(f)
	if err != nil {
		return err
	}
	return w.c.Write(ctx, websocket.MessageBinary, b)
}

func (w *wsConn) ReadFrame(ctx context.Context) (*transport.Frame, error) {
	_, b, err := w.c.Read(ctx)
	if err != nil {
		return nil, err
	}
	return unmarshalFrame(b)
}

func (w *wsConn) Ping(ctx context.Context) error {
	return w.c.Ping(ctx)
}

func (w *wsConn) Close() error {
	return w.c.Close(websocket.StatusNormalClosure, "")
}
