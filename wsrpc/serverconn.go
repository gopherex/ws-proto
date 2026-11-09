package wsrpc

import (
	"context"
	"errors"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
)

// ServerConn is the server's handle on one client connection. The server puts
// it into the connection context before WithConnContext runs, so the hook,
// middleware and handlers all reach it through ServerConnFromContext.
type ServerConn struct {
	mu      sync.Mutex
	mux     *Mux
	pending *Status // CloseWithStatus called before the mux was attached
	// ended is closed once the status frames are written: the connection's
	// owner (ServeHTTP / ServeConn) then closes the socket.
	ended     chan struct{}
	markEnded func()
}

type serverConnKey struct{}

// ServerConnFromContext returns the connection handle carried by a connection
// or stream context on the server.
func ServerConnFromContext(ctx context.Context) (*ServerConn, bool) {
	sc, ok := ctx.Value(serverConnKey{}).(*ServerConn)
	return sc, ok
}

func withServerConn(ctx context.Context) (context.Context, *ServerConn) {
	ended := make(chan struct{})
	sc := &ServerConn{ended: ended, markEnded: sync.OnceFunc(func() { close(ended) })}
	return context.WithValue(ctx, serverConnKey{}, sc), sc
}

// errCloseOK rejects a close without a reason: the client would have no status
// to report.
var errCloseOK = errors.New("wsrpc: CloseWithStatus needs a non-OK status")

// CloseWithStatus ends the connection so the client learns why: every live
// stream ends with st (code, message and details), then the client gets the
// connection-level status, which fails its later calls with st and stops its
// reconnect, then the socket closes with code 1008 (policy violation) and the
// message as reason. Handlers see their context cancelled; a status they return
// afterwards is not sent. ctx bounds the writes and may be a handler's own
// stream context; the connection closes even if the writes fail. Calls after
// the first do nothing.
func (c *ServerConn) CloseWithStatus(ctx context.Context, st *Status) error {
	if st == nil || st.Code == codes.OK {
		return errCloseOK
	}
	c.mu.Lock()
	m := c.mux
	if m == nil {
		if c.pending == nil {
			c.pending = st
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	defer c.markEnded()
	return m.closeWithStatus(ctx, st)
}

// attach binds the connection's mux and applies a close requested before it
// existed (for example from the WithConnContext hook). A fresh connection has
// no streams, so that close only writes the connection-level status.
func (c *ServerConn) attach(m *Mux) error {
	c.mu.Lock()
	c.mux = m
	st := c.pending
	c.mu.Unlock()
	if st == nil {
		return nil
	}
	defer c.markEnded()
	return m.closeWithStatus(m.ctx, st)
}

// maxCloseReason is the WebSocket limit on a close frame's reason (RFC 6455:
// 125 payload bytes minus the 2-byte code).
const maxCloseReason = 123

// closeReason fits msg into a close frame reason, cutting at a rune boundary.
func closeReason(msg string) string {
	if len(msg) <= maxCloseReason {
		return msg
	}
	cut := maxCloseReason
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut]
}
