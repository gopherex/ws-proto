package wsrpc

import (
	"context"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
)

// Handler runs one server-side RPC stream. Returning an error ends the stream
// with the corresponding status; returning nil ends with codes.OK.
type Handler func(ctx context.Context, stream *Stream) error

// Server is an http.Handler that upgrades to WebSocket and dispatches streams
// to registered handlers keyed by fully-qualified method.
type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewServer() *Server {
	return &Server{handlers: make(map[string]Handler)}
}

// Register binds a handler to a method like "/pkg.Service/Method".
func (s *Server) Register(method string, h Handler) {
	s.mu.Lock()
	s.handlers[method] = h
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn := newWSConn(c)
	mux := newMux(r.Context(), conn, func(stream *Stream) { go s.serveStream(stream) })
	<-mux.ctx.Done()
	_ = c.CloseNow()
}

func (s *Server) serveStream(stream *Stream) {
	defer stream.mux.remove(stream.id)
	s.mu.RLock()
	h := s.handlers[stream.method]
	s.mu.RUnlock()
	if h == nil {
		_ = stream.end(&Status{Code: codes.Unimplemented, Message: "unknown method " + stream.method}, nil)
		return
	}
	err := h(stream.ctx, stream)
	st := FromError(err)
	if st == nil {
		st = &Status{Code: codes.OK}
	}
	_ = stream.end(st, nil)
}
