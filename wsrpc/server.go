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
	cfg      serverConfig
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewServer(opts ...ServerOption) *Server {
	cfg := defaultServerConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &Server{cfg: cfg, handlers: make(map[string]Handler)}
}

// Register binds a handler to a method like "/pkg.Service/Method".
func (s *Server) Register(method string, h Handler) {
	s.mu.Lock()
	s.handlers[method] = h
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:   []string{Subprotocol},
		OriginPatterns: s.cfg.originPatterns,
	})
	if err != nil {
		return
	}
	base := r.Context()
	if s.cfg.connContext != nil {
		base = s.cfg.connContext(base, r)
	}
	conn := newWSConn(c, s.cfg.readLimit)
	mux := newMux(base, conn, func(stream *Stream) { go s.serveStream(stream) })
	mux.startKeepalive(s.cfg.keepalive, s.cfg.keepaliveTimeout)
	<-mux.ctx.Done()
	if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
		_ = c.CloseNow()
	}
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
	h = chain(h, s.cfg.middleware)
	err := h(stream.ctx, stream)
	st := FromError(err)
	if st == nil {
		st = &Status{Code: codes.OK}
	}
	_ = stream.end(st, nil)
}
