package wsrpc

import (
	"context"
	"fmt"
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
	// Fail closed on origin policy: an operator must make an explicit choice via
	// WithOriginPatterns (restrict) or WithInsecureSkipOriginCheck (allow all).
	// With neither, every upgrade is rejected rather than silently accepting
	// cross-origin clients (CSRF / cross-site WebSocket hijacking defense).
	if len(s.cfg.originPatterns) == 0 && !s.cfg.insecureSkipOrigin {
		http.Error(w, "wsrpc: origin policy not configured (use WithOriginPatterns or WithInsecureSkipOriginCheck)", http.StatusForbidden)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{Subprotocol},
		OriginPatterns:     s.cfg.originPatterns,
		InsecureSkipVerify: s.cfg.insecureSkipOrigin,
		CompressionMode:    s.cfg.compression,
	})
	if err != nil {
		return
	}
	base := r.Context()
	if s.cfg.connContext != nil {
		base = s.cfg.connContext(base, r)
	}
	conn := newWSConn(c, s.cfg.readLimit)
	mux := s.newServerMux(base, conn)
	mux.startKeepalive(s.cfg.keepalive, s.cfg.keepaliveTimeout)
	<-mux.ctx.Done()
	if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
		_ = c.CloseNow()
	}
}

// newServerMux builds the per-connection server mux that dispatches streams to
// registered handlers (with middleware and the max-streams cap applied). Shared
// by ServeHTTP (WebSocket) and ServeConn (in-memory).
func (s *Server) newServerMux(ctx context.Context, conn FrameConn) *Mux {
	return newMuxConfig(ctx, conn, func(stream *Stream) { go s.serveStream(stream) },
		s.cfg.receiveBuffer, s.cfg.initialWindow, s.cfg.maxStreams)
}

// ServeConn serves a single connection over an existing FrameConn — the
// in-memory analog of serving a WebSocket upgrade. Streams are dispatched to
// registered handlers exactly as in ServeHTTP, but origin policy and subprotocol
// negotiation are skipped: those defend against browser/intermediary attacks
// that have no meaning on a handed-off in-process channel. Keepalive pings are
// skipped too — there is no proxy idle timeout to outlast.
//
// ServeConn blocks until ctx is canceled or conn is closed by the peer. Pair it
// with NewPipe and DialConn to run a server and client in one process:
//
//	srvEnd, cliEnd := wsrpc.NewPipe()
//	go srv.ServeConn(ctx, srvEnd)
//	cc, _ := wsrpc.DialConn(ctx, cliEnd)
func (s *Server) ServeConn(ctx context.Context, conn FrameConn) {
	mux := s.newServerMux(ctx, conn)
	<-mux.ctx.Done()
}

func (s *Server) serveStream(stream *Stream) {
	defer stream.mux.remove(stream.id)
	if stream.deadlineCancel != nil {
		defer stream.deadlineCancel()
	}
	// Recover panics from handlers, middleware, or gRPC interceptors so one bad
	// RPC ends with codes.Internal instead of taking down the whole process.
	defer func() {
		if r := recover(); r != nil {
			_ = stream.end(&Status{Code: codes.Internal, Message: fmt.Sprintf("wsrpc: panic: %v", r)}, nil)
		}
	}()
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
	_ = stream.end(st, stream.takeTrailer())
}
