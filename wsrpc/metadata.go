package wsrpc

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mdSink accumulates unary response header/trailer metadata set by a handler or
// interceptor during a unary RPC. The generated unary registrar installs a fresh
// sink on the per-call context (WithUnaryMetadata), and flushes it onto the
// wsrpc stream after the handler returns.
type mdSink struct {
	mu      sync.Mutex
	header  map[string]string
	trailer map[string]string
}

type mdSinkKey struct{}

func sinkFrom(ctx context.Context) *mdSink {
	s, _ := ctx.Value(mdSinkKey{}).(*mdSink)
	return s
}

func (s *mdSink) mergeHeader(md map[string]string) {
	if len(md) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.header == nil {
		s.header = make(map[string]string, len(md))
	}
	for k, v := range md {
		s.header[k] = v
	}
}

func (s *mdSink) mergeTrailer(md map[string]string) {
	if len(md) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trailer == nil {
		s.trailer = make(map[string]string, len(md))
	}
	for k, v := range md {
		s.trailer[k] = v
	}
}

func (s *mdSink) takeHeader() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.header) == 0 {
		return nil
	}
	return s.header
}

func (s *mdSink) takeTrailer() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.trailer) == 0 {
		return nil
	}
	return s.trailer
}

// SetHeader merges leading response header metadata for a unary RPC. It is a
// no-op when called outside a unary handler context (e.g. from a streaming
// handler, which should use the stream's SendHeader/SetHeader instead).
func SetHeader(ctx context.Context, md map[string]string) {
	if s := sinkFrom(ctx); s != nil {
		s.mergeHeader(md)
	}
}

// SetTrailer merges response trailer metadata for a unary RPC. It is a no-op
// when called outside a unary handler context (streaming handlers use the
// stream's SetTrailer instead).
func SetTrailer(ctx context.Context, md map[string]string) {
	if s := sinkFrom(ctx); s != nil {
		s.mergeTrailer(md)
	}
}

// UnaryMD is the opaque handle the generated unary registrar uses to read back
// the header/trailer metadata accumulated during a unary handler call.
type UnaryMD struct {
	sink *mdSink
}

// Header returns the accumulated leading response header metadata, or nil.
func (u *UnaryMD) Header() map[string]string {
	if u == nil || u.sink == nil {
		return nil
	}
	return u.sink.takeHeader()
}

// Trailer returns the accumulated response trailer metadata, or nil.
func (u *UnaryMD) Trailer() map[string]string {
	if u == nil || u.sink == nil {
		return nil
	}
	return u.sink.takeTrailer()
}

// WithUnaryMetadata installs a fresh metadata sink on ctx and returns the new
// context plus an opaque *UnaryMD the caller (the generated unary registrar)
// flushes after the handler returns. User handlers and gRPC interceptors set
// metadata via wsrpc.SetHeader/SetTrailer (or grpc.SetHeader/SetTrailer through
// the bridge's ServerTransportStream) on the returned context.
func WithUnaryMetadata(ctx context.Context) (context.Context, *UnaryMD) {
	s := &mdSink{}
	return context.WithValue(ctx, mdSinkKey{}, s), &UnaryMD{sink: s}
}

// unaryServerTransportStream adapts the unary metadata sink to
// grpc.ServerTransportStream so that grpc.SetHeader / grpc.SendHeader /
// grpc.SetTrailer called by a unary interceptor (which look up the
// ServerTransportStream from the context) forward into the sink. The registrar
// flushes the sink after the handler returns, so SendHeader need not send
// immediately.
type unaryServerTransportStream struct {
	ctx    context.Context
	method string
}

func (t *unaryServerTransportStream) Method() string { return t.method }

func (t *unaryServerTransportStream) SetHeader(md metadata.MD) error {
	SetHeader(t.ctx, FlattenMD(md))
	return nil
}

func (t *unaryServerTransportStream) SendHeader(md metadata.MD) error {
	SetHeader(t.ctx, FlattenMD(md))
	return nil
}

func (t *unaryServerTransportStream) SetTrailer(md metadata.MD) error {
	SetTrailer(t.ctx, FlattenMD(md))
	return nil
}

// UnaryServerTransportStream returns a grpc.ServerTransportStream that forwards
// header/trailer metadata into the unary metadata sink already installed on ctx
// (via WithUnaryMetadata). The generated gRPC bridge installs it with
// grpc.NewContextWithServerTransportStream so that grpc.SetHeader /
// grpc.SetTrailer inside a unary interceptor are captured and propagated.
func UnaryServerTransportStream(ctx context.Context, fullMethod string) grpc.ServerTransportStream {
	return &unaryServerTransportStream{ctx: ctx, method: fullMethod}
}
