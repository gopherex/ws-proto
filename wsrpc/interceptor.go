package wsrpc

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// StreamKind classifies a method so interceptors can branch on call shape.
type StreamKind int

const (
	StreamKindUnary StreamKind = iota
	StreamKindServerStream
	StreamKindClientStream
	StreamKindBidiStream
)

// MethodSpec describes one RPC method to the dispatch layer and interceptors.
type MethodSpec struct {
	Route string // "/pkg.Service/Method"
	Kind  StreamKind
}

// AnyRequest is the unary request as seen by interceptors. Header is mutable
// (add auth/trace metadata before the call); Msg is the request message — a
// pointer, so its fields may be mutated, or it may be replaced entirely.
type AnyRequest struct {
	Spec   MethodSpec
	Header map[string]string
	Msg    proto.Message
}

// AnyResponse is the completed unary response. Interceptors may read/replace Msg
// and read the leading Header / Trailer.
type AnyResponse struct {
	Spec    MethodSpec
	Header  map[string]string // leading response metadata (KIND_HEADER)
	Trailer map[string]string // END trailers
	Msg     proto.Message
}

// UnaryFunc performs one unary RPC; interceptors wrap it.
type UnaryFunc func(ctx context.Context, req *AnyRequest) (*AnyResponse, error)

// StreamingClientConn drives one streaming RPC. The terminal implementation
// wraps a *Stream; interceptors may wrap it to observe/modify Send/Receive.
type StreamingClientConn interface {
	Spec() MethodSpec
	RequestHeader() map[string]string
	Send(proto.Message) error
	CloseRequest() error
	Receive(proto.Message) error
	ResponseHeader() map[string]string
	ResponseTrailer() map[string]string
	CloseResponse() error
}

// StreamingClientFunc opens a streaming RPC; interceptors wrap it.
type StreamingClientFunc func(ctx context.Context, spec MethodSpec, header map[string]string) (StreamingClientConn, error)

// Interceptor wraps unary and streaming client calls. Implement both methods, or
// embed one of the adapter funcs below to wrap only one call shape.
type Interceptor interface {
	WrapUnary(UnaryFunc) UnaryFunc
	WrapStreamingClient(StreamingClientFunc) StreamingClientFunc
}

// UnaryInterceptorFunc adapts a plain unary wrapper into an Interceptor that
// leaves streaming calls untouched.
type UnaryInterceptorFunc func(UnaryFunc) UnaryFunc

func (f UnaryInterceptorFunc) WrapUnary(next UnaryFunc) UnaryFunc { return f(next) }
func (f UnaryInterceptorFunc) WrapStreamingClient(next StreamingClientFunc) StreamingClientFunc {
	return next
}

// StreamInterceptorFunc adapts a plain streaming wrapper into an Interceptor
// that leaves unary calls untouched.
type StreamInterceptorFunc func(StreamingClientFunc) StreamingClientFunc

func (f StreamInterceptorFunc) WrapUnary(next UnaryFunc) UnaryFunc { return next }
func (f StreamInterceptorFunc) WrapStreamingClient(next StreamingClientFunc) StreamingClientFunc {
	return f(next)
}

// chainUnary composes interceptors around a terminal UnaryFunc. The FIRST
// interceptor is the OUTERMOST (runs first in, last out).
func chainUnary(interceptors []Interceptor, terminal UnaryFunc) UnaryFunc {
	for i := len(interceptors) - 1; i >= 0; i-- {
		terminal = interceptors[i].WrapUnary(terminal)
	}
	return terminal
}

// chainStreaming composes interceptors around a terminal StreamingClientFunc,
// outermost-first.
func chainStreaming(interceptors []Interceptor, terminal StreamingClientFunc) StreamingClientFunc {
	for i := len(interceptors) - 1; i >= 0; i-- {
		terminal = interceptors[i].WrapStreamingClient(terminal)
	}
	return terminal
}
