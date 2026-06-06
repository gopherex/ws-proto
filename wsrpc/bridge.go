package wsrpc

import "google.golang.org/grpc"

// BridgeConfig carries optional gRPC server interceptors applied by a generated
// XxxServiceFromGRPC bridge. Chain multiple with grpc.ChainUnaryInterceptor /
// grpc.ChainStreamInterceptor before passing.
//
// Limitation: response metadata an interceptor sets via grpc.SetHeader /
// grpc.SendHeader / grpc.SetTrailer is NOT propagated — wsrpc has no
// gRPC-metadata channel mapped onto the wire. Interceptors used for auth,
// logging, recovery, validation and rate limiting (which act on ctx/err) work
// as expected; ones whose sole purpose is emitting response headers do not.
type BridgeConfig struct {
	Unary  grpc.UnaryServerInterceptor
	Stream grpc.StreamServerInterceptor
}

// BridgeOption configures a generated gRPC bridge.
type BridgeOption func(*BridgeConfig)

// WithUnaryInterceptor sets the unary server interceptor run around unary RPCs.
// Response metadata set via grpc.SetHeader/SendHeader is not propagated (see BridgeConfig).
func WithUnaryInterceptor(i grpc.UnaryServerInterceptor) BridgeOption {
	return func(c *BridgeConfig) { c.Unary = i }
}

// WithStreamInterceptor sets the stream server interceptor run around streaming RPCs.
//
// A stream interceptor that wraps the grpc.ServerStream MUST delegate SendMsg /
// RecvMsg to the embedded ServerStream (the idiomatic pattern). The bridge
// captures the client-streaming response through the underlying stream, so a
// wrapper that swallows SendMsg would make a successful RPC fail with
// codes.Internal. Response metadata is not propagated (see BridgeConfig).
func WithStreamInterceptor(i grpc.StreamServerInterceptor) BridgeOption {
	return func(c *BridgeConfig) { c.Stream = i }
}

// ApplyBridgeOptions resolves options into a BridgeConfig (used by generated code).
func ApplyBridgeOptions(opts ...BridgeOption) BridgeConfig {
	var c BridgeConfig
	for _, o := range opts {
		o(&c)
	}
	return c
}
