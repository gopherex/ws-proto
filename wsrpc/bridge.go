package wsrpc

import "google.golang.org/grpc"

// BridgeConfig carries optional gRPC server interceptors applied by a generated
// XxxServiceFromGRPC bridge. Chain multiple with grpc.ChainUnaryInterceptor /
// grpc.ChainStreamInterceptor before passing.
type BridgeConfig struct {
	Unary  grpc.UnaryServerInterceptor
	Stream grpc.StreamServerInterceptor
}

// BridgeOption configures a generated gRPC bridge.
type BridgeOption func(*BridgeConfig)

// WithUnaryInterceptor sets the unary server interceptor run around unary RPCs.
func WithUnaryInterceptor(i grpc.UnaryServerInterceptor) BridgeOption {
	return func(c *BridgeConfig) { c.Unary = i }
}

// WithStreamInterceptor sets the stream server interceptor run around streaming RPCs.
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
