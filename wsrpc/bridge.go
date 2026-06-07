package wsrpc

import (
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// FlattenMD flattens a gRPC metadata.MD (map[string][]string) into the
// map[string]string carried on the wire by joining each key's values with ",".
func FlattenMD(md metadata.MD) map[string]string {
	if len(md) == 0 {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		out[k] = strings.Join(v, ",")
	}
	return out
}

// BridgeConfig carries optional gRPC server interceptors applied by a generated
// XxxServiceFromGRPC bridge. Chain multiple with grpc.ChainUnaryInterceptor /
// grpc.ChainStreamInterceptor before passing.
//
// Streaming response metadata an interceptor or handler sets via the
// grpc.ServerStream's SetHeader / SendHeader / SetTrailer IS propagated: leading
// headers become a KIND_HEADER frame and trailers ride the END frame. Unary
// response metadata set via grpc.SetHeader / grpc.SendHeader / grpc.SetTrailer
// (the unary path has no ServerStream; the bridge installs a
// grpc.ServerTransportStream that forwards into the unary metadata sink the
// generated registrar flushes after the handler returns) IS now propagated too.
type BridgeConfig struct {
	Unary  grpc.UnaryServerInterceptor
	Stream grpc.StreamServerInterceptor
}

// BridgeOption configures a generated gRPC bridge.
type BridgeOption func(*BridgeConfig)

// WithUnaryInterceptor sets the unary server interceptor run around unary RPCs.
// Unary response metadata set via grpc.SetHeader/SendHeader/SetTrailer is propagated (see BridgeConfig).
func WithUnaryInterceptor(i grpc.UnaryServerInterceptor) BridgeOption {
	return func(c *BridgeConfig) { c.Unary = i }
}

// WithStreamInterceptor sets the stream server interceptor run around streaming RPCs.
//
// A stream interceptor that wraps the grpc.ServerStream MUST delegate SendMsg /
// RecvMsg to the embedded ServerStream (the idiomatic pattern). The bridge
// captures the client-streaming response through the underlying stream, so a
// wrapper that swallows SendMsg would make a successful RPC fail with
// codes.Internal. Streaming response metadata (SetHeader/SendHeader/SetTrailer
// on the ServerStream) IS propagated (see BridgeConfig).
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
