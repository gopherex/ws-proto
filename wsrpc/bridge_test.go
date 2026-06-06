package wsrpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
)

func TestApplyBridgeOptions(t *testing.T) {
	u := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, req)
	}
	s := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, ss)
	}

	cfg := ApplyBridgeOptions(WithUnaryInterceptor(u), WithStreamInterceptor(s))
	if cfg.Unary == nil {
		t.Fatal("expected Unary interceptor to be set")
	}
	if cfg.Stream == nil {
		t.Fatal("expected Stream interceptor to be set")
	}

	empty := ApplyBridgeOptions()
	if empty.Unary != nil || empty.Stream != nil {
		t.Fatal("expected empty config with no options")
	}
}
