package wsrpc

import (
	"context"
	"slices"
	"testing"

	"google.golang.org/grpc"
)

func TestServerMethods(t *testing.T) {
	srv := NewServer()
	srv.Register("/b.S/M", func(context.Context, *Stream) error { return nil })
	GRPCRegistrar(srv).RegisterService(&grpc.ServiceDesc{
		ServiceName: "a.S",
		HandlerType: (*any)(nil),
		Methods:     []grpc.MethodDesc{{MethodName: "U"}},
		Streams:     []grpc.StreamDesc{{StreamName: "B", ServerStreams: true, ClientStreams: true}},
	}, struct{}{})

	if got, want := srv.Methods(), []string{"/a.S/B", "/a.S/U", "/b.S/M"}; !slices.Equal(got, want) {
		t.Fatalf("Methods() = %v, want %v", got, want)
	}
}
