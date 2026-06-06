// Command server is a standalone WebSocket RPC server used by the
// cross-language integration test (Plan 4). It registers the golden
// EchoService wsrpc handler and listens on a fixed address.
//
// Readiness contract for the test harness:
//   - Listen address comes from WS_PROTO_TEST_ADDR (default 127.0.0.1:8910).
//   - Once the listener is bound, it prints "LISTENING <addr>" to stdout.
//
// The handler implementations mirror example/roundtrip_test.go's impl so the
// generated TypeScript client can assert identical behavior over a real socket.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
)

type impl struct{}

func (impl) Unary(ctx context.Context, req *echov1.UnaryRequest) (*echov1.UnaryResponse, error) {
	return &echov1.UnaryResponse{Greeting: "hello " + req.Name}, nil
}

func (impl) ServerStream(ctx context.Context, req *echov1.ServerStreamRequest, stream *echov1.EchoService_ServerStreamServerWS) error {
	for i := int32(0); i < req.Count; i++ {
		if err := stream.Send(&echov1.ServerStreamResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}

func (impl) ClientStream(ctx context.Context, stream *echov1.EchoService_ClientStreamServerWS) (*echov1.ClientStreamResponse, error) {
	var sum int32
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		sum += req.Value
	}
	return &echov1.ClientStreamResponse{Sum: sum}, nil
}

func (impl) Bidi(ctx context.Context, stream *echov1.EchoService_BidiServerWS) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echov1.BidiResponse{Echo: "echo:" + req.Text}); err != nil {
			return err
		}
	}
}

func main() {
	addr := os.Getenv("WS_PROTO_TEST_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8910"
	}

	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, impl{})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", addr, err)
		os.Exit(1)
	}

	// Signal readiness to the test harness once the socket is bound.
	fmt.Printf("LISTENING %s\n", ln.Addr().String())

	httpSrv := &http.Server{Handler: srv}
	if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
