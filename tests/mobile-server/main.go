// Command mobile-server serves the shared Swift/Kotlin interoperability fixture.
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func main() {
	addr := os.Getenv("WS_PROTO_TEST_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8910"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("LISTENING %s", listener.Addr())
	server := &http.Server{Handler: newHandler(), ReadHeaderTimeout: 5 * time.Second}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newHandler() http.Handler {
	server := wsrpc.NewServer(wsrpc.WithSameOriginOnly())
	server.Register("/echo.v1.EchoService/Unary", withMetadata(unary))
	server.Register("/echo.v1.EchoService/ServerStream", withMetadata(serverStream))
	server.Register("/echo.v1.EchoService/ClientStream", withMetadata(clientStream))
	server.Register("/echo.v1.EchoService/Bidi", withMetadata(bidi))
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", server)
	return mux
}

func withMetadata(next wsrpc.Handler) wsrpc.Handler {
	return func(ctx context.Context, stream *wsrpc.Stream) error {
		stream.SetTrailer(map[string]string{"x-trailer": "done"})
		if err := stream.SendHeader(map[string]string{
			"x-header": "ready", "x-echo": stream.Header()["x-test"],
		}); err != nil {
			return err
		}
		return next(ctx, stream)
	}
}

func unary(ctx context.Context, stream *wsrpc.Stream) error {
	request := new(echov1.UnaryRequest)
	if err := stream.Recv(request); err != nil {
		return err
	}
	switch request.GetName() {
	case "!wait":
		<-ctx.Done()
		return ctx.Err()
	case "!empty":
		return nil
	case "!error":
		return detailedError()
	case "!conn-end":
		connection, ok := wsrpc.ServerConnFromContext(ctx)
		if !ok {
			return wsrpc.Errorf(codes.Internal, "missing connection")
		}
		return connection.CloseWithStatus(ctx, &wsrpc.Status{Code: codes.PermissionDenied, Message: "session expired"})
	default:
		return stream.Send(&echov1.UnaryResponse{Greeting: "hello " + request.GetName()})
	}
}

func serverStream(ctx context.Context, stream *wsrpc.Stream) error {
	request := new(echov1.ServerStreamRequest)
	if err := stream.Recv(request); err != nil {
		return err
	}
	if request.GetCount() < 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	for index := int32(0); index < request.GetCount(); index++ {
		if err := stream.Send(&echov1.ServerStreamResponse{Index: index}); err != nil {
			return err
		}
	}
	return nil
}

func clientStream(_ context.Context, stream *wsrpc.Stream) error {
	var sum int32
	for {
		request := new(echov1.ClientStreamRequest)
		err := stream.Recv(request)
		if errors.Is(err, io.EOF) {
			return stream.Send(&echov1.ClientStreamResponse{Sum: sum})
		}
		if err != nil {
			return err
		}
		sum += request.GetValue()
	}
}

func bidi(_ context.Context, stream *wsrpc.Stream) error {
	for {
		request := new(echov1.BidiRequest)
		err := stream.Recv(request)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch request.GetText() {
		case "!end":
			return nil
		case "!error":
			return detailedError()
		default:
			if err := stream.Send(&echov1.BidiResponse{Echo: "echo:" + request.GetText()}); err != nil {
				return err
			}
		}
	}
}

func detailedError() error {
	detail, err := anypb.New(wrapperspb.String("mobile-detail"))
	if err != nil {
		return err
	}
	encoded, err := proto.Marshal(detail)
	if err != nil {
		return err
	}
	return &wsrpc.Status{Code: codes.InvalidArgument, Message: "mobile-error", Details: [][]byte{encoded}}
}
