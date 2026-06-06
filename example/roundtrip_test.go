package example_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// impl is a hand-written EchoServiceHandler.
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

func dial(t *testing.T) echov1.EchoServiceWSClient {
	t.Helper()
	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, impl{})

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return echov1.NewEchoServiceWSClient(cc)
}

func TestUnary(t *testing.T) {
	client := dial(t)
	res, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "bob"})
	require.NoError(t, err)
	require.Equal(t, "hello bob", res.Greeting)
}

func TestServerStream(t *testing.T) {
	client := dial(t)
	stream, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 3})
	require.NoError(t, err)
	var got []int32
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, res.Index)
	}
	require.Equal(t, []int32{0, 1, 2}, got)
}

func TestClientStream(t *testing.T) {
	client := dial(t)
	stream, err := client.ClientStream(context.Background())
	require.NoError(t, err)
	for i := int32(1); i <= 4; i++ {
		require.NoError(t, stream.Send(&echov1.ClientStreamRequest{Value: i}))
	}
	res, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, int32(10), res.Sum)
}

func TestBidi(t *testing.T) {
	client := dial(t)
	stream, err := client.Bidi(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echov1.BidiRequest{Text: "a"}))
	r1, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "echo:a", r1.Echo)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

// grpcImpl implements the protoc-gen-go-grpc EchoServiceServer interface.
// It embeds the Unimplemented base to satisfy forward-compat method additions.
type grpcImpl struct {
	echov1.UnimplementedEchoServiceServer
}

func (grpcImpl) Unary(ctx context.Context, req *echov1.UnaryRequest) (*echov1.UnaryResponse, error) {
	return &echov1.UnaryResponse{Greeting: "grpc " + req.Name}, nil
}

func (grpcImpl) ServerStream(req *echov1.ServerStreamRequest, stream grpc.ServerStreamingServer[echov1.ServerStreamResponse]) error {
	for i := int32(0); i < req.Count; i++ {
		if err := stream.Send(&echov1.ServerStreamResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}

func (grpcImpl) ClientStream(stream grpc.ClientStreamingServer[echov1.ClientStreamRequest, echov1.ClientStreamResponse]) error {
	var sum int32
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&echov1.ClientStreamResponse{Sum: sum})
		}
		if err != nil {
			return err
		}
		sum += req.Value
	}
}

func (grpcImpl) Bidi(stream grpc.BidiStreamingServer[echov1.BidiRequest, echov1.BidiResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echov1.BidiResponse{Echo: "gecho:" + req.Text}); err != nil {
			return err
		}
	}
}

func dialGRPC(t *testing.T) echov1.EchoServiceWSClient {
	t.Helper()
	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, echov1.EchoServiceFromGRPC(grpcImpl{}))

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return echov1.NewEchoServiceWSClient(cc)
}

func TestBridgeUnary(t *testing.T) {
	client := dialGRPC(t)
	res, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "x"})
	require.NoError(t, err)
	require.Equal(t, "grpc x", res.Greeting)
}

func TestBridgeServerStream(t *testing.T) {
	client := dialGRPC(t)
	stream, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 3})
	require.NoError(t, err)
	var got []int32
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, res.Index)
	}
	require.Equal(t, []int32{0, 1, 2}, got)
}

func TestBridgeClientStream(t *testing.T) {
	client := dialGRPC(t)
	stream, err := client.ClientStream(context.Background())
	require.NoError(t, err)
	for i := int32(1); i <= 3; i++ {
		require.NoError(t, stream.Send(&echov1.ClientStreamRequest{Value: i}))
	}
	res, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, int32(6), res.Sum)
}

func TestBridgeBidi(t *testing.T) {
	client := dialGRPC(t)
	stream, err := client.Bidi(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echov1.BidiRequest{Text: "p"}))
	r1, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "gecho:p", r1.Echo)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}
