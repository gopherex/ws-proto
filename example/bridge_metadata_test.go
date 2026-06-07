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
	"google.golang.org/grpc/metadata"
)

// mdImpl is a gRPC EchoServiceServer whose streaming method emits leading header
// and trailer response metadata via the grpc.ServerStream, the realistic path
// the bridge now propagates.
type mdImpl struct {
	echov1.UnimplementedEchoServiceServer
}

func (mdImpl) ServerStream(req *echov1.ServerStreamRequest, stream grpc.ServerStreamingServer[echov1.ServerStreamResponse]) error {
	if err := stream.SendHeader(metadata.Pairs("x-md", "v")); err != nil {
		return err
	}
	stream.SetTrailer(metadata.Pairs("x-trl", "t"))
	for i := int32(0); i < req.Count; i++ {
		if err := stream.Send(&echov1.ServerStreamResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}

// TestBridgeStreamingMetadataPropagates verifies that streaming response
// metadata set on the grpc.ServerStream (SendHeader/SetTrailer) reaches the
// wsrpc client as a leading header and trailer respectively.
func TestBridgeStreamingMetadataPropagates(t *testing.T) {
	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, echov1.EchoServiceFromGRPC(mdImpl{}))

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	client := echov1.NewEchoServiceWSClient(cc)

	stream, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 2})
	require.NoError(t, err)

	// Drain the responses so END (and trailers) arrive.
	var got int
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
		got++
	}
	require.Equal(t, 2, got)

	require.Equal(t, "v", stream.Header()["x-md"])
	require.Equal(t, "t", stream.Trailer()["x-trl"])
}

// unaryMDImpl is a native wsrpc EchoServiceHandler whose unary method sets
// leading-header and trailer response metadata via wsrpc.SetHeader/SetTrailer.
type unaryMDImpl struct{ impl }

func (unaryMDImpl) Unary(ctx context.Context, req *echov1.UnaryRequest) (*echov1.UnaryResponse, error) {
	wsrpc.SetHeader(ctx, map[string]string{"x-h": "1"})
	wsrpc.SetTrailer(ctx, map[string]string{"x-t": "2"})
	return &echov1.UnaryResponse{Greeting: "hello " + req.Name}, nil
}

// TestUnaryNativeMetadataPropagates verifies a native unary handler's
// wsrpc.SetHeader/SetTrailer reach the client as a leading header and trailer.
// The generated unary client returns only the response, so the test drives the
// underlying wsrpc stream directly to observe Header()/Trailer().
func TestUnaryNativeMetadataPropagates(t *testing.T) {
	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, unaryMDImpl{})

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	s, err := cc.NewStream(context.Background(), "/echo.v1.EchoService/Unary", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&echov1.UnaryRequest{Name: "bob"}))
	require.NoError(t, s.CloseSend())

	var res echov1.UnaryResponse
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "hello bob", res.Greeting)
	require.Equal(t, "1", s.Header()["x-h"])

	// Drain to END so trailers are applied.
	require.ErrorIs(t, s.Recv(&echov1.UnaryResponse{}), io.EOF)
	require.Equal(t, "2", s.Trailer()["x-t"])
}

// unaryInterceptorImpl is a gRPC EchoServiceServer; its unary response metadata
// is set by an interceptor (below) rather than the handler.
type unaryInterceptorImpl struct {
	echov1.UnimplementedEchoServiceServer
}

func (unaryInterceptorImpl) Unary(ctx context.Context, req *echov1.UnaryRequest) (*echov1.UnaryResponse, error) {
	return &echov1.UnaryResponse{Greeting: "grpc " + req.Name}, nil
}

// TestBridgeUnaryInterceptorMetadataPropagates verifies that a unary
// interceptor's grpc.SetHeader/grpc.SetTrailer (captured via the bridge's
// ServerTransportStream) reach the wsrpc client as a leading header and trailer.
func TestBridgeUnaryInterceptorMetadataPropagates(t *testing.T) {
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-md", "v"))
		_ = grpc.SetTrailer(ctx, metadata.Pairs("x-tr", "w"))
		return handler(ctx, req)
	}

	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv,
		echov1.EchoServiceFromGRPC(unaryInterceptorImpl{}, wsrpc.WithUnaryInterceptor(interceptor)))

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	s, err := cc.NewStream(context.Background(), "/echo.v1.EchoService/Unary", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&echov1.UnaryRequest{Name: "x"}))
	require.NoError(t, s.CloseSend())

	var res echov1.UnaryResponse
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "grpc x", res.Greeting)
	require.Equal(t, "v", s.Header()["x-md"])

	require.ErrorIs(t, s.Recv(&echov1.UnaryResponse{}), io.EOF)
	require.Equal(t, "w", s.Trailer()["x-tr"])
}
