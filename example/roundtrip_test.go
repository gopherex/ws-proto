package example_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// recorder collects FullMethod values observed by interceptors, guarded for -race.
type recorder struct {
	mu     sync.Mutex
	unary  []string
	stream []string
}

func (r *recorder) addUnary(m string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unary = append(r.unary, m)
}

func (r *recorder) addStream(m string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stream = append(r.stream, m)
}

func (r *recorder) snapshot() (unary, stream []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.unary...), append([]string(nil), r.stream...)
}

// dialGRPCOpts serves grpcImpl through the bridge with the given options.
func dialGRPCOpts(t *testing.T, opts ...wsrpc.BridgeOption) echov1.EchoServiceWSClient {
	t.Helper()
	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, echov1.EchoServiceFromGRPC(grpcImpl{}, opts...))

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return echov1.NewEchoServiceWSClient(cc)
}

func TestBridgeUnaryInterceptor(t *testing.T) {
	rec := &recorder{}
	recU := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rec.addUnary(info.FullMethod)
		return handler(ctx, req)
	}
	client := dialGRPCOpts(t, wsrpc.WithUnaryInterceptor(recU))

	res, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "x"})
	require.NoError(t, err)
	require.Equal(t, "grpc x", res.Greeting)

	unary, _ := rec.snapshot()
	require.Equal(t, []string{"/echo.v1.EchoService/Unary"}, unary)
}

func TestBridgeStreamInterceptorAllKinds(t *testing.T) {
	rec := &recorder{}
	recS := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		rec.addStream(info.FullMethod)
		return handler(srv, ss)
	}
	client := dialGRPCOpts(t, wsrpc.WithStreamInterceptor(recS))

	// server-stream
	ss, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 2})
	require.NoError(t, err)
	var got []int32
	for {
		res, err := ss.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, res.Index)
	}
	require.Equal(t, []int32{0, 1}, got)

	// client-stream
	cs, err := client.ClientStream(context.Background())
	require.NoError(t, err)
	for i := int32(1); i <= 3; i++ {
		require.NoError(t, cs.Send(&echov1.ClientStreamRequest{Value: i}))
	}
	cres, err := cs.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, int32(6), cres.Sum)

	// bidi
	bs, err := client.Bidi(context.Background())
	require.NoError(t, err)
	require.NoError(t, bs.Send(&echov1.BidiRequest{Text: "p"}))
	r1, err := bs.Recv()
	require.NoError(t, err)
	require.Equal(t, "gecho:p", r1.Echo)
	require.NoError(t, bs.CloseSend())
	_, err = bs.Recv()
	require.Equal(t, io.EOF, err)

	_, stream := rec.snapshot()
	require.ElementsMatch(t, []string{
		"/echo.v1.EchoService/ServerStream",
		"/echo.v1.EchoService/ClientStream",
		"/echo.v1.EchoService/Bidi",
	}, stream)
}

func TestBridgeStreamInterceptorShortCircuits(t *testing.T) {
	denyS := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return status.Error(codes.PermissionDenied, "denied")
	}
	client := dialGRPCOpts(t, wsrpc.WithStreamInterceptor(denyS))

	ss, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 3})
	require.NoError(t, err)
	_, err = ss.Recv()
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestBridgeUnaryInterceptorShortCircuits(t *testing.T) {
	denyU := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	client := dialGRPCOpts(t, wsrpc.WithUnaryInterceptor(denyU))

	_, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "x"})
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
