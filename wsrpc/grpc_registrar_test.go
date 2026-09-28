package wsrpc_test

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
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func dialRegistrarServer(t *testing.T, srv *wsrpc.Server) *wsrpc.ClientConn {
	t.Helper()
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	cc, err := wsrpc.Dial(context.Background(), "ws"+strings.TrimPrefix(hs.URL, "http"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// TestGRPCRegistrar_HealthUnaryAndServerStream serves the stock grpc-go health
// server (a unary and a server-streaming method) through the registrar, with
// no generated wsrpc code involved.
func TestGRPCRegistrar_HealthUnaryAndServerStream(t *testing.T) {
	t.Parallel()

	srv := wsrpc.NewServer(wsrpc.WithInsecureSkipOriginCheck())
	reg := wsrpc.GRPCRegistrar(srv)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", hv1.HealthCheckResponse_SERVING)
	hv1.RegisterHealthServer(reg, healthSrv)

	info := reg.(interface {
		GetServiceInfo() map[string]grpc.ServiceInfo
	}).GetServiceInfo()
	require.Len(t, info, 1)
	require.Contains(t, info, "grpc.health.v1.Health")

	cc := dialRegistrarServer(t, srv)
	ctx := context.Background()

	unary, err := cc.NewStream(ctx, "/grpc.health.v1.Health/Check", nil)
	require.NoError(t, err)
	require.NoError(t, unary.Send(&hv1.HealthCheckRequest{}))
	_ = unary.CloseSend()

	var res hv1.HealthCheckResponse
	require.NoError(t, unary.Recv(&res))
	require.Equal(t, hv1.HealthCheckResponse_SERVING, res.GetStatus())

	watch, err := cc.NewStream(ctx, "/grpc.health.v1.Health/Watch", nil)
	require.NoError(t, err)
	require.NoError(t, watch.Send(&hv1.HealthCheckRequest{}))
	_ = watch.CloseSend()

	var first hv1.HealthCheckResponse
	require.NoError(t, watch.Recv(&first))
	require.Equal(t, hv1.HealthCheckResponse_SERVING, first.GetStatus())

	healthSrv.SetServingStatus("", hv1.HealthCheckResponse_NOT_SERVING)

	var second hv1.HealthCheckResponse
	require.NoError(t, watch.Recv(&second))
	require.Equal(t, hv1.HealthCheckResponse_NOT_SERVING, second.GetStatus())
}

type ctxKey struct{}

// registrarEcho is a protoc-gen-go-grpc EchoServiceServer that reports the
// request metadata and interceptor-injected context value it observes, and
// emits response header/trailer metadata through the gRPC APIs.
type registrarEcho struct {
	echov1.UnimplementedEchoServiceServer
}

func fromCtx(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	v, _ := ctx.Value(ctxKey{}).(string)
	return strings.Join(md.Get("x-req"), ",") + "|" + v
}

func (registrarEcho) Unary(ctx context.Context, req *echov1.UnaryRequest) (*echov1.UnaryResponse, error) {
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-handler-h", "h"))
	_ = grpc.SetTrailer(ctx, metadata.Pairs("x-handler-t", "t"))
	return &echov1.UnaryResponse{Greeting: req.GetName() + ":" + fromCtx(ctx)}, nil
}

func (registrarEcho) ServerStream(req *echov1.ServerStreamRequest, stream grpc.ServerStreamingServer[echov1.ServerStreamResponse]) error {
	if fromCtx(stream.Context()) != "r|icept" {
		return status.Errorf(codes.FailedPrecondition, "ctx: %q", fromCtx(stream.Context()))
	}
	if err := stream.SetHeader(metadata.Pairs("x-h", "h")); err != nil {
		return err
	}
	stream.SetTrailer(metadata.Pairs("x-t", "t"))
	for i := int32(0); i < req.GetCount(); i++ {
		if err := stream.Send(&echov1.ServerStreamResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}

func (registrarEcho) ClientStream(stream grpc.ClientStreamingServer[echov1.ClientStreamRequest, echov1.ClientStreamResponse]) error {
	if fromCtx(stream.Context()) != "r|icept" {
		return status.Errorf(codes.FailedPrecondition, "ctx: %q", fromCtx(stream.Context()))
	}
	var sum int32
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			stream.SetTrailer(metadata.Pairs("x-t", "t"))
			return stream.SendAndClose(&echov1.ClientStreamResponse{Sum: sum})
		}
		if err != nil {
			return err
		}
		sum += m.GetValue()
	}
}

func (registrarEcho) Bidi(stream grpc.BidiStreamingServer[echov1.BidiRequest, echov1.BidiResponse]) error {
	if fromCtx(stream.Context()) != "r|icept" {
		return status.Errorf(codes.FailedPrecondition, "ctx: %q", fromCtx(stream.Context()))
	}
	if err := stream.SendHeader(metadata.Pairs("x-h", "h")); err != nil {
		return err
	}
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echov1.BidiResponse{Echo: m.GetText()}); err != nil {
			return err
		}
	}
}

// ctxStream is the idiomatic interceptor wrapper: it overrides Context and
// delegates everything else to the embedded grpc.ServerStream.
type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }

func TestGRPCRegistrar_UnaryInterceptor(t *testing.T) {
	t.Parallel()

	impl := registrarEcho{}
	var (
		mu   sync.Mutex
		seen []*grpc.UnaryServerInfo
	)
	icept := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		seen = append(seen, info)
		mu.Unlock()
		md, _ := metadata.FromIncomingContext(ctx)
		if len(md.Get("x-deny")) > 0 {
			return nil, status.Error(codes.PermissionDenied, "denied")
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-icept-h", "ih"))
		_ = grpc.SetTrailer(ctx, metadata.Pairs("x-icept-t", "it"))
		return handler(context.WithValue(ctx, ctxKey{}, "icept"), req)
	}

	srv := wsrpc.NewServer(wsrpc.WithInsecureSkipOriginCheck())
	echov1.RegisterEchoServiceServer(wsrpc.GRPCRegistrar(srv, wsrpc.WithUnaryInterceptor(icept)), impl)
	cc := dialRegistrarServer(t, srv)
	ctx := context.Background()

	s, err := cc.NewStream(ctx, echov1.EchoService_Unary_FullMethodName, map[string]string{"x-req": "r"})
	require.NoError(t, err)
	require.NoError(t, s.Send(&echov1.UnaryRequest{Name: "bob"}))
	require.NoError(t, s.CloseSend())

	var res echov1.UnaryResponse
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "bob:r|icept", res.GetGreeting())
	require.Equal(t, "ih", s.Header()["x-icept-h"])
	require.Equal(t, "h", s.Header()["x-handler-h"])
	require.ErrorIs(t, s.Recv(&echov1.UnaryResponse{}), io.EOF)
	require.Equal(t, "it", s.Trailer()["x-icept-t"])
	require.Equal(t, "t", s.Trailer()["x-handler-t"])

	mu.Lock()
	require.Len(t, seen, 1)
	require.Equal(t, echov1.EchoService_Unary_FullMethodName, seen[0].FullMethod)
	require.Equal(t, impl, seen[0].Server)
	mu.Unlock()

	// An interceptor rejection surfaces to the client with its code.
	denied, err := cc.NewStream(ctx, echov1.EchoService_Unary_FullMethodName, map[string]string{"x-deny": "1"})
	require.NoError(t, err)
	require.NoError(t, denied.Send(&echov1.UnaryRequest{Name: "eve"}))
	require.NoError(t, denied.CloseSend())
	err = denied.Recv(&echov1.UnaryResponse{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

// TestGRPCRegistrar_UnaryNoInterceptorMetadata pins that grpc.SetHeader /
// grpc.SetTrailer inside a handler propagate even with no interceptor set.
func TestGRPCRegistrar_UnaryNoInterceptorMetadata(t *testing.T) {
	t.Parallel()

	srv := wsrpc.NewServer(wsrpc.WithInsecureSkipOriginCheck())
	echov1.RegisterEchoServiceServer(wsrpc.GRPCRegistrar(srv), registrarEcho{})
	cc := dialRegistrarServer(t, srv)

	client := echov1.NewEchoServiceWSClient(cc)
	res, err := client.Unary(context.Background(), &echov1.UnaryRequest{Name: "a"}, wsrpc.WithCallHeader("x-req", "r"))
	require.NoError(t, err)
	require.Equal(t, "a:r|", res.GetGreeting())

	s, err := cc.NewStream(context.Background(), echov1.EchoService_Unary_FullMethodName, nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&echov1.UnaryRequest{Name: "b"}))
	require.NoError(t, s.CloseSend())
	require.NoError(t, s.Recv(&echov1.UnaryResponse{}))
	require.Equal(t, "h", s.Header()["x-handler-h"])
	require.ErrorIs(t, s.Recv(&echov1.UnaryResponse{}), io.EOF)
	require.Equal(t, "t", s.Trailer()["x-handler-t"])
}

func TestGRPCRegistrar_StreamInterceptor(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		seen = map[string]grpc.StreamServerInfo{}
	)
	icept := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		mu.Lock()
		seen[info.FullMethod] = *info
		mu.Unlock()
		if _, ok := srv.(registrarEcho); !ok {
			return status.Errorf(codes.Internal, "srv: %T", srv)
		}
		return handler(srv, &ctxStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), ctxKey{}, "icept")})
	}

	srv := wsrpc.NewServer(wsrpc.WithInsecureSkipOriginCheck())
	echov1.RegisterEchoServiceServer(wsrpc.GRPCRegistrar(srv, wsrpc.WithStreamInterceptor(icept)), registrarEcho{})
	cc := dialRegistrarServer(t, srv)
	client := echov1.NewEchoServiceWSClient(cc)
	ctx := context.Background()
	hdr := wsrpc.WithCallHeader("x-req", "r")

	t.Run("server stream", func(t *testing.T) {
		stream, err := client.ServerStream(ctx, &echov1.ServerStreamRequest{Count: 3}, hdr)
		require.NoError(t, err)
		n := 0
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			n++
		}
		require.Equal(t, 3, n)
		require.Equal(t, "h", stream.Header()["x-h"])
		require.Equal(t, "t", stream.Trailer()["x-t"])
	})

	t.Run("client stream", func(t *testing.T) {
		stream, err := client.ClientStream(ctx, hdr)
		require.NoError(t, err)
		for _, v := range []int32{1, 2, 3} {
			require.NoError(t, stream.Send(&echov1.ClientStreamRequest{Value: v}))
		}
		res, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, int32(6), res.GetSum())
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, "t", stream.Trailer()["x-t"])
	})

	t.Run("bidi", func(t *testing.T) {
		stream, err := client.Bidi(ctx, hdr)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&echov1.BidiRequest{Text: "ping"}))
		res, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, "ping", res.GetEcho())
		require.Equal(t, "h", stream.Header()["x-h"])
		require.NoError(t, stream.CloseSend())
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
	})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[string]grpc.StreamServerInfo{
		echov1.EchoService_ServerStream_FullMethodName: {FullMethod: echov1.EchoService_ServerStream_FullMethodName, IsServerStream: true},
		echov1.EchoService_ClientStream_FullMethodName: {FullMethod: echov1.EchoService_ClientStream_FullMethodName, IsClientStream: true},
		echov1.EchoService_Bidi_FullMethodName:         {FullMethod: echov1.EchoService_Bidi_FullMethodName, IsClientStream: true, IsServerStream: true},
	}, seen)
}
