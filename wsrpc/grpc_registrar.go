package wsrpc

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto" // registers the proto codec
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
)

// GRPCRegistrar returns a grpc.ServiceRegistrar that serves any
// protoc-gen-go-grpc service on srv by driving the generated grpc.ServiceDesc
// handlers over wsrpc streams:
//
//	echov1.RegisterEchoServiceServer(wsrpc.GRPCRegistrar(srv), impl)
//
// The gRPC proto codec does the (de)serialization, so no protoc-gen-go-ws code
// is needed for the service. Each method is registered on srv under its gRPC
// full method name ("/pkg.Service/Method"). OPEN headers become incoming gRPC
// metadata (metadata.FromIncomingContext).
//
// Interceptors are installed with the same BridgeOption values the generated
// XxxServiceFromGRPC bridges take (WithUnaryInterceptor, WithStreamInterceptor)
// and see the real grpc.UnaryServerInfo / grpc.StreamServerInfo. Response
// metadata propagates as described on BridgeConfig: unary grpc.SetHeader /
// grpc.SendHeader / grpc.SetTrailer (from the handler or an interceptor) and
// streaming ServerStream SetHeader / SendHeader / SetTrailer.
//
// The returned value also implements GetServiceInfo (like *grpc.Server), so it
// satisfies grpc/reflection's ServiceInfoProvider.
func GRPCRegistrar(srv *Server, opts ...BridgeOption) grpc.ServiceRegistrar {
	return &grpcRegistrar{
		srv:      srv,
		cfg:      ApplyBridgeOptions(opts...),
		codec:    encoding.GetCodecV2("proto"),
		services: make(map[string]grpc.ServiceInfo),
	}
}

type grpcRegistrar struct {
	srv   *Server
	cfg   BridgeConfig
	codec encoding.CodecV2

	mu       sync.Mutex
	services map[string]grpc.ServiceInfo
}

// RegisterService implements grpc.ServiceRegistrar.
func (r *grpcRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	info := grpc.ServiceInfo{Metadata: desc.Metadata}
	for _, m := range desc.Methods {
		method := "/" + desc.ServiceName + "/" + m.MethodName
		r.srv.Register(method, r.unary(method, m.Handler, impl))
		info.Methods = append(info.Methods, grpc.MethodInfo{Name: m.MethodName})
	}
	for _, st := range desc.Streams {
		method := "/" + desc.ServiceName + "/" + st.StreamName
		r.srv.Register(method, r.stream(grpc.StreamServerInfo{
			FullMethod:     method,
			IsClientStream: st.ClientStreams,
			IsServerStream: st.ServerStreams,
		}, st.Handler, impl))
		info.Methods = append(info.Methods, grpc.MethodInfo{
			Name:           st.StreamName,
			IsClientStream: st.ClientStreams,
			IsServerStream: st.ServerStreams,
		})
	}

	r.mu.Lock()
	r.services[desc.ServiceName] = info
	r.mu.Unlock()
}

// GetServiceInfo returns the registered services keyed by full service name,
// mirroring (*grpc.Server).GetServiceInfo.
func (r *grpcRegistrar) GetServiceInfo() map[string]grpc.ServiceInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]grpc.ServiceInfo, len(r.services))
	for k, v := range r.services {
		out[k] = v
	}
	return out
}

// incoming turns OPEN headers into gRPC incoming metadata.
func (r *grpcRegistrar) incoming(ctx context.Context, s *Stream) context.Context {
	md := metadata.MD{}
	for k, v := range s.Header() {
		md.Append(k, v)
	}
	return metadata.NewIncomingContext(ctx, md)
}

func (r *grpcRegistrar) unary(method string, handler grpc.MethodHandler, impl any) Handler {
	return func(ctx context.Context, s *Stream) error {
		raw, err := s.RecvRaw()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		ctx, sink := WithUnaryMetadata(r.incoming(ctx, s))
		ctx = grpc.NewContextWithServerTransportStream(ctx, UnaryServerTransportStream(ctx, method))
		decode := func(v any) error { return r.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, v) }

		// The generated MethodHandler runs the interceptor (if any) itself,
		// with UnaryServerInfo{Server: impl, FullMethod: method}.
		res, err := handler(impl, ctx, decode, r.cfg.Unary)
		if err != nil {
			return err
		}

		out, err := r.codec.Marshal(res)
		if err != nil {
			return err
		}
		defer out.Free()

		if h := sink.Header(); h != nil {
			_ = s.SendHeader(h)
		}
		s.SetTrailer(sink.Trailer())
		return s.SendRaw(out.Materialize())
	}
}

func (r *grpcRegistrar) stream(info grpc.StreamServerInfo, handler grpc.StreamHandler, impl any) Handler {
	return func(ctx context.Context, s *Stream) error {
		ss := &grpcServerStream{s: s, ctx: r.incoming(ctx, s), codec: r.codec}
		if r.cfg.Stream == nil {
			return handler(impl, ss)
		}
		info := info // fresh per call, as grpc-go does
		return r.cfg.Stream(impl, ss, &info, handler)
	}
}

// grpcServerStream adapts *Stream to grpc.ServerStream using the gRPC codec.
type grpcServerStream struct {
	s       *Stream
	ctx     context.Context
	codec   encoding.CodecV2
	pending metadata.MD // leading headers, flushed on SendHeader or first SendMsg
}

func (x *grpcServerStream) Context() context.Context { return x.ctx }

func (x *grpcServerStream) SetHeader(md metadata.MD) error {
	x.pending = metadata.Join(x.pending, md)
	return nil
}

func (x *grpcServerStream) SendHeader(md metadata.MD) error {
	x.pending = metadata.Join(x.pending, md)
	err := x.s.SendHeader(FlattenMD(x.pending))
	x.pending = nil
	return err
}

func (x *grpcServerStream) SetTrailer(md metadata.MD) { x.s.SetTrailer(FlattenMD(md)) }

func (x *grpcServerStream) SendMsg(m any) error {
	if x.pending != nil {
		// Best-effort: ignore FailedPrecondition if headers were already sent.
		_ = x.s.SendHeader(FlattenMD(x.pending))
		x.pending = nil
	}

	out, err := x.codec.Marshal(m)
	if err != nil {
		return err
	}
	defer out.Free()
	return x.s.SendRaw(out.Materialize())
}

func (x *grpcServerStream) RecvMsg(m any) error {
	raw, err := x.s.RecvRaw()
	if err != nil {
		return err
	}
	return x.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, m)
}
