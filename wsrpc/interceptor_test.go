package wsrpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// echoServerConn wires a server-side mux over a pipe whose handlers echo a
// StringValue and report the request's authorization header.
func echoServerConn(t *testing.T, ctx context.Context, gotAuth chan<- string) *ClientConn {
	t.Helper()
	srvEnd, cliEnd := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) {
		go func() {
			select {
			case gotAuth <- s.Header()["authorization"]:
			default:
			}
			var v wrapperspb.StringValue
			if err := s.Recv(&v); err != nil {
				_ = s.end(&Status{Code: 2, Message: err.Error()}, nil)
				return
			}
			_ = s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value})
			_ = s.end(&Status{Code: 0}, nil)
		}()
	}, defaultReceiveBuffer)
	return newClientConn(ctx, cliEnd)
}

// TestClientUnaryInterceptor verifies a unary interceptor runs, can inject a
// request header that reaches the server, and that the response flows back.
func TestClientUnaryInterceptor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotAuth := make(chan string, 1)
	cc := echoServerConn(t, ctx, gotAuth)

	ran := false
	cc.interceptors = []Interceptor{
		UnaryInterceptorFunc(func(next UnaryFunc) UnaryFunc {
			return func(ctx context.Context, req *AnyRequest) (*AnyResponse, error) {
				ran = true
				req.Header["authorization"] = "Bearer t"
				return next(ctx, req)
			}
		}),
	}

	out, err := InvokeUnary(ctx, cc, MethodSpec{Route: "/t/Echo", Kind: StreamKindUnary},
		&wrapperspb.StringValue{Value: "hi"},
		func() proto.Message { return new(wrapperspb.StringValue) }, nil)
	require.NoError(t, err)
	require.True(t, ran, "interceptor did not run")
	require.Equal(t, "echo:hi", out.(*wrapperspb.StringValue).Value)

	select {
	case auth := <-gotAuth:
		require.Equal(t, "Bearer t", auth, "interceptor-injected header did not reach the server")
	case <-time.After(time.Second):
		t.Fatal("server never reported the auth header")
	}
}

// TestClientUnaryInterceptorOrderAndShortCircuit verifies outermost-first
// ordering and that an interceptor can short-circuit without calling next.
func TestClientUnaryInterceptorOrderAndShortCircuit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotAuth := make(chan string, 1)
	cc := echoServerConn(t, ctx, gotAuth)

	var order []string
	outer := UnaryInterceptorFunc(func(next UnaryFunc) UnaryFunc {
		return func(ctx context.Context, req *AnyRequest) (*AnyResponse, error) {
			order = append(order, "outer")
			return next(ctx, req)
		}
	})
	shortCircuit := UnaryInterceptorFunc(func(next UnaryFunc) UnaryFunc {
		return func(ctx context.Context, req *AnyRequest) (*AnyResponse, error) {
			order = append(order, "short")
			return &AnyResponse{Spec: req.Spec, Msg: &wrapperspb.StringValue{Value: "canned"}}, nil
		}
	})
	cc.interceptors = []Interceptor{outer, shortCircuit}

	out, err := InvokeUnary(ctx, cc, MethodSpec{Route: "/t/Echo", Kind: StreamKindUnary},
		&wrapperspb.StringValue{Value: "hi"},
		func() proto.Message { return new(wrapperspb.StringValue) }, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"outer", "short"}, order)
	require.Equal(t, "canned", out.(*wrapperspb.StringValue).Value)
}
