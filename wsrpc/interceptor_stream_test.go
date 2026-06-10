package wsrpc

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// countingStreamConn wraps a StreamingClientConn and counts Send/Receive — the kind of
// thing a streaming interceptor does to observe message flow.
type countingStreamConn struct {
	StreamingClientConn
	sends, recvs *int
}

func (c *countingStreamConn) Send(m proto.Message) error {
	*c.sends++
	return c.StreamingClientConn.Send(m)
}

func (c *countingStreamConn) Receive(m proto.Message) error {
	err := c.StreamingClientConn.Receive(m)
	if err == nil {
		*c.recvs++
	}
	return err
}

// TestClientStreamingInterceptor verifies a streaming interceptor wraps the
// connection and observes each Send/Receive, and can inject a request header.
func TestClientStreamingInterceptor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotAuth := make(chan string, 1)
	srvEnd, cliEnd := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) {
		go func() {
			select {
			case gotAuth <- s.Header()["authorization"]:
			default:
			}
			for {
				var v wrapperspb.StringValue
				if err := s.Recv(&v); err != nil {
					break
				}
				_ = s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value})
			}
			_ = s.end(&Status{Code: 0}, nil)
		}()
	}, defaultReceiveBuffer)
	cc := newClientConn(ctx, cliEnd)

	var sends, recvs int
	cc.interceptors = []Interceptor{
		StreamInterceptorFunc(func(next StreamingClientFunc) StreamingClientFunc {
			return func(ctx context.Context, spec MethodSpec, header map[string]string) (StreamingClientConn, error) {
				header["authorization"] = "Bearer s"
				conn, err := next(ctx, spec, header)
				if err != nil {
					return nil, err
				}
				return &countingStreamConn{StreamingClientConn: conn, sends: &sends, recvs: &recvs}, nil
			}
		}),
	}

	conn, err := OpenStreamingClient(ctx, cc, MethodSpec{Route: "/t/Bidi", Kind: StreamKindBidiStream}, nil)
	require.NoError(t, err)

	require.NoError(t, conn.Send(&wrapperspb.StringValue{Value: "a"}))
	require.NoError(t, conn.Send(&wrapperspb.StringValue{Value: "b"}))
	require.NoError(t, conn.CloseRequest())

	var got []string
	for {
		var v wrapperspb.StringValue
		err := conn.Receive(&v)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, v.Value)
	}
	require.Equal(t, []string{"echo:a", "echo:b"}, got)
	require.Equal(t, 2, sends)
	require.Equal(t, 2, recvs)

	auth := <-gotAuth
	require.Equal(t, "Bearer s", auth)
}
