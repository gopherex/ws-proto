package example_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// startRustServer launches the Rust example server binary named by
// WS_PROTO_RUST_SERVER (built by `make test-interop`) and returns its ws URL.
// Skipped when the variable is unset so plain `go test ./...` needs no cargo.
// WS_PROTO_IMPL=tonic (inherited by the child) selects the tonic-bridge impl.
func startRustServer(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("WS_PROTO_RUST_SERVER")
	if bin == "" {
		t.Skip("WS_PROTO_RUST_SERVER not set (run via make test-interop)")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "WS_PROTO_TEST_ADDR="+addr)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	ready := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		ready <- line
	}()
	select {
	case line := <-ready:
		require.True(t, strings.HasPrefix(line, "LISTENING"), "unexpected: %q", line)
	case <-time.After(20 * time.Second):
		t.Fatal("rust server did not become ready in 20s")
	}
	return "ws://" + addr + "/"
}

func dialRust(t *testing.T) (echov1.EchoServiceWSClient, *wsrpc.ClientConn) {
	t.Helper()
	cc, err := wsrpc.Dial(context.Background(), startRustServer(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return echov1.NewEchoServiceWSClient(cc), cc
}

// TestRustServer drives the generated Go client against the Rust server with
// the same scenarios the Rust client runs against the Go server.
func TestRustServer(t *testing.T) {
	client, cc := dialRust(t)
	ctx := context.Background()

	t.Run("unary", func(t *testing.T) {
		res, err := client.Unary(ctx, &echov1.UnaryRequest{Name: "bob"})
		require.NoError(t, err)
		require.Equal(t, "hello bob", res.Greeting)
	})

	t.Run("unary metadata", func(t *testing.T) {
		s, err := cc.NewStream(ctx, "/echo.v1.EchoService/Unary", map[string]string{"x-echo-md": "v1"})
		require.NoError(t, err)
		require.NoError(t, s.Send(&echov1.UnaryRequest{Name: "md"}))
		require.NoError(t, s.CloseSend())
		res := new(echov1.UnaryResponse)
		require.NoError(t, s.Recv(res))
		require.Equal(t, io.EOF, s.Recv(new(echov1.UnaryResponse)))
		require.Equal(t, "v1", s.Header()["x-echo-md"])
		// tonic responses carry metadata only; the bridge maps it to the leading
		// header and has no trailers to forward (WS_PROTO_IMPL=tonic).
		if os.Getenv("WS_PROTO_IMPL") != "tonic" {
			require.Equal(t, "v1", s.Trailer()["x-echo-trailer"])
		}
	})

	t.Run("server stream", func(t *testing.T) {
		stream, err := client.ServerStream(ctx, &echov1.ServerStreamRequest{Count: 3})
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
	})

	t.Run("client stream", func(t *testing.T) {
		stream, err := client.ClientStream(ctx)
		require.NoError(t, err)
		for i := int32(1); i <= 4; i++ {
			require.NoError(t, stream.Send(&echov1.ClientStreamRequest{Value: i}))
		}
		res, err := stream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, int32(10), res.Sum)
	})

	t.Run("bidi", func(t *testing.T) {
		stream, err := client.Bidi(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&echov1.BidiRequest{Text: "a"}))
		r1, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, "echo:a", r1.Echo)
		require.NoError(t, stream.CloseSend())
		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})

	t.Run("unknown method", func(t *testing.T) {
		s, err := cc.NewStream(ctx, "/echo.v1.EchoService/Nope", nil)
		require.NoError(t, err)
		require.NoError(t, s.CloseSend())
		err = s.Recv(new(echov1.UnaryResponse))
		st, ok := err.(*wsrpc.Status)
		require.True(t, ok, "got %T %v", err, err)
		require.Equal(t, codes.Unimplemented, st.Code)
		if os.Getenv("WS_PROTO_IMPL") != "tonic" { // tonic routes internally: empty message
			require.Equal(t, "unknown method /echo.v1.EchoService/Nope", st.Message)
		}
	})

	t.Run("flow control oversized", func(t *testing.T) {
		big := strings.Repeat("x", 300*1024)
		stream, err := client.Bidi(ctx)
		require.NoError(t, err)
		for i := 0; i < 4; i++ {
			require.NoError(t, stream.Send(&echov1.BidiRequest{Text: big}))
			r, err := stream.Recv()
			require.NoError(t, err)
			require.Len(t, r.Echo, len(big)+5)
		}
		require.NoError(t, stream.CloseSend())
		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})

	t.Run("many small messages", func(t *testing.T) {
		stream, err := client.ServerStream(ctx, &echov1.ServerStreamRequest{Count: 20000})
		require.NoError(t, err)
		var n int32
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			require.Equal(t, n, res.Index)
			n++
		}
		require.Equal(t, int32(20000), n)
	})

	t.Run("deadline", func(t *testing.T) {
		dctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		stream, err := client.Bidi(dctx)
		require.NoError(t, err)
		// Never send: the Rust server ends the stream once ws-timeout-ms elapses.
		_, err = stream.Recv()
		require.Error(t, err)
	})

	t.Run("concurrent", func(t *testing.T) {
		errs := make(chan error, 50)
		for i := 0; i < 50; i++ {
			go func(i int) {
				res, err := client.Unary(ctx, &echov1.UnaryRequest{Name: "c"})
				if err == nil && res.Greeting != "hello c" {
					err = io.ErrUnexpectedEOF
				}
				errs <- err
			}(i)
		}
		for i := 0; i < 50; i++ {
			require.NoError(t, <-errs)
		}
	})
}
