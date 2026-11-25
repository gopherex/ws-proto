package gopeer_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	echo "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/transport"
	"github.com/gopherex/ws-proto/wsrpc"
	"google.golang.org/protobuf/proto"
)

type peer struct {
	input     io.WriteCloser
	output    io.ReadCloser
	closeOnce sync.Once
}

func (p *peer) WriteFrame(_ context.Context, f *transport.Frame) error {
	b, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	if len(b) > 4<<20 {
		return errors.New("frame too large")
	}
	_, err = p.input.Write(append(binary.BigEndian.AppendUint32(nil, uint32(len(b))), b...))
	return err
}
func (p *peer) ReadFrame(context.Context) (*transport.Frame, error) {
	var n uint32
	if err := binary.Read(p.output, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if n > 4<<20 {
		return nil, errors.New("frame too large")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(p.output, b); err != nil {
		return nil, err
	}
	f := new(transport.Frame)
	return f, proto.Unmarshal(b, f)
}
func (*peer) Ping(context.Context) error { return nil }
func (p *peer) Close() error {
	p.closeOnce.Do(func() { p.input.Close(); p.output.Close() })
	return nil
}

func TestGoClientCppServer(t *testing.T) {
	exe := os.Getenv("WS_PROTO_CPP_SERVER")
	if exe == "" {
		t.Skip("CTest supplies the C++ server executable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe)
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &peer{input: in, output: out}
	defer func() {
		p.Close()
		if err := cmd.Wait(); err != nil {
			t.Error(err)
		}
	}()
	cc, err := wsrpc.DialConn(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	api := echo.NewEchoServiceWSClient(cc)
	u, err := api.Unary(ctx, &echo.UnaryRequest{Name: "Go"})
	if err != nil || u.GetGreeting() != "hello Go" {
		t.Fatalf("unary: %v %v", u, err)
	}
	s, err := api.ServerStream(ctx, &echo.ServerStreamRequest{Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		msg, err := s.Recv()
		if err != nil || msg.GetIndex() != i {
			t.Fatalf("download: %v %v", msg, err)
		}
	}
	if _, err = s.Recv(); err != io.EOF {
		t.Fatalf("download EOF: %v", err)
	}
	up, err := api.ClientStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []int32{3, 4} {
		if err := up.Send(&echo.ClientStreamRequest{Value: v}); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := up.CloseAndRecv()
	if err != nil || sum.GetSum() != 7 {
		t.Fatalf("upload: %v %v", sum, err)
	}
	b, err := api.Bidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Send(&echo.BidiRequest{Text: "before half-close"}); err != nil {
		t.Fatal(err)
	}
	msg, err := b.Recv()
	if err != nil || msg.GetEcho() != "before half-close" {
		t.Fatalf("bidi: %v %v", msg, err)
	}
	if err = b.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Recv(); err != io.EOF {
		t.Fatalf("bidi EOF: %v", err)
	}
}
