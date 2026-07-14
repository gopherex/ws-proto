package wsrpc

import (
	"context"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/protobuf/proto"
)

// FrameConn is the abstract transport a Mux runs over. The production
// implementation wraps a coder/websocket connection; NewPipe returns an
// in-memory pair for tests and single-process topologies. DialConn and
// Server.ServeConn accept any FrameConn, so callers may also supply their own
// implementation (over net.Pipe, an io.Pipe, a Unix socket, etc.).
type FrameConn interface {
	WriteFrame(ctx context.Context, f *transport.Frame) error
	ReadFrame(ctx context.Context) (*transport.Frame, error)
	Ping(ctx context.Context) error
	Close() error
}

func marshalFrame(f *transport.Frame) ([]byte, error) {
	return proto.Marshal(f)
}

func unmarshalFrame(b []byte) (*transport.Frame, error) {
	f := &transport.Frame{}
	if err := proto.Unmarshal(b, f); err != nil {
		return nil, err
	}
	return f, nil
}
