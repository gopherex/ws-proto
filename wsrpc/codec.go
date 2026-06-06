package wsrpc

import (
	"context"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/protobuf/proto"
)

// frameConn is the abstract transport the Mux runs over. Implemented by
// pipeConn (tests) and wsConn (coder/websocket).
type frameConn interface {
	WriteFrame(ctx context.Context, f *transport.Frame) error
	ReadFrame(ctx context.Context) (*transport.Frame, error)
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
