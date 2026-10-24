package wsrpc

import (
	"context"
	"errors"
	"sync"

	"github.com/gopherex/ws-proto/transport"
)

// pipeConn is an in-memory FrameConn. newPipe returns the two connected ends.
type pipeConn struct {
	in   chan *transport.Frame // frames readable on this end
	out  chan *transport.Frame // frames written from this end
	done chan struct{}
	// closeDone closes done once, whichever end closes first or how many
	// times; shared by both ends, as done is.
	closeDone func()
}

func newPipe() (*pipeConn, *pipeConn) {
	ab := make(chan *transport.Frame, 16)
	ba := make(chan *transport.Frame, 16)
	done := make(chan struct{})
	closeDone := sync.OnceFunc(func() { close(done) })
	a := &pipeConn{in: ba, out: ab, done: done, closeDone: closeDone}
	b := &pipeConn{in: ab, out: ba, done: done, closeDone: closeDone}
	return a, b
}

// NewPipe returns two connected in-memory FrameConn ends: a frame written on
// one is readable on the other, and either Close unblocks the peer.
//
// NewPipe is the ws-proto analog of google.golang.org/grpc/test/bufconn: pair
// it with DialConn and (*Server).ServeConn to run a client and server in one
// process with no socket and no WebSocket handshake:
//
//	srvEnd, cliEnd := wsrpc.NewPipe()
//	go srv.ServeConn(ctx, srvEnd)
//	cc, _ := wsrpc.DialConn(ctx, cliEnd)
//	client := echov1.NewEchoServiceWSClient(cc)
func NewPipe() (FrameConn, FrameConn) {
	a, b := newPipe()
	return a, b
}

var errPipeClosed = errors.New("wsrpc: pipe closed")

func (p *pipeConn) WriteFrame(ctx context.Context, f *transport.Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.done:
		return errPipeClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *pipeConn) ReadFrame(ctx context.Context) (*transport.Frame, error) {
	select {
	case f := <-p.in:
		return f, nil
	case <-p.done:
		return nil, errPipeClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeConn) Ping(ctx context.Context) error { return nil }

// Close closes both ends; it is safe to call concurrently and repeatedly.
func (p *pipeConn) Close() error {
	p.closeDone()
	return nil
}
