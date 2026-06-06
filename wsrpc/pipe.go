package wsrpc

import (
	"context"
	"errors"

	"github.com/gopherex/ws-proto/transport"
)

// pipeConn is an in-memory frameConn. newPipe returns the two connected ends.
type pipeConn struct {
	in   chan *transport.Frame // frames readable on this end
	out  chan *transport.Frame // frames written from this end
	done chan struct{}
}

func newPipe() (*pipeConn, *pipeConn) {
	ab := make(chan *transport.Frame, 16)
	ba := make(chan *transport.Frame, 16)
	done := make(chan struct{})
	a := &pipeConn{in: ba, out: ab, done: done}
	b := &pipeConn{in: ab, out: ba, done: done}
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

func (p *pipeConn) Close() error {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
	return nil
}
