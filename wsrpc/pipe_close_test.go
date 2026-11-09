package wsrpc

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gopherex/ws-proto/transport"
)

// Frames written before Close are read before the close is reported, every
// time: the read must not pick the close over a queued frame.
func TestPipeDeliversQueuedFramesBeforeClose(t *testing.T) {
	t.Parallel()
	for range 200 {
		a, b := NewPipe()
		for id := range uint32(3) {
			if err := a.WriteFrame(t.Context(), &transport.Frame{StreamId: id}); err != nil {
				t.Fatal(err)
			}
		}
		_ = a.Close()
		for id := range uint32(3) {
			f, err := b.ReadFrame(t.Context())
			if err != nil || f.GetStreamId() != id {
				t.Fatalf("frame %d: got %v, %v", id, f, err)
			}
		}
		if _, err := b.ReadFrame(t.Context()); !errors.Is(err, errPipeClosed) {
			t.Fatalf("after the queued frames: %v, want errPipeClosed", err)
		}
	}
}

// Both ends close at once, repeatedly, through the pipe and through the
// muxes and client over it: no end panics on an already closed pipe.
func TestPipeConcurrentClose(t *testing.T) {
	for range 200 {
		a, b := NewPipe()
		var closing sync.WaitGroup
		for _, end := range []FrameConn{a, b, a, b} {
			closing.Go(func() { _ = end.Close() })
		}
		closing.Wait()
	}

	srv := NewServer(WithSameOriginOnly())
	for range 200 {
		srvEnd, cliEnd := NewPipe()
		ctx, cancel := context.WithCancel(t.Context())
		served := make(chan struct{})
		go func() { srv.ServeConn(ctx, srvEnd); close(served) }()
		cc, err := DialConn(t.Context(), cliEnd)
		if err != nil {
			t.Fatal(err)
		}
		var closing sync.WaitGroup
		closing.Go(func() { _ = cc.Close() })
		closing.Go(func() { _ = srvEnd.Close() })
		closing.Go(cancel)
		closing.Wait()
		<-served
	}
}
