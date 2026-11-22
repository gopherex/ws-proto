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

// A reader that took the nonblocking check on an empty queue, then found a
// frame and the close both ready (the writer queued and closed in between),
// still returns the frame before the close. awaitFrame is that second stage;
// before the fix select chose between the two at random.
func TestPipeAwaitPrefersQueuedFrameOverClose(t *testing.T) {
	t.Parallel()
	for range 200 {
		a, b := newPipe()
		if err := a.WriteFrame(t.Context(), &transport.Frame{StreamId: 7}); err != nil {
			t.Fatal(err)
		}
		_ = a.Close()
		f, err := b.awaitFrame(t.Context())
		if err != nil || f.GetStreamId() != 7 {
			t.Fatalf("got %v, %v; want the queued frame before the close", f, err)
		}
		if _, err := b.awaitFrame(t.Context()); !errors.Is(err, errPipeClosed) {
			t.Fatalf("after the queued frame: %v, want errPipeClosed", err)
		}
	}
}

// A write after Close fails instead of queueing a frame nobody reads.
func TestPipeWriteAfterCloseFails(t *testing.T) {
	t.Parallel()
	for range 200 {
		a, b := newPipe()
		_ = b.Close()
		if err := a.WriteFrame(t.Context(), &transport.Frame{}); !errors.Is(err, errPipeClosed) {
			t.Fatalf("write after close: %v, want errPipeClosed", err)
		}
	}
}

func BenchmarkPipeRoundTrip(b *testing.B) {
	a, z := newPipe()
	defer a.Close()
	f := &transport.Frame{StreamId: 1}
	for b.Loop() {
		if err := a.WriteFrame(b.Context(), f); err != nil {
			b.Fatal(err)
		}
		if _, err := z.ReadFrame(b.Context()); err != nil {
			b.Fatal(err)
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
