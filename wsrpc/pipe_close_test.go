package wsrpc

import (
	"context"
	"sync"
	"testing"
)

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
