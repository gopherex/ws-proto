package wsrpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/grpc/codes"
)

func TestTerminalFrameWakesBlockedSenders(t *testing.T) {
	t.Parallel()
	for _, kind := range []transport.Kind{transport.Kind_KIND_END, transport.Kind_KIND_RST} {
		for _, parked := range []bool{true, false} {
			name := kind.String() + "/concurrent"
			if parked {
				name = kind.String() + "/parked"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					const count = 128
					streams := make([]*Stream, 0, count)
					results := make(chan error, count)
					start := make(chan struct{})
					for id := uint32(1); id <= count; id++ {
						// No socket write is possible: each send has zero credit and
						// must terminate on END/RST without touching the mux.
						stream := newStream(ctx, &Mux{}, id, "/t/TerminalWake", 1)
						stream.sendWindow = 0
						streams = append(streams, stream)
						go func() {
							<-start
							results <- stream.SendRaw([]byte{1})
						}()
						if !parked {
							go func() {
								<-start
								stream.signalEnd(&transport.Frame{Kind: kind})
							}()
						}
					}
					close(start)
					if parked {
						synctest.Wait()
						for _, stream := range streams {
							go stream.signalEnd(&transport.Frame{Kind: kind})
						}
					}
					synctest.Wait()
					if len(results) != count {
						t.Errorf("%d senders remained parked after terminal frames", count-len(results))
						cancel()
						synctest.Wait()
						return
					}
					// The send reports Recv's outcome: a clean END is io.EOF, a
					// reset without a status is Canceled.
					for range count {
						err := <-results
						if kind == transport.Kind_KIND_END && !errors.Is(err, io.EOF) {
							t.Errorf("terminal send error: got %v, want EOF", err)
						}
						if kind == transport.Kind_KIND_RST && FromError(err).Code != codes.Canceled {
							t.Errorf("terminal send error: got %v, want Canceled", err)
						}
					}
				})
			})
		}
	}
}
