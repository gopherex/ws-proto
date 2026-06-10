package wsrpc

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestConcurrentSendRecvDuringDisconnect stresses the terminal-state machine
// (endSt / endFrame / ended / ctx, plus the send-cond and recv-signal wakeups)
// by racing in-flight Send and Recv against an abrupt transport disconnect.
// Run under -race, it guards against data races and missed wakeups on the
// teardown path: every goroutine must observe a terminal error and exit; none
// may hang or panic.
func TestConcurrentSendRecvDuringDisconnect(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		ctx, cancel := context.WithCancel(context.Background())

		srvEnd, cliEnd := newPipe()
		_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) {
			go func() {
				for {
					var v wrapperspb.StringValue
					if err := s.Recv(&v); err != nil {
						return
					}
					if err := s.Send(&v); err != nil {
						return
					}
				}
			}()
		}, defaultReceiveBuffer)

		cc := newClientConn(ctx, cliEnd)
		s, err := cc.NewStream(ctx, "/t/Echo", nil)
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := s.Send(&wrapperspb.StringValue{Value: "x"}); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				var v wrapperspb.StringValue
				if err := s.Recv(&v); err != nil {
					return // terminal error (disconnect) or EOF
				}
			}
		}()

		// Abruptly drop the transport mid-flight: the read loop fails, every
		// in-flight stream must be torn down and both goroutines must exit.
		_ = cliEnd.Close()

		wg.Wait()
		cancel()
	}
}
