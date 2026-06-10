package wsrpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestReceiveBoundIsByteBasedNotFrameCount reproduces M3: the receive backlog
// must be bounded by BYTES (aligned with the flow-control window), not by a
// fixed frame count. With the default 256 KiB window a well-behaved peer may
// have thousands of tiny messages in flight; a 256-FRAME cap would falsely reset
// such a stream with ResourceExhausted even though it obeys the window.
func TestReceiveBoundIsByteBasedNotFrameCount(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) { gotStream <- s }, defaultReceiveBuffer)

	s := openServerStream(t, ctx, peer, gotStream, 1, "/t/M")

	// 300 tiny messages: far more than the 256-frame count, but only ~900 bytes
	// total — well under the 256 KiB window. A window-obeying peer would send
	// all of these without waiting for credit.
	const n = 300
	for i := 0; i < n; i++ {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{
			StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, "x"),
		}))
	}
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_HALF_CLOSE}))

	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	count := 0
	for {
		var v wrapperspb.StringValue
		err := s.Recv(&v)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "window-obeying peer was falsely reset after %d msgs", count)
		count++
		_ = rctx
	}
	require.Equal(t, n, count)
}
