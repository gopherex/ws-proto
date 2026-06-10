package wsrpc

import (
	"context"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// TestMaxConcurrentStreamsRejectsOverLimit verifies the server caps the number
// of concurrent server streams: once the limit is reached, a further OPEN is
// answered with an RST(ResourceExhausted) and is NOT dispatched to a handler.
func TestMaxConcurrentStreamsRejectsOverLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const maxStreams = 2
	gotStream := make(chan *Stream, 8)
	srvEnd, peer := newPipe()
	_ = newMuxConfig(ctx, srvEnd, func(s *Stream) { gotStream <- s }, defaultReceiveBuffer, defaultInitialWindow, maxStreams)

	// First two OPENs are dispatched (handlers never end them, so they stay live).
	for _, id := range []uint32{1, 3} {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: id, Kind: transport.Kind_KIND_OPEN, Method: "/t/M"}))
		select {
		case <-gotStream:
		case <-time.After(time.Second):
			t.Fatalf("stream %d not dispatched", id)
		}
	}

	// The third OPEN exceeds the cap: it must be rejected, not dispatched.
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 5, Kind: transport.Kind_KIND_OPEN, Method: "/t/M"}))

	// Expect an RST(ResourceExhausted) back on the peer for stream 5.
	rst := readFrameTimeout(t, ctx, peer, time.Second)
	require.Equal(t, uint32(5), rst.StreamId)
	require.Equal(t, transport.Kind_KIND_RST, rst.Kind)
	require.NotNil(t, rst.Status)
	require.Equal(t, int32(codes.ResourceExhausted), rst.Status.Code)

	// And it must NOT have been dispatched to a handler.
	select {
	case <-gotStream:
		t.Fatal("over-limit stream was dispatched to a handler")
	case <-time.After(100 * time.Millisecond):
	}
}

// readFrameTimeout reads one frame from peer or fails after d.
func readFrameTimeout(t *testing.T, ctx context.Context, peer *pipeConn, d time.Duration) *transport.Frame {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	f, err := peer.ReadFrame(rctx)
	require.NoError(t, err)
	return f
}
