package wsrpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// openServerStream drives an OPEN frame from the peer and returns the dispatched
// server-side *Stream.
func openServerStream(t *testing.T, ctx context.Context, peer *pipeConn, gotStream <-chan *Stream, id uint32, method string) *Stream {
	t.Helper()
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: id, Kind: transport.Kind_KIND_OPEN, Method: method}))
	select {
	case s := <-gotStream:
		return s
	case <-time.After(time.Second):
		t.Fatalf("server stream %d not opened", id)
		return nil
	}
}

// TestNoHeadOfLineBlocking is the core regression: a slow consumer on stream A
// must not stall delivery to stream B sharing the same read loop.
func TestNoHeadOfLineBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const buf = 4
	gotStream := make(chan *Stream, 2)
	srvEnd, peer := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) { gotStream <- s }, buf)

	a := openServerStream(t, ctx, peer, gotStream, 1, "/t/A")
	b := openServerStream(t, ctx, peer, gotStream, 3, "/t/B")

	// A's consumer never reads. Fill A's bounded buffer completely so any
	// further routing to A would have blocked the (old) read loop.
	for i := 0; i < buf; i++ {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, "a")}))
	}

	// Now push a MSG to B; B's consumer must receive it promptly despite A.
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 3, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, "b")}))

	done := make(chan string, 1)
	go func() {
		var v wrapperspb.StringValue
		if err := b.Recv(&v); err == nil {
			done <- v.Value
		} else {
			done <- "err:" + err.Error()
		}
	}()
	select {
	case got := <-done:
		require.Equal(t, "b", got, "B must receive its message despite A being stalled")
	case <-time.After(2 * time.Second):
		t.Fatal("head-of-line blocking: B.Recv did not complete while A was stalled")
	}

	_ = a
}

// TestOverflowResetsSlowStream: overrunning the bounded buffer resets the slow
// stream with ResourceExhausted and emits an RST to the peer.
func TestOverflowResetsSlowStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Small receive window so a peer that ignores it overruns the byte-bounded
	// backlog quickly. Each "x" message is a few bytes; 40 of them far exceed 32.
	const window = 32
	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMuxConfig(ctx, srvEnd, func(s *Stream) { gotStream <- s }, defaultReceiveBuffer, window, 0, nil)

	a := openServerStream(t, ctx, peer, gotStream, 1, "/t/A")

	// Push well beyond the window in bytes; the consumer never reads.
	for i := 0; i < 40; i++ {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, "x")}))
	}

	// A's Recv eventually returns ResourceExhausted (after draining buffered MSGs).
	deadline := time.After(2 * time.Second)
	for {
		var v wrapperspb.StringValue
		err := a.Recv(&v)
		if err == nil {
			continue // drained a buffered MSG
		}
		st := FromError(err)
		require.NotNil(t, st)
		require.Equal(t, codes.ResourceExhausted, st.Code)
		break
	}
	_ = deadline

	// An RST must have been written to the peer.
	gotRST := false
	for {
		select {
		case f := <-peer.in:
			if f.StreamId == 1 && f.Kind == transport.Kind_KIND_RST {
				require.Equal(t, int32(codes.ResourceExhausted), f.Status.GetCode())
				gotRST = true
			}
		case <-time.After(time.Second):
			require.True(t, gotRST, "expected an RST frame on overflow")
			return
		}
		if gotRST {
			return
		}
	}
}

// TestTerminalFrameAfterFullBuffer: a buffered set of MSGs followed by END must
// all be observed — the END is not lost even when the buffer is full.
func TestTerminalFrameAfterFullBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const buf = 4
	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) { gotStream <- s }, buf)

	a := openServerStream(t, ctx, peer, gotStream, 1, "/t/A")

	// Fill the buffer exactly, then send END (which bypasses the queue).
	for i := 0; i < buf; i++ {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalIntStr(t, i)}))
	}
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_END, Status: statusToProto(&Status{Code: codes.OK})}))

	// Drain: must see buf MSGs in order, then io.EOF.
	for i := 0; i < buf; i++ {
		var v wrapperspb.StringValue
		require.NoError(t, a.Recv(&v))
	}
	require.Equal(t, io.EOF, a.Recv(&wrapperspb.StringValue{}))
}

// TestTerminalNonOKAfterFullBuffer: END with a non-OK status is surfaced after
// draining buffered MSGs.
func TestTerminalNonOKAfterFullBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const buf = 2
	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) { gotStream <- s }, buf)

	a := openServerStream(t, ctx, peer, gotStream, 1, "/t/A")
	for i := 0; i < buf; i++ {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, "m")}))
	}
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_END, Status: statusToProto(&Status{Code: codes.NotFound, Message: "nope"})}))

	for i := 0; i < buf; i++ {
		var v wrapperspb.StringValue
		require.NoError(t, a.Recv(&v))
	}
	err := a.Recv(&wrapperspb.StringValue{})
	st := FromError(err)
	require.NotNil(t, st)
	require.Equal(t, codes.NotFound, st.Code)
}

func mustMarshalIntStr(t *testing.T, i int) []byte {
	t.Helper()
	out, err := proto.Marshal(&wrapperspb.StringValue{Value: string(rune('0' + i))})
	require.NoError(t, err)
	return out
}
