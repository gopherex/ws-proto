package wsrpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// RecvContext ends with its own context and leaves the stream as it was: a
// buffered message is delivered first, a cancelled receive takes nothing,
// and the terminal status still comes after.
func TestRecvContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) { gotStream <- s }, defaultReceiveBuffer)

	s := openServerStream(t, ctx, peer, gotStream, 1, "/t/M")
	send := func(v string) {
		require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{
			StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, v),
		}))
	}

	// A receive bounded by its own deadline returns with it; nothing is lost.
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()

	var v wrapperspb.StringValue
	require.ErrorIs(t, s.RecvContext(short, &v), context.DeadlineExceeded)

	// A receiver parked on a cancellable context is released at once, so its
	// owner can wait for it before returning.
	parked, release := context.WithCancel(ctx)
	done := make(chan error, 1)

	go func() {
		var m wrapperspb.StringValue
		done <- s.RecvContext(parked, &m)
	}()

	time.Sleep(20 * time.Millisecond)
	release()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("a cancelled receive did not return")
	}

	// The stream still works: a later message reaches a plain Recv.
	send("first")
	require.NoError(t, s.Recv(&v))
	require.Equal(t, "first", v.GetValue())

	// Drain first: a buffered message comes even when the context is done.
	send("second")
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return len(s.recvItems) == 1
	}, time.Second, time.Millisecond)

	gone, cancelGone := context.WithCancel(ctx)
	cancelGone()

	raw, err := s.RecvRawContext(gone)
	require.NoError(t, err)
	require.Equal(t, mustMarshalString(t, "second"), raw)

	// The terminal status is kept: after the peer half-closes, io.EOF.
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_HALF_CLOSE}))
	require.True(t, errors.Is(s.RecvContext(ctx, &v), io.EOF))
}

func BenchmarkRecvRawContext(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMuxBuffered(ctx, srvEnd, func(s *Stream) { gotStream <- s }, defaultReceiveBuffer)

	s := openServerStream(b, ctx, peer, gotStream, 1, "/t/M")
	payload := mustMarshalString(b, "x")

	// The peer reads the window updates the receives send back; unread, they
	// would fill the pipe and stall the stream.
	go func() {
		for {
			if _, err := peer.ReadFrame(ctx); err != nil {
				return
			}
		}
	}()

	for b.Loop() {
		if err := peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: payload}); err != nil {
			b.Fatal(err)
		}

		if _, err := s.RecvRawContext(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
