package wsrpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// windowedHarness wires a server mux + client conn over a pipe, both using the
// given per-stream initial credit window so flow-control blocking is exercised
// quickly with a small window.
func windowedHarness(t *testing.T, window int, handlers map[string]Handler) *ClientConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	srvEnd, cliEnd := newPipe()

	_ = newMuxConfig(ctx, srvEnd, func(s *Stream) {
		go func() {
			h := handlers[s.method]
			if h == nil {
				_ = s.end(&Status{Code: codes.Unimplemented}, nil)
				return
			}
			err := h(s.ctx, s)
			st := FromError(err)
			if st == nil {
				st = &Status{Code: codes.OK}
			}
			_ = s.end(st, nil)
		}()
	}, defaultReceiveBuffer, window, 0)

	cc := &ClientConn{
		mux:     newMuxConfig(ctx, cliEnd, nil, defaultReceiveBuffer, window, 0),
		waitCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
	}
	return cc
}

// bytesValue builds a payload whose marshaled size is roughly n bytes.
func bytesValue(n int) *wrapperspb.BytesValue {
	if n < 0 {
		n = 0
	}
	return &wrapperspb.BytesValue{Value: make([]byte, n)}
}

// TestBackpressureSlowConsumer: a server-streaming handler sends many large
// messages while the client reads slowly. With a small initial window the
// handler's Send must BLOCK waiting for credit rather than overflowing the
// bounded queue. All messages must arrive in order, none dropped/reset.
func TestBackpressureSlowConsumer(t *testing.T) {
	ctx := context.Background()
	const msgSize = 4 * 1024
	const count = 50
	const window = 16 * 1024 // 4 messages fit before Send must block

	cc := windowedHarness(t, window, map[string]Handler{
		"/t/Stream": func(ctx context.Context, s *Stream) error {
			for i := 0; i < count; i++ {
				v := bytesValue(msgSize)
				// Encode the index into the first 4 bytes to assert ordering.
				v.Value[0] = byte(i)
				v.Value[1] = byte(i >> 8)
				if err := s.Send(v); err != nil {
					return err
				}
			}
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/Stream", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	got := 0
	for {
		var v wrapperspb.BytesValue
		err := s.Recv(&v)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "no message must be dropped or reset under flow control")
		require.Len(t, v.Value, msgSize)
		idx := int(v.Value[0]) | int(v.Value[1])<<8
		require.Equal(t, got, idx, "messages must arrive in order")
		got++
		// Read slowly so the sender outpaces us and must block on credit.
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, count, got, "all messages must arrive")
}

// TestWindowUpdateUnblocksSend: fill the window so the sender's Send blocks;
// once the consumer reads (returning credit), the blocked Send must complete.
func TestWindowUpdateUnblocksSend(t *testing.T) {
	ctx := context.Background()
	const msgSize = 8 * 1024
	const window = 8 * 1024 // exactly one message fits before the window is spent

	sendDone := make(chan int, 16)
	release := make(chan struct{})

	cc := windowedHarness(t, window, map[string]Handler{
		"/t/Block": func(ctx context.Context, s *Stream) error {
			<-release
			for i := 0; i < 3; i++ {
				if err := s.Send(bytesValue(msgSize)); err != nil {
					return err
				}
				sendDone <- i
			}
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/Block", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	close(release)

	// First send goes through (window has room). The window is now spent
	// (1 msg == whole window) so the second Send must block until we read.
	select {
	case i := <-sendDone:
		require.Equal(t, 0, i)
	case <-time.After(time.Second):
		t.Fatal("first Send should not block")
	}

	// The second Send must NOT have completed yet (no credit returned).
	select {
	case i := <-sendDone:
		t.Fatalf("Send %d completed before credit was returned", i)
	case <-time.After(100 * time.Millisecond):
	}

	// Drain messages; consuming them returns credit (WINDOW_UPDATE) so the
	// blocked Send resumes.
	for i := 0; i < 3; i++ {
		var v wrapperspb.BytesValue
		require.NoError(t, s.Recv(&v))
	}
	require.Equal(t, io.EOF, s.Recv(&wrapperspb.BytesValue{}))
}

// TestMessageLargerThanWindow: with a tiny window, a single message bigger than
// the whole window must still be delivered (allowed once sendWindow > 0, then
// the window goes negative — no deadlock).
func TestMessageLargerThanWindow(t *testing.T) {
	ctx := context.Background()
	const window = 1024
	const msgSize = 64 * 1024 // far larger than the window

	cc := windowedHarness(t, window, map[string]Handler{
		"/t/Big": func(ctx context.Context, s *Stream) error {
			return s.Send(bytesValue(msgSize))
		},
	})

	s, err := cc.NewStream(ctx, "/t/Big", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	done := make(chan error, 1)
	go func() {
		var v wrapperspb.BytesValue
		if err := s.Recv(&v); err != nil {
			done <- err
			return
		}
		require.Len(t, v.Value, msgSize)
		done <- nil
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "an oversized message must be delivered, not deadlock")
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: oversized message was never delivered")
	}
}

// TestBlockedSendNoHoLRegression: a Send blocked on stream A (window exhausted,
// nobody reading) must not stall delivery/credit on stream B. The read loop must
// keep running. We verify B completes a full request/response while A is parked.
func TestBlockedSendNoHoLRegression(t *testing.T) {
	ctx := context.Background()
	const window = 4 * 1024

	aBlocked := make(chan struct{})
	cc := windowedHarness(t, window, map[string]Handler{
		// A spends its window then blocks forever on the next Send (nobody reads A).
		"/t/A": func(ctx context.Context, s *Stream) error {
			_ = s.Send(bytesValue(window)) // fills the window exactly
			close(aBlocked)
			return s.Send(bytesValue(window)) // blocks: no credit ever returns
		},
		// B is a simple unary echo.
		"/t/B": func(ctx context.Context, s *Stream) error {
			var v wrapperspb.StringValue
			if err := s.Recv(&v); err != nil {
				return err
			}
			return s.Send(&wrapperspb.StringValue{Value: "B:" + v.Value})
		},
	})

	// Open A; let its handler spend the window and park on the second Send.
	sa, err := cc.NewStream(ctx, "/t/A", nil)
	require.NoError(t, err)
	require.NoError(t, sa.CloseSend())
	select {
	case <-aBlocked:
	case <-time.After(time.Second):
		t.Fatal("A did not reach its blocked Send")
	}

	// While A's Send is parked, B must complete promptly: the read loop and
	// per-stream blocking must not interfere with another stream.
	sb, err := cc.NewStream(ctx, "/t/B", nil)
	require.NoError(t, err)
	require.NoError(t, sb.Send(&wrapperspb.StringValue{Value: "hi"}))
	require.NoError(t, sb.CloseSend())

	done := make(chan string, 1)
	go func() {
		var v wrapperspb.StringValue
		if err := sb.Recv(&v); err == nil {
			done <- v.Value
		} else {
			done <- "err:" + err.Error()
		}
	}()
	select {
	case got := <-done:
		require.Equal(t, "B:hi", got, "B must complete while A's Send is blocked")
	case <-time.After(2 * time.Second):
		t.Fatal("head-of-line: B stalled while A's Send was blocked on credit")
	}

	_ = sa
}
