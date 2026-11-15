package wsrpc

import (
	"context"
	"io"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCloseSendAfterTerminalPreservesResponse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	peer, local := newPipe()
	connection := newClientConn(ctx, local)
	defer connection.Close()
	stream, err := connection.NewStream(ctx, "/t/EarlyEnd", nil)
	require.NoError(t, err)
	_, err = peer.ReadFrame(ctx)
	require.NoError(t, err)
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: stream.id, Kind: transport.Kind_KIND_MSG, Payload: []byte{8, 7}}))
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: stream.id, Kind: transport.Kind_KIND_END}))
	select {
	case <-stream.ended:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, stream.CloseSend())
	message := new(wrapperspb.Int32Value)
	require.NoError(t, stream.Recv(message))
	require.Equal(t, int32(7), message.GetValue())
	require.ErrorIs(t, stream.Recv(message), io.EOF)
	select {
	case frame := <-peer.in:
		t.Fatalf("unexpected frame after terminal status: %s", frame.GetKind())
	default:
	}
}

func TestCancelledSendUnblocksCloseSend(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	peer, local := newPipe()
	connection := newClientConn(ctx, local)
	defer connection.Close()
	callContext, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	stream, err := connection.NewStream(callContext, "/t/HalfClose", nil)
	require.NoError(t, err)
	_, err = peer.ReadFrame(ctx)
	require.NoError(t, err)
	stream.mu.Lock()
	stream.sendWindow = 0
	stream.mu.Unlock()
	sent := make(chan error, 1)
	go func() { sent <- stream.SendRaw([]byte{1}) }()
	for stream.sendMu.TryLock() {
		stream.sendMu.Unlock()
		require.NoError(t, ctx.Err())
		runtime.Gosched()
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.CloseSend() }()
	cancelCall()
	select {
	case err := <-sent:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled SendRaw is still waiting for credit")
	}
	select {
	case err := <-closed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("CloseSend is still waiting for cancelled SendRaw")
	}
}

func TestTerminalWinsOverReturnedCredit(t *testing.T) {
	t.Parallel()
	for _, kind := range []transport.Kind{transport.Kind_KIND_END, transport.Kind_KIND_RST} {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				peer, local := newPipe()
				connection := newClientConn(ctx, local)
				defer connection.Close()
				stream, err := connection.NewStream(ctx, "/t/HalfClose", nil)
				require.NoError(t, err)
				_, err = peer.ReadFrame(ctx)
				require.NoError(t, err)
				stream.mu.Lock()
				stream.sendWindow = 0
				stream.mu.Unlock()
				done := make(chan error, 1)
				go func() { done <- stream.SendRaw([]byte{1}) }()
				synctest.Wait()
				// Both incoming frames become visible before the blocked sender runs.
				stream.mu.Lock()
				stream.sendWindow = 1
				stream.endFrame = &transport.Frame{Kind: kind}
				stream.endOne.Do(func() { close(stream.ended) })
				stream.sendCond.Broadcast()
				stream.mu.Unlock()
				// The send reports Recv's outcome: io.EOF for a clean END,
				// Canceled for a reset without a status.
				sendErr := <-done
				if kind == transport.Kind_KIND_END {
					require.ErrorIs(t, sendErr, io.EOF)
				} else {
					require.Equal(t, codes.Canceled, FromError(sendErr).Code)
				}
				select {
				case frame := <-peer.in:
					t.Fatalf("unexpected frame after terminal status: %s", frame.GetKind())
				default:
				}
			})
		})
	}
}

func TestCloseSendWaitsForBlockedSend(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	peer, local := newPipe()
	connection := newClientConn(ctx, local)
	defer connection.Close()
	stream, err := connection.NewStream(ctx, "/t/HalfClose", nil)
	require.NoError(t, err)
	_, err = peer.ReadFrame(ctx)
	require.NoError(t, err)
	stream.mu.Lock()
	stream.sendWindow = 0
	stream.mu.Unlock()
	sent := make(chan error, 1)
	go func() { sent <- stream.SendRaw([]byte{1}) }()
	for stream.sendMu.TryLock() {
		stream.sendMu.Unlock()
		require.NoError(t, ctx.Err())
		runtime.Gosched()
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.CloseSend() }()
	select {
	case err := <-closed:
		t.Fatalf("close overtook blocked send: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case frame := <-peer.in:
		t.Fatalf("frame %s overtook blocked MSG", frame.GetKind())
	default:
	}
	stream.creditSend(1)
	require.NoError(t, <-sent)
	require.NoError(t, <-closed)
	message, err := peer.ReadFrame(ctx)
	require.NoError(t, err)
	require.Equal(t, transport.Kind_KIND_MSG, message.GetKind())
	halfClose, err := peer.ReadFrame(ctx)
	require.NoError(t, err)
	require.Equal(t, transport.Kind_KIND_HALF_CLOSE, halfClose.GetKind())
}

func TestSendAfterCloseSend(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cc := windowedHarness(t, 8, map[string]Handler{
		"/t/HalfClose": func(ctx context.Context, stream *Stream) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	defer cc.Close()
	stream, err := cc.NewStream(ctx, "/t/HalfClose", nil)
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	require.NoError(t, stream.CloseSend())
	require.ErrorIs(t, stream.Send(wrapperspb.String("late")), io.ErrClosedPipe)
}

func TestCloseSendPreservesFlowControlledRequests(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cc := windowedHarness(t, 8, map[string]Handler{
		"/t/HalfClose": func(_ context.Context, stream *Stream) error {
			var count int32
			for {
				message := new(wrapperspb.StringValue)
				err := stream.Recv(message)
				if err == io.EOF {
					return stream.Send(wrapperspb.Int32(count))
				}
				if err != nil {
					return err
				}
				count++
			}
		},
	})
	defer cc.Close()
	stream, err := cc.NewStream(ctx, "/t/HalfClose", nil)
	require.NoError(t, err)
	for range 30 {
		require.NoError(t, stream.Send(wrapperspb.String("larger than window")))
	}
	require.NoError(t, stream.CloseSend())
	response := new(wrapperspb.Int32Value)
	require.NoError(t, stream.Recv(response))
	require.Equal(t, int32(30), response.GetValue())
	require.ErrorIs(t, stream.Recv(response), io.EOF)
}
