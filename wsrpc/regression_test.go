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

// BUG 1: a frame arriving after HALF_CLOSE must not panic the read loop.
func TestFrameAfterHalfCloseNoPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotStream := make(chan *Stream, 1)
	srvEnd, peer := newPipe()
	_ = newMux(ctx, srvEnd, func(s *Stream) { gotStream <- s })

	// Peer opens a stream, half-closes, then sends a stray MSG to the same id.
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_OPEN, Method: "/t/X"}))
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_HALF_CLOSE}))
	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: mustMarshalString(t, "late")}))

	s := <-gotStream
	// Recv should drain the buffered late MSG (or return EOF) without panicking.
	var v wrapperspb.StringValue
	_ = s.Recv(&v) // must not panic
	// Give the read loop a moment to process the stray frame.
	time.Sleep(20 * time.Millisecond)
}

// BUG 3: server streams are removed from the mux after the RPC completes.
func TestServerStreamRemovedAfterEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvEnd, cliEnd := newPipe()
	srv := NewServer()
	srv.Register("/t/U", func(ctx context.Context, s *Stream) error {
		var v wrapperspb.StringValue
		_ = s.Recv(&v)
		return s.Send(&wrapperspb.StringValue{Value: "ok"})
	})
	srvMux := newMux(ctx, srvEnd, func(s *Stream) { go srv.serveStream(s) })

	cc := newClientConn(ctx, cliEnd)
	st, err := cc.NewStream(ctx, "/t/U", nil)
	require.NoError(t, err)
	require.NoError(t, st.Send(&wrapperspb.StringValue{Value: "hi"}))
	require.NoError(t, st.CloseSend())
	var res wrapperspb.StringValue
	require.NoError(t, st.Recv(&res))
	require.Equal(t, "ok", res.Value)
	require.Equal(t, io.EOF, st.Recv(&wrapperspb.StringValue{}))

	// After completion the server-side stream must be gone from the registry.
	require.Eventually(t, func() bool {
		srvMux.mu.Lock()
		n := len(srvMux.streams)
		srvMux.mu.Unlock()
		return n == 0
	}, time.Second, 10*time.Millisecond)
}

func mustMarshalString(t *testing.T, v string) []byte {
	t.Helper()
	out, err := proto.Marshal(&wrapperspb.StringValue{Value: v})
	require.NoError(t, err)
	return out
}

// BUG 2: verify no data race on endSt by concurrently calling Recv and
// triggering failAll from a simulated connection drop.
func TestNoRaceOnEndSt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvEnd, peer := newPipe()
	gotStream := make(chan *Stream, 1)
	m := newMux(ctx, srvEnd, func(s *Stream) { gotStream <- s })

	require.NoError(t, peer.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_OPEN, Method: "/t/R"}))
	s := <-gotStream

	// Concurrently close peer (causes readLoop to call failAll) while Recv is
	// blocked — the race detector will catch unsynchronised access to endSt.
	go func() {
		time.Sleep(5 * time.Millisecond)
		_ = peer.Close()
	}()

	var v wrapperspb.StringValue
	err := s.Recv(&v)
	require.Error(t, err)
	_ = m
	_ = codes.OK // keep import used
}
