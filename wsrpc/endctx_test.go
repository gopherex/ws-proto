package wsrpc

import (
	"context"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// TestEndTransmittedAfterStreamContextCancelled reproduces M1: when a server
// stream's context is already cancelled (e.g. its deadline fired), the terminal
// END frame carrying the final status/trailers must still reach the client.
// Writing END with the cancelled stream context drops it on the floor, so the
// client never learns the real status. END must be written with a live context.
func TestEndTransmittedAfterStreamContextCancelled(t *testing.T) {
	conn := newBlockConn()
	gotStream := make(chan *Stream, 1)
	m := newMuxBuffered(context.Background(), conn, func(s *Stream) { gotStream <- s }, defaultReceiveBuffer)
	defer m.Close()

	conn.reads <- &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_OPEN, Method: "/t/M"}
	var s *Stream
	select {
	case s = <-gotStream:
	case <-time.After(time.Second):
		t.Fatal("server stream not dispatched")
	}

	// Simulate a fired deadline: the stream context is cancelled before end().
	s.cancel()

	err := s.end(&Status{Code: codes.DeadlineExceeded, Message: "deadline exceeded"}, nil)
	require.NoError(t, err, "end() should still transmit despite a cancelled stream context")

	select {
	case f := <-conn.writes:
		require.Equal(t, uint32(1), f.StreamId)
		require.Equal(t, transport.Kind_KIND_END, f.Kind)
		require.NotNil(t, f.Status)
		require.Equal(t, int32(codes.DeadlineExceeded), f.Status.Code)
	case <-time.After(time.Second):
		t.Fatal("END frame was not transmitted")
	}
}
