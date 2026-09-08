package wsrpc

import (
	"context"
	"io"
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

func TestServerEndTerminatesStream(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler Handler
		code    codes.Code
	}{
		{"ok", func(context.Context, *Stream) error { return nil }, codes.OK},
		{"error", func(context.Context, *Stream) error {
			return Errorf(codes.Unavailable, "upstream stopped")
		}, codes.Unavailable},
		{"panic", func(context.Context, *Stream) error { panic("upstream panic") }, codes.Internal},
		{"unknown", nil, codes.Unimplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer()
			if tc.handler != nil {
				srv.Register("/t/Sync", tc.handler)
			}
			srvEnd, peer := newPipe()
			gotStream := make(chan *Stream, 1)
			m := newMux(context.Background(), srvEnd, func(s *Stream) {
				gotStream <- s
				go srv.serveStream(s)
			})
			t.Cleanup(func() { _ = m.Close() })
			readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, peer.WriteFrame(readCtx, &transport.Frame{
				StreamId: 1, Kind: transport.Kind_KIND_OPEN, Method: "/t/Sync",
			}))
			f, err := peer.ReadFrame(readCtx)
			require.NoError(t, err)
			require.Equal(t, transport.Kind_KIND_END, f.Kind)
			require.Equal(t, int32(tc.code), f.Status.Code)
			s := <-gotStream
			t.Cleanup(s.cancel)
			select {
			case <-s.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("server stream context still live after END")
			}
			select {
			case <-s.ended:
			default:
				t.Fatal("server stream not terminal after END")
			}
			select {
			case <-s.halfClosed:
				t.Fatal("client send side unexpectedly closed")
			default:
			}
			// Both raw and typed reads, including repeated reads, retain the
			// terminal status instead of exposing context.Canceled.
			_, rawErr := s.RecvRaw()
			typedErr := s.Recv(&transport.Frame{})
			for _, err := range []error{rawErr, typedErr} {
				if tc.code == codes.OK {
					require.ErrorIs(t, err, io.EOF)
				} else {
					require.Error(t, err)
					require.Equal(t, tc.code, FromError(err).Code)
				}
			}
			require.NoError(t, m.ctx.Err(), "connection must remain live")
		})
	}
}

func TestEndWriteFailureTerminatesStream(t *testing.T) {
	conn := newBlockConn()
	conn.blockWrites.Store(true)
	require.NoError(t, conn.Close())
	// No mux read loop: write failure itself must trigger stream cleanup.
	m := &Mux{ctx: context.Background(), conn: conn}
	s := newStream(m.ctx, m, 1, "/t/Sync", defaultInitialWindow)
	defer s.cancel()
	require.ErrorIs(t, s.end(&Status{Code: codes.Unavailable}, nil), errBlockClosed)
	require.ErrorIs(t, s.Context().Err(), context.Canceled)
	select {
	case <-s.ended:
	default:
		t.Fatal("failed END write left stream non-terminal")
	}
}
