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

// headerHarness wires a server mux + client conn over a pipe, flushing the
// server-side accumulated trailers on the END frame (like serveStream does).
func headerHarness(t *testing.T, handlers map[string]Handler) *ClientConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	srvEnd, cliEnd := newPipe()

	_ = newMux(ctx, srvEnd, func(s *Stream) {
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
			_ = s.end(st, s.takeTrailer())
		}()
	})

	return newClientConn(ctx, cliEnd)
}

// TestLeadingHeaderAndTrailer verifies a server handler can send leading headers
// via SendHeader (observed by the client via Header()) and trailers via
// SetTrailer (observed via Trailer() after END).
func TestLeadingHeaderAndTrailer(t *testing.T) {
	ctx := context.Background()
	cc := headerHarness(t, map[string]Handler{
		"/t/H": func(ctx context.Context, s *Stream) error {
			if err := s.SendHeader(map[string]string{"x-hdr": "1"}); err != nil {
				return err
			}
			if err := s.Send(&wrapperspb.StringValue{Value: "body"}); err != nil {
				return err
			}
			s.SetTrailer(map[string]string{"x-tlr": "2"})
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/H", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	var msg wrapperspb.StringValue
	require.NoError(t, s.Recv(&msg))
	require.Equal(t, "body", msg.Value)
	// Leading header is observed with/before the message.
	require.Equal(t, "1", s.Header()["x-hdr"])

	// Drain to END so trailers are applied.
	require.ErrorIs(t, s.Recv(&wrapperspb.StringValue{}), io.EOF)
	require.Equal(t, "2", s.Trailer()["x-tlr"])
}

// TestSendHeaderAfterMessageFails verifies SendHeader after the first Send
// returns FailedPrecondition.
func TestSendHeaderAfterMessageFails(t *testing.T) {
	ctx := context.Background()
	errCh := make(chan error, 1)
	cc := headerHarness(t, map[string]Handler{
		"/t/H": func(ctx context.Context, s *Stream) error {
			if err := s.Send(&wrapperspb.StringValue{Value: "body"}); err != nil {
				return err
			}
			errCh <- s.SendHeader(map[string]string{"x-hdr": "1"})
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/H", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	var msg wrapperspb.StringValue
	require.NoError(t, s.Recv(&msg))

	select {
	case got := <-errCh:
		require.Error(t, got)
		require.Equal(t, codes.FailedPrecondition, FromError(got).Code)
	case <-time.After(2 * time.Second):
		t.Fatal("handler never reported SendHeader result")
	}
}
