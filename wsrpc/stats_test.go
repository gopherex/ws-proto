package wsrpc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// statsRecorder collects ServerStats events under a lock (callbacks run on the
// read loop and handler goroutines concurrently).
type statsRecorder struct {
	mu sync.Mutex

	connOpened, connClosed int
	connRejected           []string
	streamsStarted         []string
	streamsEnded           map[string]codes.Code
	streamsRejected        []string
	sentBytes, recvBytes   int
	sentMsgs, recvMsgs     int
}

func newStatsRecorder() *statsRecorder {
	return &statsRecorder{streamsEnded: make(map[string]codes.Code)}
}

func (r *statsRecorder) stats() *wsrpc.ServerStats {
	return &wsrpc.ServerStats{
		ConnOpened: func(context.Context) { r.mu.Lock(); r.connOpened++; r.mu.Unlock() },
		ConnClosed: func(context.Context) { r.mu.Lock(); r.connClosed++; r.mu.Unlock() },
		ConnRejected: func(reason string) {
			r.mu.Lock()
			r.connRejected = append(r.connRejected, reason)
			r.mu.Unlock()
		},
		StreamStarted: func(_ context.Context, method string) {
			r.mu.Lock()
			r.streamsStarted = append(r.streamsStarted, method)
			r.mu.Unlock()
		},
		StreamEnded: func(_ context.Context, method string, code codes.Code) {
			r.mu.Lock()
			r.streamsEnded[method] = code
			r.mu.Unlock()
		},
		StreamRejected: func(reason string) {
			r.mu.Lock()
			r.streamsRejected = append(r.streamsRejected, reason)
			r.mu.Unlock()
		},
		MsgSent: func(_ context.Context, _ string, bytes int) {
			r.mu.Lock()
			r.sentMsgs++
			r.sentBytes += bytes
			r.mu.Unlock()
		},
		MsgReceived: func(_ context.Context, _ string, bytes int) {
			r.mu.Lock()
			r.recvMsgs++
			r.recvBytes += bytes
			r.mu.Unlock()
		},
	}
}

// TestStats_UnaryFlow pins the full event sequence of one healthy connection
// with one unary call: conn open/close, stream start/end(OK), one MSG each way
// with non-zero payload sizes.
func TestStats_UnaryFlow(t *testing.T) {
	t.Parallel()

	recorder := newStatsRecorder()

	srv := wsrpc.NewServer(wsrpc.WithStats(recorder.stats()))
	srv.Register("/t/Echo", func(ctx context.Context, s *wsrpc.Stream) error {
		var req wrapperspb.StringValue
		if err := s.Recv(&req); err != nil {
			return err
		}
		return s.Send(&wrapperspb.StringValue{Value: "echo:" + req.Value})
	})

	srvEnd, cliEnd := wsrpc.NewPipe()
	t.Cleanup(func() { _ = srvEnd.Close(); _ = cliEnd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() { srv.ServeConn(ctx, srvEnd); close(serveDone) }()

	cc, err := wsrpc.DialConn(ctx, cliEnd)
	require.NoError(t, err)

	stream, err := cc.NewStream(ctx, "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&wrapperspb.StringValue{Value: "hi"}))
	require.NoError(t, stream.CloseSend())

	var resp wrapperspb.StringValue
	require.NoError(t, stream.Recv(&resp))
	require.Equal(t, "echo:hi", resp.Value)

	// End the connection and wait for ServeConn to return (ConnClosed fired).
	_ = cc.Close()
	cancel()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ServeConn did not return")
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Equal(t, 1, recorder.connOpened)
	require.Equal(t, 1, recorder.connClosed)
	require.Empty(t, recorder.connRejected)
	require.Equal(t, []string{"/t/Echo"}, recorder.streamsStarted)
	require.Equal(t, codes.OK, recorder.streamsEnded["/t/Echo"])
	require.Empty(t, recorder.streamsRejected)
	require.Equal(t, 1, recorder.recvMsgs)
	require.Positive(t, recorder.recvBytes)
	require.Equal(t, 1, recorder.sentMsgs)
	require.Positive(t, recorder.sentBytes)
}

// TestStats_NilCallbacksSafe pins that a partially-filled (and a nil) stats
// struct never panics: every dispatch helper is nil-safe.
func TestStats_NilCallbacksSafe(t *testing.T) {
	t.Parallel()

	srv := wsrpc.NewServer(wsrpc.WithStats(&wsrpc.ServerStats{}))
	srv.Register("/t/Echo", func(ctx context.Context, s *wsrpc.Stream) error {
		var req wrapperspb.StringValue
		if err := s.Recv(&req); err != nil {
			return err
		}
		return s.Send(&req)
	})

	srvEnd, cliEnd := wsrpc.NewPipe()
	t.Cleanup(func() { _ = srvEnd.Close(); _ = cliEnd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.ServeConn(ctx, srvEnd)

	cc, err := wsrpc.DialConn(ctx, cliEnd)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	stream, err := cc.NewStream(ctx, "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&wrapperspb.StringValue{Value: "ok"}))
	require.NoError(t, stream.CloseSend())

	var resp wrapperspb.StringValue
	require.NoError(t, stream.Recv(&resp))
	require.Equal(t, "ok", resp.Value)
}
