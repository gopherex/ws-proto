package wsrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Gate the socket Write itself, after coder/websocket has installed its
// cancellation callback. The handshake is the first write and passes through.
type pausedNetConn struct {
	net.Conn
	entered, release, closed chan struct{}
	writes                   atomic.Int32
	once                     sync.Once
}

func (c *pausedNetConn) Write(payload []byte) (int, error) {
	if c.writes.Add(1) == 2 {
		close(c.entered)
		select {
		case <-c.release:
		case <-c.closed:
			return 0, io.ErrClosedPipe
		}
	}
	return c.Conn.Write(payload)
}
func (c *pausedNetConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type pausedListener struct {
	net.Listener
	entered, release chan struct{}
}

func (l *pausedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &pausedNetConn{Conn: conn, entered: l.entered, release: l.release, closed: make(chan struct{})}, nil
}

func TestStreamResetDuringWebsocketWritePreservesSibling(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	stopped := make(chan struct{})
	server := NewServer()
	server.Register("/t/Paused", func(ctx context.Context, stream *Stream) error {
		defer close(stopped)
		return stream.Send(wrapperspb.String("cancel me"))
	})
	server.Register("/t/Sibling", func(ctx context.Context, stream *Stream) error {
		return stream.Send(wrapperspb.String("alive"))
	})
	httpServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}})
		if err != nil {
			return
		}
		server.ServeConn(ctx, newWSConn(socket, 1<<20))
	}))
	httpServer.Listener = &pausedListener{Listener: httpServer.Listener, entered: entered, release: release}
	httpServer.Start()
	defer httpServer.Close()
	connection, err := Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"))
	require.NoError(t, err)
	defer connection.Close()
	stream, err := connection.NewStream(ctx, "/t/Paused", nil)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, stream.mux.write(ctx, &transport.Frame{StreamId: stream.id, Kind: transport.Kind_KIND_RST}))
	// Cancellation must finish the handler while its physical write is paused.
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("reset did not interrupt handler")
	}
	close(release)
	sibling, err := connection.NewStream(ctx, "/t/Sibling", nil)
	require.NoError(t, err)
	response := new(wrapperspb.StringValue)
	require.NoError(t, sibling.Recv(response))
	require.Equal(t, "alive", response.Value)
}

func TestCanceledQueuedWriteReturnsWithoutSending(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	conn := newBlockConn()
	mux := newMux(ctx, conn, nil)
	defer mux.Close()
	conn.blockWrites.Store(true)
	first := make(chan error, 1)
	go func() { first <- mux.write(ctx, &transport.Frame{StreamId: 1}) }()
	select {
	case <-conn.writeBlocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queuedContext, cancelQueued := context.WithCancel(ctx)
	queued := make(chan error, 1)
	go func() { queued <- mux.write(queuedContext, &transport.Frame{StreamId: 3}) }()
	cancelQueued()
	select {
	case err := <-queued:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("canceled write is stuck behind another stream")
	}
	close(conn.release)
	require.NoError(t, <-first)
	require.NoError(t, mux.write(ctx, &transport.Frame{StreamId: 5}))
	for range 2 {
		select {
		case frame := <-conn.writes:
			require.NotEqual(t, uint32(3), frame.StreamId)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestMuxCloseInterruptsActiveWrite(t *testing.T) {
	t.Parallel()
	conn := newBlockConn()
	mux := newMux(t.Context(), conn, nil)
	conn.blockWrites.Store(true)
	result := make(chan error, 1)
	go func() { result <- mux.write(context.Background(), &transport.Frame{StreamId: 1}) }()
	select {
	case <-conn.writeBlocked:
	case <-time.After(time.Second):
		t.Fatal("write not started")
	}
	require.NoError(t, mux.Close())
	select {
	case err := <-result:
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, errBlockClosed), "%v", err)
	case <-time.After(time.Second):
		t.Fatal("mux close did not interrupt write")
	}
}

type traceWriteConn struct {
	*blockConn
	contexts chan context.Context
}

func (c *traceWriteConn) WriteFrame(ctx context.Context, frame *transport.Frame) error {
	c.contexts <- ctx
	return c.blockConn.WriteFrame(ctx, frame)
}

func TestCanceledActiveWriteOwnsFrameAndPreservesTrace(t *testing.T) {
	t.Parallel()
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			type traceKey struct{}
			trace := context.WithValue(ctx, traceKey{}, "trace123")
			var call context.Context
			var stop context.CancelFunc
			if deadline {
				call, stop = context.WithTimeout(trace, 50*time.Millisecond)
			} else {
				call, stop = context.WithCancel(trace)
			}
			defer stop()
			conn := &traceWriteConn{blockConn: newBlockConn(), contexts: make(chan context.Context, 2)}
			conn.blockWrites.Store(true)
			mux := newMux(ctx, conn, nil)
			defer mux.Close()
			frame := &transport.Frame{StreamId: 1, Payload: []byte{1, 2, 3}, Headers: map[string]string{"key": "before"}}
			result := make(chan error, 1)
			go func() { result <- mux.write(call, frame) }()
			select {
			case <-conn.writeBlocked:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !deadline {
				stop()
			}
			select {
			case err := <-result:
				if deadline {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-ctx.Done():
				t.Fatal("stream cancellation did not return promptly")
			}
			// Ownership returned to the caller; the admitted physical write still owns
			// its snapshot and carries trace values without the expired RPC deadline.
			frame.Payload[0] = 9
			frame.Headers["key"] = "after"
			physical := <-conn.contexts
			require.NoError(t, physical.Err())
			require.Equal(t, "trace123", physical.Value(traceKey{}))
			require.NoError(t, context.Cause(physical))
			close(conn.release)
			require.NoError(t, mux.write(ctx, &transport.Frame{StreamId: 3}))
			sent := <-conn.writes
			require.Equal(t, []byte{1, 2, 3}, sent.Payload)
			require.Equal(t, "before", sent.Headers["key"])
		})
	}
}

func BenchmarkMuxWrite(b *testing.B) {
	for _, size := range []int{128, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			mux := newMux(b.Context(), newBlockConn(), nil)
			defer mux.Close()
			frame := &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG, Payload: make([]byte, size)}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if err := mux.write(b.Context(), frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Model a queued terminal frame that the read loop has not yet dispatched
// when a concurrent OPEN discovers the socket has closed.
type finalStatusConn struct {
	release   chan struct{}
	delivered bool
}

func (c *finalStatusConn) WriteFrame(context.Context, *transport.Frame) error { return errBlockClosed }
func (c *finalStatusConn) ReadFrame(ctx context.Context) (*transport.Frame, error) {
	<-ctx.Done()
	if c.delivered {
		return nil, errBlockClosed
	}
	<-c.release
	c.delivered = true
	return &transport.Frame{Kind: transport.Kind_KIND_CONN_END, Status: statusToProto(&Status{Code: codes.Unauthenticated, Message: "revoked"})}, nil
}
func (*finalStatusConn) Close() error               { return nil }
func (*finalStatusConn) Ping(context.Context) error { return nil }

func TestFailedOpenPreservesQueuedConnectionStatus(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		conn := &finalStatusConn{release: make(chan struct{})}
		mux := newMux(t.Context(), conn, nil)
		defer mux.Close()
		result := make(chan error, 1)
		go func() { _, err := mux.newClientStream(t.Context(), "/t/Echo", nil); result <- err }()
		synctest.Wait()
		select {
		case err := <-result:
			t.Errorf("OPEN preempted pending terminal status: %v", err)
		default:
		}
		close(conn.release)
		synctest.Wait()
		if t.Failed() {
			return
		}
		select {
		case err := <-result:
			require.Equal(t, codes.Unauthenticated, FromError(err).Code)
		default:
			t.Fatal("OPEN did not finish after terminal status")
		}
	})
}
