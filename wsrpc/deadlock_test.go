package wsrpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
)

// blockConn is a frameConn whose writes can be made to block indefinitely (to
// simulate a peer whose TCP buffer is full), while reads keep flowing. It lets a
// test prove the read loop never stalls waiting on a blocked write.
type blockConn struct {
	reads        chan *transport.Frame
	readCount    atomic.Int32
	blockWrites  atomic.Bool
	writeBlocked chan struct{}         // signalled each time a write parks
	release      chan struct{}         // closed to release parked writes
	writes       chan *transport.Frame // frames successfully written
	closed       chan struct{}
}

var errBlockClosed = errors.New("blockConn closed")

func newBlockConn() *blockConn {
	return &blockConn{
		reads:        make(chan *transport.Frame, 16),
		writeBlocked: make(chan struct{}, 16),
		release:      make(chan struct{}),
		writes:       make(chan *transport.Frame, 16),
		closed:       make(chan struct{}),
	}
}

func (c *blockConn) WriteFrame(ctx context.Context, f *transport.Frame) error {
	// Honor a cancelled context up front, deterministically (like the real
	// websocket conn): a write issued with a dead context fails and sends nothing.
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.blockWrites.Load() {
		select {
		case c.writeBlocked <- struct{}{}:
		default:
		}
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return errBlockClosed
		}
	}
	select {
	case c.writes <- f:
	default:
	}
	return nil
}

func (c *blockConn) ReadFrame(ctx context.Context) (*transport.Frame, error) {
	select {
	case f := <-c.reads:
		c.readCount.Add(1)
		return f, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, errBlockClosed
	}
}

func (c *blockConn) Ping(context.Context) error { return nil }

func (c *blockConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

// TestReadLoopNotBlockedByPendingWrite reproduces C1: when a Send is parked
// inside a blocking socket write (holding the write lock), an inbound MSG that
// overflows a stream's receive buffer forces the read loop to emit an RST. If
// that RST write is performed inline on the read loop it deadlocks behind the
// parked Send and the read loop can no longer read — so a subsequent frame is
// never consumed. The read loop must keep reading regardless of write progress.
func TestReadLoopNotBlockedByPendingWrite(t *testing.T) {
	conn := newBlockConn()
	m := newMuxBuffered(context.Background(), conn, nil, 1) // recvBuffer=1, client
	defer m.Close()

	s, err := m.newClientStream(context.Background(), "/svc/M", nil)
	require.NoError(t, err)

	// From here every socket write parks, so the next Send holds the write lock.
	conn.blockWrites.Store(true)

	go func() { _ = s.Send(&transport.Frame{}) }() // parks inside WriteFrame
	select {
	case <-conn.writeBlocked:
	case <-time.After(time.Second):
		t.Fatal("Send did not reach the blocking write")
	}

	// frame1 fills the cap-1 receive buffer; frame2 overflows it -> read loop
	// must emit an RST without blocking on the held write lock.
	conn.reads <- &transport.Frame{StreamId: s.id, Kind: transport.Kind_KIND_MSG, Payload: []byte("a")}
	conn.reads <- &transport.Frame{StreamId: s.id, Kind: transport.Kind_KIND_MSG, Payload: []byte("b")}
	// A probe for an unknown stream: routing it requires no write, so the only
	// reason it would go unread is a stalled read loop.
	conn.reads <- &transport.Frame{StreamId: 9999, Kind: transport.Kind_KIND_HALF_CLOSE}

	// All three frames (fill, overflow, probe) must be read within the timeout.
	require.Eventually(t, func() bool {
		return conn.readCount.Load() >= 3
	}, time.Second, 2*time.Millisecond, "read loop stalled on a blocked write (deadlock)")
}
