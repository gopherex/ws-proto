package wsrpc

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/grpc/codes"
)

// timeoutHeader carries the caller's remaining deadline (decimal milliseconds)
// on the OPEN frame so the server can derive a per-stream context deadline.
const timeoutHeader = "ws-timeout-ms"

// Mux multiplexes streams over one FrameConn. Used by both client and server;
// onOpen is nil on the client and set on the server to dispatch new streams.
type Mux struct {
	conn   FrameConn
	ctx    context.Context
	cancel context.CancelFunc

	nextID uint32 // client stream-id allocator (odd ids), atomic

	mu      sync.Mutex
	streams map[uint32]*Stream

	onOpen func(*Stream) // server-side dispatch; nil on client

	initialWindow int // per-stream credit window (bytes) for flow control
	maxStreams    int // cap on concurrent server streams; 0 = unlimited (client)

	writeMu sync.Mutex // serialize frame writes

	// ctrlCh carries control frames the read loop needs to emit (overflow RST,
	// over-limit RST) so they are written by a separate writer goroutine. The
	// read loop must NEVER perform a blocking socket write: a Send parked inside
	// WriteFrame (peer's buffer full) holds writeMu, and an inline write on the
	// read loop would block behind it, stalling reads and deadlocking the conn.
	ctrlCh chan *transport.Frame

	// closed records a deliberate user Close so the conn-loss path can
	// distinguish an intentional shutdown (codes.Canceled, "transport closed")
	// from a transport disconnect (codes.Unavailable, "wsrpc: connection lost").
	closedFlag atomic.Bool
}

func newMux(ctx context.Context, conn FrameConn, onOpen func(*Stream)) *Mux {
	return newMuxBuffered(ctx, conn, onOpen, defaultReceiveBuffer)
}

func newMuxBuffered(ctx context.Context, conn FrameConn, onOpen func(*Stream), recvBuffer int) *Mux {
	return newMuxConfig(ctx, conn, onOpen, recvBuffer, defaultInitialWindow, 0)
}

func newMuxConfig(ctx context.Context, conn FrameConn, onOpen func(*Stream), recvBuffer, initialWindow, maxStreams int) *Mux {
	if recvBuffer <= 0 {
		recvBuffer = defaultReceiveBuffer
	}
	if initialWindow <= 0 {
		initialWindow = defaultInitialWindow
	}
	c, cancel := context.WithCancel(ctx)
	m := &Mux{
		conn:          conn,
		ctx:           c,
		cancel:        cancel,
		nextID:        1,
		streams:       make(map[uint32]*Stream),
		onOpen:        onOpen,
		initialWindow: initialWindow,
		maxStreams:    maxStreams,
		ctrlCh:        make(chan *transport.Frame, ctrlBufferSize),
	}
	go m.ctrlWriter()
	go m.readLoop()
	return m
}

// ctrlBufferSize bounds the read loop's outstanding control-frame backlog. These
// frames (overflow / over-limit RST) are rare; if the writer cannot keep up the
// read loop drops them rather than block (the affected stream is already failed
// locally, and a dropped RST only delays the peer's own cleanup).
const ctrlBufferSize = 256

// writeCtrl enqueues a control frame for the writer goroutine without ever
// blocking the caller (the read loop). A full buffer means the writer is wedged
// behind a stuck socket write; dropping the frame is safe (see ctrlBufferSize).
func (m *Mux) writeCtrl(f *transport.Frame) {
	select {
	case m.ctrlCh <- f:
	default:
	}
}

// ctrlWriter drains ctrlCh and performs the (potentially blocking) socket writes
// off the read loop, so the read loop only ever enqueues. Exits on mux teardown.
func (m *Mux) ctrlWriter() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case f := <-m.ctrlCh:
			_ = m.write(m.ctx, f)
		}
	}
}

// startKeepalive launches a goroutine that pings the peer every interval,
// failing the mux if a pong is not received within timeout. interval<=0
// disables pinging. Must run concurrently with readLoop, which supplies the
// pong reads that coder/websocket Ping awaits.
func (m *Mux) startKeepalive(interval, timeout time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(m.ctx, timeout)
				err := m.conn.Ping(ctx)
				cancel()
				if err != nil {
					m.cancel()
					return
				}
			}
		}
	}()
}

func (m *Mux) write(ctx context.Context, f *transport.Frame) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.conn.WriteFrame(ctx, f)
}

// newClientStream allocates an id, registers the stream, and sends OPEN.
func (m *Mux) newClientStream(ctx context.Context, method string, headers map[string]string) (*Stream, error) {
	// Propagate the caller's deadline as a ws-timeout-ms header so the server
	// can derive a matching per-stream context deadline.
	if d, ok := ctx.Deadline(); ok {
		if ms := time.Until(d).Milliseconds(); ms > 0 {
			// Copy the caller's map (or allocate) so we never mutate it.
			h := make(map[string]string, len(headers)+1)
			for k, v := range headers {
				h[k] = v
			}
			h[timeoutHeader] = strconv.FormatInt(ms, 10)
			headers = h
		}
	}
	id := atomic.AddUint32(&m.nextID, 2) - 2 // 1,3,5,...
	s := newStream(ctx, m, id, method, m.initialWindow)
	m.mu.Lock()
	m.streams[id] = s
	m.mu.Unlock()
	if err := m.write(ctx, &transport.Frame{
		StreamId: id,
		Kind:     transport.Kind_KIND_OPEN,
		Method:   method,
		Headers:  headers,
	}); err != nil {
		m.remove(id)
		return nil, err
	}
	return s, nil
}

func (m *Mux) remove(id uint32) {
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
}

func (m *Mux) readLoop() {
	defer m.cancel()
	for {
		f, err := m.conn.ReadFrame(m.ctx)
		if err != nil {
			m.failAll(m.disconnectErr())
			return
		}
		m.route(f)
	}
}

// disconnectErr maps a read-loop termination to a stream status. A deliberate
// user Close surfaces codes.Canceled ("transport closed"); any other
// termination (websocket close, read error, keepalive failure) is a transport
// disconnect and surfaces codes.Unavailable ("wsrpc: connection lost"), which
// callers are expected to retry.
func (m *Mux) disconnectErr() error {
	if m.closedFlag.Load() {
		return Errorf(codes.Canceled, "wsrpc: transport closed")
	}
	return Errorf(codes.Unavailable, "wsrpc: connection lost")
}

func (m *Mux) route(f *transport.Frame) {
	if f.Kind == transport.Kind_KIND_OPEN {
		if m.onOpen == nil {
			return // clients ignore OPEN
		}
		// Bound concurrent server streams: reject an OPEN past the cap with an
		// RST(ResourceExhausted) without allocating a Stream or handler goroutine.
		// The RST goes through writeCtrl so the read loop never blocks (see C1).
		if m.maxStreams > 0 {
			m.mu.Lock()
			over := len(m.streams) >= m.maxStreams
			m.mu.Unlock()
			if over {
				m.writeCtrl(&transport.Frame{
					StreamId: f.StreamId,
					Kind:     transport.Kind_KIND_RST,
					Status: statusToProto(&Status{
						Code:    codes.ResourceExhausted,
						Message: "max concurrent streams exceeded",
					}),
				})
				return
			}
		}
		// Honor a caller-supplied deadline (ws-timeout-ms) by deriving the
		// stream context with that timeout; otherwise inherit the mux context.
		sctx := m.ctx
		var dcancel context.CancelFunc
		if v := f.Headers[timeoutHeader]; v != "" {
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
				sctx, dcancel = context.WithTimeout(m.ctx, time.Duration(ms)*time.Millisecond)
			}
		}
		s := newStream(sctx, m, f.StreamId, f.Method, m.initialWindow)
		s.deadlineCancel = dcancel
		s.header = f.Headers
		m.mu.Lock()
		m.streams[f.StreamId] = s
		m.mu.Unlock()
		m.onOpen(s)
		return
	}

	m.mu.Lock()
	s := m.streams[f.StreamId]
	m.mu.Unlock()
	if s == nil {
		return
	}
	switch f.Kind {
	case transport.Kind_KIND_HALF_CLOSE:
		s.halfClose()
		return
	case transport.Kind_KIND_HEADER:
		// Leading response metadata: server->client only. It bypasses the
		// bounded MSG queue and is not terminal. Servers never receive it
		// (clients don't send KIND_HEADER), so ignore it there.
		if m.onOpen == nil {
			s.setLeadingHeader(f.Headers)
		}
		return
	case transport.Kind_KIND_WINDOW_UPDATE:
		// Flow control: the peer (the receiver) returns credit for what it has
		// consumed. Credit the send window and wake any blocked Send. This runs
		// on the read loop and never blocks (creditSend signals via a cond).
		s.creditSend(int(f.Window))
		return
	case transport.Kind_KIND_END, transport.Kind_KIND_RST:
		// Terminal frames bypass the bounded MSG queue entirely so a slow
		// consumer can never lose or stall an END/RST. Recv drains buffered
		// MSGs first, then observes this terminal frame.
		s.signalEnd(f)
		m.remove(f.StreamId)
		return
	default: // KIND_MSG
		if !s.tryDeliver(f) {
			// Consumer too slow: the bounded receive buffer overflowed. Reset
			// the stream locally and tell the peer to stop. The read loop must
			// never block, so we drop this stream and move on to the others.
			s.failWith(Errorf(codes.ResourceExhausted, "wsrpc: receive buffer overflow"))
			m.writeCtrl(&transport.Frame{
				StreamId: f.StreamId,
				Kind:     transport.Kind_KIND_RST,
				Status: statusToProto(&Status{
					Code:    codes.ResourceExhausted,
					Message: "receive buffer overflow",
				}),
			})
			m.remove(f.StreamId)
			return
		}
	}
}

func (m *Mux) failAll(err error) {
	m.mu.Lock()
	for id, s := range m.streams {
		s.failWith(err)
		delete(m.streams, id)
	}
	m.mu.Unlock()
}

// Close shuts the mux and underlying conn. It marks the mux as deliberately
// closed so in-flight streams fail with codes.Canceled ("transport closed")
// rather than the codes.Unavailable mapping used for transport disconnects.
func (m *Mux) Close() error {
	m.closedFlag.Store(true)
	m.cancel()
	return m.conn.Close()
}
