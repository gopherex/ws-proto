package wsrpc

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopherex/ws-proto/transport"
)

// timeoutHeader carries the caller's remaining deadline (decimal milliseconds)
// on the OPEN frame so the server can derive a per-stream context deadline.
const timeoutHeader = "ws-timeout-ms"

// Mux multiplexes streams over one frameConn. Used by both client and server;
// onOpen is nil on the client and set on the server to dispatch new streams.
type Mux struct {
	conn   frameConn
	ctx    context.Context
	cancel context.CancelFunc

	nextID uint32 // client stream-id allocator (odd ids), atomic

	mu      sync.Mutex
	streams map[uint32]*Stream

	onOpen func(*Stream) // server-side dispatch; nil on client

	writeMu sync.Mutex // serialize frame writes
}

func newMux(ctx context.Context, conn frameConn, onOpen func(*Stream)) *Mux {
	c, cancel := context.WithCancel(ctx)
	m := &Mux{
		conn:    conn,
		ctx:     c,
		cancel:  cancel,
		nextID:  1,
		streams: make(map[uint32]*Stream),
		onOpen:  onOpen,
	}
	go m.readLoop()
	return m
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
	s := newStream(ctx, m, id, method)
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
			m.failAll(err)
			return
		}
		m.route(f)
	}
}

func (m *Mux) route(f *transport.Frame) {
	if f.Kind == transport.Kind_KIND_OPEN {
		if m.onOpen == nil {
			return // clients ignore OPEN
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
		s := newStream(sctx, m, f.StreamId, f.Method)
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
	if f.Kind == transport.Kind_KIND_HALF_CLOSE {
		s.halfClose()
		return
	}
	s.deliver(f)
	if f.Kind == transport.Kind_KIND_END || f.Kind == transport.Kind_KIND_RST {
		m.remove(f.StreamId)
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

// Close shuts the mux and underlying conn.
func (m *Mux) Close() error {
	m.cancel()
	return m.conn.Close()
}
