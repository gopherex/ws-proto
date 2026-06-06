package wsrpc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gopherex/ws-proto/transport"
)

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

func (m *Mux) write(ctx context.Context, f *transport.Frame) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.conn.WriteFrame(ctx, f)
}

// newClientStream allocates an id, registers the stream, and sends OPEN.
func (m *Mux) newClientStream(ctx context.Context, method string, headers map[string]string) (*Stream, error) {
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
		s := newStream(m.ctx, m, f.StreamId, f.Method)
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
