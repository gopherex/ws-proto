package wsrpc

import (
	"context"
	"io"
	"sync"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// Stream is one multiplexed RPC. It is untyped: callers pass proto.Message.
// Generated code (Plan 2) wraps this in typed Send/Recv methods.
type Stream struct {
	id     uint32
	method string
	mux    *Mux
	ctx    context.Context
	cancel context.CancelFunc

	// deadlineCancel releases the timeout context derived from a caller's
	// ws-timeout-ms header (server side); nil when no per-stream deadline.
	deadlineCancel context.CancelFunc

	mu     sync.Mutex
	header map[string]string // guarded by mu
	endSt  *Status           // guarded by mu

	recvCh     chan *transport.Frame // MSG/END/RST frames routed here by the mux
	halfClosed chan struct{}         // closed once when peer half-closes (inbound)

	sendDone     bool // guarded by mu
	halfCloseOne sync.Once
}

func newStream(ctx context.Context, mux *Mux, id uint32, method string) *Stream {
	c, cancel := context.WithCancel(ctx)
	return &Stream{
		id:         id,
		method:     method,
		mux:        mux,
		ctx:        c,
		cancel:     cancel,
		recvCh:     make(chan *transport.Frame, 16),
		halfClosed: make(chan struct{}),
	}
}

// Context returns the stream context, cancelled on end/RST.
func (s *Stream) Context() context.Context { return s.ctx }

// Method returns the fully-qualified RPC method.
func (s *Stream) Method() string { return s.method }

// Header returns headers/trailers observed for this stream.
func (s *Stream) Header() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.header
}

// Send marshals msg and writes a MSG frame.
func (s *Stream) Send(msg proto.Message) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_MSG,
		Payload:  b,
	})
}

// Recv waits for the next MSG and unmarshals into msg. Returns io.EOF on a
// clean END, or the *Status as error on a non-OK END / RST.
func (s *Stream) Recv(msg proto.Message) error {
	// Prefer any already-buffered frame so half-close never preempts pending MSGs.
	select {
	case f := <-s.recvCh:
		return s.handleRecv(f, msg)
	default:
	}
	select {
	case f := <-s.recvCh:
		return s.handleRecv(f, msg)
	case <-s.halfClosed:
		return io.EOF
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *Stream) handleRecv(f *transport.Frame, msg proto.Message) error {
	switch f.Kind {
	case transport.Kind_KIND_MSG:
		return proto.Unmarshal(f.Payload, msg)
	case transport.Kind_KIND_END:
		st := s.applyEnd(f)
		if st.Code != codes.OK {
			return st
		}
		return io.EOF
	case transport.Kind_KIND_RST:
		st := s.applyEnd(f)
		if st.Code == codes.OK {
			st = &Status{Code: codes.Canceled, Message: "stream reset"}
		}
		return st
	default:
		return io.EOF
	}
}

// CloseSend signals the client is done sending (HALF_CLOSE).
func (s *Stream) CloseSend() error {
	s.mu.Lock()
	if s.sendDone {
		s.mu.Unlock()
		return nil
	}
	s.sendDone = true
	s.mu.Unlock()
	return s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_HALF_CLOSE,
	})
}

// end is called by the server side to finish a stream with a status + trailers.
func (s *Stream) end(st *Status, trailers map[string]string) error {
	f := &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_END,
		Headers:  trailers,
		Status:   statusToProto(st),
	}
	return s.mux.write(s.ctx, f)
}

func (s *Stream) applyEnd(f *transport.Frame) *Status {
	st := statusFromProto(f.Status)
	s.mu.Lock()
	if f.Headers != nil {
		s.header = f.Headers
	}
	s.endSt = st
	s.mu.Unlock()
	s.cancel()
	return st
}

// failWith is called by mux.failAll to signal an error on the stream.
func (s *Stream) failWith(err error) {
	s.mu.Lock()
	s.endSt = FromError(err)
	s.mu.Unlock()
	s.cancel()
}

// deliver routes an inbound frame into the stream (called by the mux).
func (s *Stream) deliver(f *transport.Frame) {
	select {
	case s.recvCh <- f:
	case <-s.ctx.Done():
	}
}

// halfClose signals the peer finished sending; a blocked Recv returns io.EOF
// once recvCh is drained.
func (s *Stream) halfClose() {
	s.halfCloseOne.Do(func() { close(s.halfClosed) })
}

func statusToProto(st *Status) *transport.Status {
	if st == nil {
		st = &Status{Code: codes.OK}
	}
	return &transport.Status{
		Code:    int32(st.Code),
		Message: st.Message,
		Details: st.Details,
	}
}

func statusFromProto(p *transport.Status) *Status {
	if p == nil {
		return &Status{Code: codes.OK}
	}
	return &Status{
		Code:    codes.Code(p.Code),
		Message: p.Message,
		Details: p.Details,
	}
}
