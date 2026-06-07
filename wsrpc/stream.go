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

	mu       sync.Mutex
	header   map[string]string // guarded by mu
	endSt    *Status           // guarded by mu
	endFrame *transport.Frame  // terminal END/RST frame; guarded by mu

	recvCh     chan *transport.Frame // bounded inbound MSG queue, routed by the mux
	halfClosed chan struct{}         // closed once when peer half-closes (inbound)
	ended      chan struct{}         // closed once when a terminal END/RST arrives

	sendDone     bool // guarded by mu
	halfCloseOne sync.Once
	endOne       sync.Once
}

func newStream(ctx context.Context, mux *Mux, id uint32, method string, recvBuffer int) *Stream {
	if recvBuffer <= 0 {
		recvBuffer = defaultReceiveBuffer
	}
	c, cancel := context.WithCancel(ctx)
	return &Stream{
		id:         id,
		method:     method,
		mux:        mux,
		ctx:        c,
		cancel:     cancel,
		recvCh:     make(chan *transport.Frame, recvBuffer),
		halfClosed: make(chan struct{}),
		ended:      make(chan struct{}),
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
	// Prefer any already-buffered MSG so terminal/half-close signals never
	// preempt pending data (drain-first).
	select {
	case f := <-s.recvCh:
		return proto.Unmarshal(f.Payload, msg)
	default:
	}
	select {
	case f := <-s.recvCh:
		return proto.Unmarshal(f.Payload, msg)
	case <-s.halfClosed:
		return io.EOF
	case <-s.ended:
		return s.recvEnded(msg)
	case <-s.ctx.Done():
		// A terminal frame may have closed ctx via failWith/applyEnd; surface a
		// recorded END/RST in preference to the bare context error.
		select {
		case <-s.ended:
			return s.recvEnded(msg)
		default:
		}
		return s.ctx.Err()
	}
}

// recvEnded is entered once a terminal END/RST has been recorded. Any MSGs
// still buffered are delivered first; only when the queue is empty is the
// terminal status returned.
func (s *Stream) recvEnded(msg proto.Message) error {
	select {
	case f := <-s.recvCh:
		return proto.Unmarshal(f.Payload, msg)
	default:
	}
	s.mu.Lock()
	f := s.endFrame
	s.mu.Unlock()
	if f == nil {
		// Terminal via failWith (no frame), e.g. connection drop / overflow.
		if st := s.status(); st != nil && st.Code != codes.OK {
			return st
		}
		return io.EOF
	}
	switch f.Kind {
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

func (s *Stream) status() *Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endSt
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

// failWith is called by mux.failAll / overflow handling to signal an error on
// the stream. It records the status, wakes a blocked Recv via the terminal
// signal (with no frame), and cancels the context.
func (s *Stream) failWith(err error) {
	s.mu.Lock()
	s.endSt = FromError(err)
	s.mu.Unlock()
	s.endOne.Do(func() { close(s.ended) }) // terminal, frameless
	s.cancel()
}

// tryDeliver enqueues a MSG frame without ever blocking the read loop. It
// returns false only when the bounded buffer is full (the consumer is too
// slow), which the mux turns into a stream reset.
func (s *Stream) tryDeliver(f *transport.Frame) bool {
	select {
	case s.recvCh <- f:
		return true
	case <-s.ctx.Done():
		return true // stream already ending; dropping the frame is fine
	default:
		return false
	}
}

// signalEnd records a terminal END/RST frame and wakes Recv, without consuming
// any space in the bounded MSG queue.
func (s *Stream) signalEnd(f *transport.Frame) {
	s.endOne.Do(func() {
		s.mu.Lock()
		s.endFrame = f
		s.mu.Unlock()
		close(s.ended)
	})
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
