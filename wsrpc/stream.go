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

	header map[string]string // headers received on OPEN (server) or END (client trailers)

	recvCh chan *transport.Frame // MSG/END/RST frames routed here by the mux

	mu       sync.Mutex
	sendDone bool
	closed   bool
	endSt    *Status // set when END/RST observed
}

func newStream(ctx context.Context, mux *Mux, id uint32, method string) *Stream {
	c, cancel := context.WithCancel(ctx)
	return &Stream{
		id:     id,
		method: method,
		mux:    mux,
		ctx:    c,
		cancel: cancel,
		recvCh: make(chan *transport.Frame, 16),
	}
}

// Context returns the stream context, cancelled on end/RST.
func (s *Stream) Context() context.Context { return s.ctx }

// Method returns the fully-qualified RPC method.
func (s *Stream) Method() string { return s.method }

// Header returns headers/trailers observed for this stream.
func (s *Stream) Header() map[string]string { return s.header }

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
	select {
	case f, ok := <-s.recvCh:
		if !ok {
			return io.EOF
		}
		switch f.Kind {
		case transport.Kind_KIND_MSG:
			return proto.Unmarshal(f.Payload, msg)
		case transport.Kind_KIND_END:
			s.applyEnd(f)
			if s.endSt != nil && s.endSt.Code != codes.OK {
				return s.endSt
			}
			return io.EOF
		case transport.Kind_KIND_RST:
			s.applyEnd(f)
			if s.endSt == nil {
				s.endSt = &Status{Code: codes.Canceled, Message: "stream reset"}
			}
			return s.endSt
		default:
			return io.EOF
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
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

func (s *Stream) applyEnd(f *transport.Frame) {
	if f.Headers != nil {
		s.header = f.Headers
	}
	s.endSt = statusFromProto(f.Status)
	s.cancel()
}

// deliver routes an inbound frame into the stream (called by the mux).
func (s *Stream) deliver(f *transport.Frame) {
	select {
	case s.recvCh <- f:
	case <-s.ctx.Done():
	}
}

// halfClose closes recvCh so a server-side Recv returns io.EOF.
func (s *Stream) halfClose() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.recvCh)
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
