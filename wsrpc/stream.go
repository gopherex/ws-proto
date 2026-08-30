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
	header   map[string]string // leading headers (OPEN on server; KIND_HEADER on client); guarded by mu
	trailer  map[string]string // END trailers (client receive-side); guarded by mu
	endSt    *Status           // guarded by mu
	endFrame *transport.Frame  // terminal END/RST frame; guarded by mu

	// Server send-side metadata, guarded by mu.
	outTrailer map[string]string // trailers flushed with the END frame
	headerSent bool              // a leading KIND_HEADER frame was sent
	msgSent    bool              // at least one response MSG was sent

	// Inbound MSG backlog, bounded by BYTES (not frame count) so a peer that
	// obeys the flow-control window is never falsely reset: with tiny messages a
	// well-behaved sender may have many frames but at most ~initialWindow bytes
	// in flight. recvItems/recvBytes are guarded by mu; recvSignal (cap 1) wakes
	// a parked Recv when a frame is enqueued. maxRecvBytes is the byte ceiling.
	recvItems    []*transport.Frame
	recvBytes    int
	maxRecvBytes int
	recvSignal   chan struct{}
	halfClosed   chan struct{} // closed once when peer half-closes (inbound)
	ended        chan struct{} // closed once when a terminal END/RST arrives

	sendDone     bool // guarded by mu
	halfCloseOne sync.Once
	endOne       sync.Once

	// Flow control (credit windowing), all guarded by mu.
	// sendWindow is the credit (bytes) we may send before blocking. It starts at
	// initialWindow and is decremented when a MSG is sent (may go negative once,
	// see Send) and incremented when a KIND_WINDOW_UPDATE arrives. sendCond
	// signals a blocked Send when credit is returned or the stream ends.
	initialWindow int
	sendWindow    int
	sendCond      *sync.Cond
	// pendingCredit accumulates bytes consumed by Recv but not yet returned to
	// the peer; flushed as a KIND_WINDOW_UPDATE once it crosses initialWindow/2.
	pendingCredit int
}

func newStream(ctx context.Context, mux *Mux, id uint32, method string, initialWindow int) *Stream {
	if initialWindow <= 0 {
		initialWindow = defaultInitialWindow
	}
	c, cancel := context.WithCancel(ctx)
	s := &Stream{
		id:            id,
		method:        method,
		mux:           mux,
		ctx:           c,
		cancel:        cancel,
		maxRecvBytes:  initialWindow,
		recvSignal:    make(chan struct{}, 1),
		halfClosed:    make(chan struct{}),
		ended:         make(chan struct{}),
		initialWindow: initialWindow,
		sendWindow:    initialWindow,
	}
	s.sendCond = sync.NewCond(&s.mu)
	return s
}

// Context returns the stream context, cancelled on end/RST.
func (s *Stream) Context() context.Context { return s.ctx }

// Method returns the fully-qualified RPC method.
func (s *Stream) Method() string { return s.method }

// Header returns the leading headers observed for this stream: on the server
// side these are the request headers carried on OPEN; on the client side they
// are the optional leading response metadata carried on a KIND_HEADER frame
// (empty if the server never sent one).
func (s *Stream) Header() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.header
}

// Trailer returns the response trailers carried on the END frame (client side).
// Empty until the stream has ended.
func (s *Stream) Trailer() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailer
}

// SendHeader sends a single leading KIND_HEADER frame carrying response metadata
// before the first response message. It is server send-side only and may be
// called at most once, before any Send. Returns FailedPrecondition if headers
// were already sent or a message was already sent.
func (s *Stream) SendHeader(md map[string]string) error {
	s.mu.Lock()
	if s.headerSent || s.msgSent {
		s.mu.Unlock()
		return Errorf(codes.FailedPrecondition, "wsrpc: headers already sent")
	}
	s.headerSent = true
	s.mu.Unlock()
	return s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_HEADER,
		Headers:  md,
	})
}

// SetTrailer merges md into the trailers flushed with the END frame (server
// send-side). Safe to call multiple times; later keys win.
func (s *Stream) SetTrailer(md map[string]string) {
	if len(md) == 0 {
		return
	}
	s.mu.Lock()
	if s.outTrailer == nil {
		s.outTrailer = make(map[string]string, len(md))
	}
	for k, v := range md {
		s.outTrailer[k] = v
	}
	s.mu.Unlock()
}

// takeTrailer returns the accumulated server send-side trailers (for serveStream).
func (s *Stream) takeTrailer() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outTrailer
}

// setLeadingHeader stores leading response metadata delivered via a KIND_HEADER
// frame (client receive-side).
func (s *Stream) setLeadingHeader(md map[string]string) {
	s.mu.Lock()
	s.header = md
	s.mu.Unlock()
}

// Send marshals msg and writes a MSG frame, blocking on the CALLER goroutine
// until enough send-window credit is available (per-stream flow control).
//
// Credit model: a MSG may be sent whenever sendWindow > 0; sendWindow is then
// decremented by len(payload) and is allowed to go NEGATIVE. This means a single
// message larger than the whole window is still delivered once any credit is
// available (matching gRPC/HTTP2 semantics, and avoiding a deadlock where a
// message bigger than the window could never be sent). Subsequent sends then
// wait until the peer returns enough credit to bring sendWindow back above 0.
//
// Blocking is on sendCond, signalled by creditSend (a KIND_WINDOW_UPDATE on the
// read loop) or by any terminal path (wakeSenders). The read loop is never
// blocked: it only ever signals the cond, never waits on it.
func (s *Stream) Send(msg proto.Message) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.SendRaw(b)
}

// SendRaw writes an already-marshaled message payload as a MSG frame, subject to
// the same flow-control blocking as Send. It exists for proxies that relay
// frames without knowing the message types; the bytes MUST be a valid marshaled
// protobuf message for the peer's decoder.
func (s *Stream) SendRaw(b []byte) error {
	n := len(b)

	s.mu.Lock()
	for s.sendWindow <= 0 {
		// Surface a terminal status / context cancellation rather than blocking
		// forever when the stream has ended or the caller's ctx is done.
		if err := s.sendBlockErrLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
		s.sendCond.Wait()
	}
	s.sendWindow -= n // may go negative for an oversized message; intentional
	s.msgSent = true
	s.mu.Unlock()

	if err := s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_MSG,
		Payload:  b,
	}); err != nil {
		return err
	}
	s.mux.stats.msgSent(s.ctx, s.method, n)
	return nil
}

// sendBlockErrLocked reports the error a blocked Send should return instead of
// waiting: a recorded terminal status, a pending terminal frame, or the context
// error. Caller holds mu.
func (s *Stream) sendBlockErrLocked() error {
	if s.endSt != nil {
		if s.endSt.Code != codes.OK {
			return s.endSt
		}
		return io.EOF
	}
	// A terminal END/RST frame may have arrived (signalEnd) before applyEnd ran.
	select {
	case <-s.ended:
		return io.EOF
	default:
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return nil
}

// creditSend adds delta bytes of credit to the send window and wakes any blocked
// Send. Called from the read loop on an inbound KIND_WINDOW_UPDATE; never blocks.
func (s *Stream) creditSend(delta int) {
	if delta <= 0 {
		return
	}
	s.mu.Lock()
	s.sendWindow += delta
	s.mu.Unlock()
	s.sendCond.Broadcast()
}

// wakeSenders unblocks any goroutine parked in Send so it can observe a terminal
// status / cancelled context. Called from every terminal path.
func (s *Stream) wakeSenders() {
	if s.sendCond != nil {
		s.sendCond.Broadcast()
	}
}

// deliverPayload consumes a MSG frame and, as part of consuming it, returns
// flow-control credit to the peer (KIND_WINDOW_UPDATE). Credit is returned on
// CONSUMPTION (here), not on arrival, which is what makes the window real
// backpressure: a sender only regains credit once the receiver has drained.
func (s *Stream) deliverPayload(f *transport.Frame) []byte {
	s.returnCredit(len(f.Payload))
	return f.Payload
}

// returnCredit accumulates consumed bytes and, once the pending total crosses
// initialWindow/2, sends a single coalesced KIND_WINDOW_UPDATE to the peer so it
// can resume a blocked Send. Coalescing avoids a window-update per message.
func (s *Stream) returnCredit(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.pendingCredit += n
	threshold := s.initialWindow / 2
	if threshold < 1 {
		threshold = 1
	}
	var delta uint32
	if s.pendingCredit >= threshold {
		delta = uint32(s.pendingCredit)
		s.pendingCredit = 0
	}
	s.mu.Unlock()
	if delta == 0 {
		return
	}
	// Best-effort: a write failure means the stream/conn is going away, in which
	// case credit no longer matters. Never blocks the read loop (runs on Recv).
	_ = s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_WINDOW_UPDATE,
		Window:   delta,
	})
}

// popRecv removes and returns the oldest buffered MSG frame, or nil if the
// backlog is empty. It also releases the frame's bytes from the byte bound.
func (s *Stream) popRecv() *transport.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recvItems) == 0 {
		return nil
	}
	f := s.recvItems[0]
	s.recvItems[0] = nil
	s.recvItems = s.recvItems[1:]
	s.recvBytes -= len(f.Payload)
	return f
}

// Recv waits for the next MSG and unmarshals into msg. Returns io.EOF on a
// clean END, or the *Status as error on a non-OK END / RST.
func (s *Stream) Recv(msg proto.Message) error {
	b, err := s.RecvRaw()
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, msg)
}

// RecvRaw waits for the next MSG and returns its payload without unmarshaling.
// Terminal semantics are identical to Recv: io.EOF on a clean END, the *Status
// as error on a non-OK END / RST. It exists for proxies that relay frames
// without knowing the message types.
func (s *Stream) RecvRaw() ([]byte, error) {
	for {
		// Prefer any already-buffered MSG so terminal/half-close signals never
		// preempt pending data (drain-first).
		if f := s.popRecv(); f != nil {
			return s.deliverPayload(f), nil
		}
		select {
		case <-s.recvSignal:
			// A frame was enqueued (or a spurious wake): loop and try to pop it.
		case <-s.halfClosed:
			if f := s.popRecv(); f != nil {
				return s.deliverPayload(f), nil
			}
			return nil, io.EOF
		case <-s.ended:
			return s.recvEnded()
		case <-s.ctx.Done():
			// A terminal frame may have closed ctx via failWith/applyEnd; surface a
			// recorded END/RST in preference to the bare context error.
			select {
			case <-s.ended:
				return s.recvEnded()
			default:
			}
			if f := s.popRecv(); f != nil {
				return s.deliverPayload(f), nil
			}
			return nil, s.ctx.Err()
		}
	}
}

// recvEnded is entered once a terminal END/RST has been recorded. Any MSGs
// still buffered are delivered first; only when the queue is empty is the
// terminal status returned.
func (s *Stream) recvEnded() ([]byte, error) {
	if f := s.popRecv(); f != nil {
		return s.deliverPayload(f), nil
	}
	s.mu.Lock()
	f := s.endFrame
	s.mu.Unlock()
	if f == nil {
		// Terminal via failWith (no frame), e.g. connection drop / overflow.
		if st := s.status(); st != nil && st.Code != codes.OK {
			return nil, st
		}
		return nil, io.EOF
	}
	switch f.Kind {
	case transport.Kind_KIND_END:
		st := s.applyEnd(f)
		if st.Code != codes.OK {
			return nil, st
		}
		return nil, io.EOF
	case transport.Kind_KIND_RST:
		st := s.applyEnd(f)
		if st.Code == codes.OK {
			st = &Status{Code: codes.Canceled, Message: "stream reset"}
		}
		return nil, st
	default:
		return nil, io.EOF
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
// It writes with the MUX context, not the stream context: a stream that ends
// BECAUSE its deadline fired has an already-cancelled s.ctx, and writing the
// terminal END with it would drop the frame so the client never learns the
// final status. The mux context stays live until the connection itself drops.
func (s *Stream) end(st *Status, trailers map[string]string) error {
	f := &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_END,
		Headers:  trailers,
		Status:   statusToProto(st),
	}
	return s.mux.write(s.mux.ctx, f)
}

func (s *Stream) applyEnd(f *transport.Frame) *Status {
	st := statusFromProto(f.Status)
	s.mu.Lock()
	if f.Headers != nil {
		s.trailer = f.Headers
	}
	s.endSt = st
	s.mu.Unlock()
	s.wakeSenders()
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
	s.wakeSenders()
	s.endOne.Do(func() { close(s.ended) }) // terminal, frameless
	s.cancel()
}

// tryDeliver enqueues a MSG frame without ever blocking the read loop. It
// returns false only when the byte-bounded backlog is already over its ceiling
// (a peer ignoring the flow-control window), which the mux turns into a stream
// reset. The check is "already over" rather than "would exceed": this always
// admits at least one message even if it is larger than the whole window
// (mirroring the send side's allowance), and a window-obeying peer — whose
// unconsumed bytes never exceed the window before it must stop for credit —
// never trips it.
func (s *Stream) tryDeliver(f *transport.Frame) bool {
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return true // stream already ending; dropping the frame is fine
	}
	if len(s.recvItems) > 0 && s.recvBytes > s.maxRecvBytes {
		s.mu.Unlock()
		return false
	}
	s.recvItems = append(s.recvItems, f)
	s.recvBytes += len(f.Payload)
	s.mu.Unlock()
	s.signalRecv()
	return true
}

// signalRecv wakes a parked Recv. The signal channel has capacity 1 and the send
// is non-blocking: a pending wake already covers any number of enqueues, and
// Recv re-checks the backlog after each wake.
func (s *Stream) signalRecv() {
	select {
	case s.recvSignal <- struct{}{}:
	default:
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
		s.wakeSenders()
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
