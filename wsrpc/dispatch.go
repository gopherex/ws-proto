package wsrpc

import (
	"context"
	"io"

	"google.golang.org/protobuf/proto"
)

// InvokeUnary runs a unary RPC through the client interceptor chain. The
// generated client calls it: req is the request message, newOut allocates a
// fresh response message, header carries request metadata. It returns the
// response message (type-asserted by the generated code).
func InvokeUnary(
	ctx context.Context,
	cc *ClientConn,
	spec MethodSpec,
	req proto.Message,
	newOut func() proto.Message,
	header map[string]string,
) (proto.Message, error) {
	terminal := func(ctx context.Context, r *AnyRequest) (*AnyResponse, error) {
		s, err := cc.NewStream(ctx, r.Spec.Route, r.Header)
		if err != nil {
			return nil, err
		}
		if err := s.Send(r.Msg); err != nil {
			return nil, err
		}
		if err := s.CloseSend(); err != nil {
			return nil, err
		}
		out := newOut()
		if err := s.Recv(out); err != nil {
			return nil, err
		}
		// Drain the trailing END so a trailing error status is surfaced and the
		// stream closes cleanly.
		if err := s.Recv(newOut()); err != nil && err != io.EOF {
			return nil, err
		}
		return &AnyResponse{Spec: r.Spec, Header: s.Header(), Trailer: s.Trailer(), Msg: out}, nil
	}
	if header == nil {
		header = map[string]string{} // interceptors must be able to add metadata
	}
	chain := chainUnary(cc.interceptors, terminal)
	resp, err := chain(ctx, &AnyRequest{Spec: spec, Header: header, Msg: req})
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// OpenStreamingClient opens a streaming RPC through the client interceptor
// chain and returns the (possibly wrapped) connection the generated stream
// wrapper drives.
func OpenStreamingClient(
	ctx context.Context,
	cc *ClientConn,
	spec MethodSpec,
	header map[string]string,
) (StreamingClientConn, error) {
	terminal := func(ctx context.Context, spec MethodSpec, header map[string]string) (StreamingClientConn, error) {
		s, err := cc.NewStream(ctx, spec.Route, header)
		if err != nil {
			return nil, err
		}
		return &streamConn{s: s, spec: spec, reqHeader: header}, nil
	}
	if header == nil {
		header = map[string]string{} // interceptors must be able to add metadata
	}
	chain := chainStreaming(cc.interceptors, terminal)
	return chain(ctx, spec, header)
}

// streamConn is the terminal StreamingClientConn wrapping a *Stream.
type streamConn struct {
	s         *Stream
	spec      MethodSpec
	reqHeader map[string]string
}

func (c *streamConn) Spec() MethodSpec                   { return c.spec }
func (c *streamConn) RequestHeader() map[string]string   { return c.reqHeader }
func (c *streamConn) Send(m proto.Message) error         { return c.s.Send(m) }
func (c *streamConn) CloseRequest() error                { return c.s.CloseSend() }
func (c *streamConn) Receive(m proto.Message) error      { return c.s.Recv(m) }
func (c *streamConn) ResponseHeader() map[string]string  { return c.s.Header() }
func (c *streamConn) ResponseTrailer() map[string]string { return c.s.Trailer() }
func (c *streamConn) CloseResponse() error               { c.s.cancel(); return nil }
