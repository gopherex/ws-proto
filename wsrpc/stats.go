package wsrpc

import (
	"context"

	"google.golang.org/grpc/codes"
)

// ServerStats observes server transport events that per-RPC middleware cannot
// see: connection lifecycle, upgrade/stream rejections, and per-message flow.
// Install it with WithStats.
//
// Every callback is optional (nil fields are skipped). Callbacks MUST be fast
// and non-blocking: MsgSent/MsgReceived run on the send path and the shared
// read loop respectively — a blocking callback stalls every stream on the
// connection. Typical implementations only bump counters (e.g. OpenTelemetry
// instruments).
type ServerStats struct {
	// ConnOpened fires after a successful WebSocket upgrade (or ServeConn
	// start), with the per-connection base context (WithConnContext applied).
	ConnOpened func(ctx context.Context)
	// ConnClosed fires when the connection's mux terminates, with the same
	// base context as ConnOpened.
	ConnClosed func(ctx context.Context)
	// ConnRejected fires when an upgrade never becomes a connection, with the
	// HTTP request context. Reasons: "origin_policy" (fail-closed origin gate,
	// see NewServer), "upgrade" (websocket handshake failed).
	ConnRejected func(ctx context.Context, reason string)
	// StreamStarted fires when a dispatched stream reaches its handler chain.
	StreamStarted func(ctx context.Context, method string)
	// StreamEnded fires when the handler chain returns, with the terminal
	// status code sent to the peer.
	StreamEnded func(ctx context.Context, method string, code codes.Code)
	// StreamRejected fires when a stream dies without (or outside) its handler,
	// with the connection context. Reasons: "max_streams"
	// (WithMaxConcurrentStreams cap; the stream never existed), "recv_overflow"
	// (slow consumer overflowed the bounded inbound backlog; the stream is reset).
	StreamRejected func(ctx context.Context, reason string)
	// MsgSent fires after a response MSG frame is written, with its payload size.
	MsgSent func(ctx context.Context, method string, bytes int)
	// MsgReceived fires when an inbound MSG frame is accepted into a stream's
	// backlog, with its payload size. Runs on the shared read loop.
	MsgReceived func(ctx context.Context, method string, bytes int)
}

// Rejection reasons reported via ServerStats.
const (
	statsReasonOriginPolicy = "origin_policy"
	statsReasonUpgrade      = "upgrade"
	statsReasonMaxStreams   = "max_streams"
	statsReasonRecvOverflow = "recv_overflow"
)

// connOpened/connClosed/connRejected/streamStarted/streamEnded/streamRejected/
// msgSent/msgReceived are nil-safe dispatch helpers.
func (st *ServerStats) connOpened(ctx context.Context) {
	if st != nil && st.ConnOpened != nil {
		st.ConnOpened(ctx)
	}
}

func (st *ServerStats) connClosed(ctx context.Context) {
	if st != nil && st.ConnClosed != nil {
		st.ConnClosed(ctx)
	}
}

func (st *ServerStats) connRejected(ctx context.Context, reason string) {
	if st != nil && st.ConnRejected != nil {
		st.ConnRejected(ctx, reason)
	}
}

func (st *ServerStats) streamStarted(ctx context.Context, method string) {
	if st != nil && st.StreamStarted != nil {
		st.StreamStarted(ctx, method)
	}
}

func (st *ServerStats) streamEnded(ctx context.Context, method string, code codes.Code) {
	if st != nil && st.StreamEnded != nil {
		st.StreamEnded(ctx, method, code)
	}
}

func (st *ServerStats) streamRejected(ctx context.Context, reason string) {
	if st != nil && st.StreamRejected != nil {
		st.StreamRejected(ctx, reason)
	}
}

func (st *ServerStats) msgSent(ctx context.Context, method string, bytes int) {
	if st != nil && st.MsgSent != nil {
		st.MsgSent(ctx, method, bytes)
	}
}

func (st *ServerStats) msgReceived(ctx context.Context, method string, bytes int) {
	if st != nil && st.MsgReceived != nil {
		st.MsgReceived(ctx, method, bytes)
	}
}
