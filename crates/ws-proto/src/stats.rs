//! Server-side transport observability hooks (Go `wsrpc.ServerStats`).

use crate::status::Code;

/// Reasons passed to [`ServerStats::conn_rejected`] / [`ServerStats::stream_rejected`].
pub mod reason {
    pub const ORIGIN_POLICY: &str = "origin_policy";
    pub const UPGRADE: &str = "upgrade";
    pub const MAX_STREAMS: &str = "max_streams";
    pub const RECV_OVERFLOW: &str = "recv_overflow";
}

/// Callbacks for connection and stream lifecycle events. Every method has a
/// no-op default; override the ones you export. Called on hot paths: keep
/// them cheap and non-blocking.
pub trait ServerStats: Send + Sync + 'static {
    fn conn_opened(&self) {}
    fn conn_closed(&self) {}
    fn conn_rejected(&self, _reason: &str) {}
    fn stream_started(&self, _method: &str) {}
    fn stream_ended(&self, _method: &str, _code: Code) {}
    fn stream_rejected(&self, _reason: &str) {}
    fn msg_sent(&self, _method: &str, _bytes: usize) {}
    fn msg_received(&self, _method: &str, _bytes: usize) {}
}

/// Stats sink shared with the mux; `None` when the operator installed none.
pub type Stats = Option<std::sync::Arc<dyn ServerStats>>;
