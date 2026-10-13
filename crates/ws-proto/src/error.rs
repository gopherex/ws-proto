//! Runtime error type.

/// Errors surfaced by the transport layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The underlying WebSocket connection failed or closed abnormally.
    #[error("websocket transport: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// The peer violated the `wsrpc.v1` protocol (e.g. sent a text message or
    /// an undecodable frame). The connection should be closed.
    #[error("protocol violation: {0}")]
    Protocol(String),
}

impl Error {
    pub(crate) fn transport<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Transport(Box::new(e))
    }
}
