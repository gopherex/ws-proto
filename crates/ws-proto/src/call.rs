//! Request/response envelopes carrying per-RPC metadata alongside the message.

use std::time::Duration;

use crate::stream::Metadata;

/// An inbound (server) or outbound (client) request: message + metadata.
#[derive(Debug, Clone, Default)]
pub struct Request<T> {
    pub msg: T,
    /// Request headers (OPEN frame).
    pub header: Metadata,
    /// Caller deadline budget; sent as `ws-timeout-ms`, enforced server-side.
    pub timeout: Option<Duration>,
}

impl<T> Request<T> {
    pub fn new(msg: T) -> Self {
        Self {
            msg,
            header: Metadata::new(),
            timeout: None,
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.header.insert(key.into(), value.into());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn into_inner(self) -> T {
        self.msg
    }
}

impl<T> From<T> for Request<T> {
    fn from(msg: T) -> Self {
        Self::new(msg)
    }
}

/// A unary response: message + leading header + trailers.
#[derive(Debug, Clone, Default)]
pub struct Response<T> {
    pub msg: T,
    /// Leading response metadata (HEADER frame).
    pub header: Metadata,
    /// Trailers (END frame).
    pub trailer: Metadata,
}

impl<T> Response<T> {
    pub fn new(msg: T) -> Self {
        Self {
            msg,
            header: Metadata::new(),
            trailer: Metadata::new(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.header.insert(key.into(), value.into());
        self
    }

    pub fn with_trailer(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.trailer.insert(key.into(), value.into());
        self
    }

    pub fn into_inner(self) -> T {
        self.msg
    }
}

impl<T> From<T> for Response<T> {
    fn from(msg: T) -> Self {
        Self::new(msg)
    }
}

/// Per-call options for streaming RPCs that carry no initial message.
#[derive(Debug, Clone, Default)]
pub struct CallOptions {
    pub header: Metadata,
    pub timeout: Option<Duration>,
}

impl CallOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.header.insert(key.into(), value.into());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
