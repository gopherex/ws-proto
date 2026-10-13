//! Client-side interceptors (Go `wsrpc.Interceptor`, modelled on connect-go).
//!
//! Interceptors wrap the two client entry points the generated code uses:
//! unary calls ([`UnaryFunc`]) and streaming-call opens ([`StreamingFunc`]).
//! They see metadata and marshaled payloads ([`AnyRequest`] / [`AnyResponse`])
//! and the [`MethodSpec`], so one interceptor serves every service. The chain
//! is applied in registration order: the first interceptor is outermost.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;

use crate::{status::Status, stream::Metadata};

/// RPC shape of a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    Unary,
    ServerStream,
    ClientStream,
    BidiStream,
}

impl StreamKind {
    pub const fn client_streaming(self) -> bool {
        matches!(self, Self::ClientStream | Self::BidiStream)
    }
    pub const fn server_streaming(self) -> bool {
        matches!(self, Self::ServerStream | Self::BidiStream)
    }
}

/// Identifies a method to interceptors: wire route and RPC shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodSpec {
    /// `/pkg.Service/Method`.
    pub route: &'static str,
    pub kind: StreamKind,
}

/// A unary request as seen by interceptors: metadata plus the marshaled message.
#[derive(Debug, Clone)]
pub struct AnyRequest {
    pub spec: MethodSpec,
    pub header: Metadata,
    pub timeout: Option<Duration>,
    pub msg: Bytes,
}

/// A unary response as seen by interceptors.
#[derive(Debug, Clone)]
pub struct AnyResponse {
    pub spec: MethodSpec,
    /// Leading response metadata (HEADER frame).
    pub header: Metadata,
    /// Trailers (END frame).
    pub trailer: Metadata,
    pub msg: Bytes,
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The unary call path: terminal implementation or an interceptor wrapping it.
pub type UnaryFunc =
    Arc<dyn Fn(AnyRequest) -> BoxFuture<'static, Result<AnyResponse, Status>> + Send + Sync>;

/// The streaming open path.
pub type StreamingFunc = Arc<
    dyn Fn(
            MethodSpec,
            Metadata,
            Option<Duration>,
        ) -> BoxFuture<'static, Result<StreamingConn, Status>>
        + Send
        + Sync,
>;

/// A streaming RPC from the client's side, type-erased so interceptors can
/// wrap it (observe or rewrite messages, metadata, termination).
pub trait StreamingClientConn: Send + Sync {
    fn spec(&self) -> MethodSpec;
    fn request_header(&self) -> Metadata;
    /// Sends one marshaled request message.
    fn send(&self, msg: Bytes) -> BoxFuture<'_, Result<(), Status>>;
    /// Half-closes the request side.
    fn close_request(&self) -> BoxFuture<'_, Result<(), Status>>;
    /// Receives the next marshaled response; `Ok(None)` on a clean end.
    fn receive(&self) -> BoxFuture<'_, Result<Option<Bytes>, Status>>;
    fn response_header(&self) -> Metadata;
    fn response_trailer(&self) -> Metadata;
}

pub type StreamingConn = Arc<dyn StreamingClientConn>;

/// A client interceptor. Override one or both hooks; the default passes the
/// call through untouched.
pub trait Interceptor: Send + Sync + 'static {
    fn wrap_unary(&self, next: UnaryFunc) -> UnaryFunc {
        next
    }
    fn wrap_streaming(&self, next: StreamingFunc) -> StreamingFunc {
        next
    }
}

/// Builds a [`UnaryFunc`] from an async closure (saves the boxing boilerplate).
pub fn unary_fn<F, Fut>(f: F) -> UnaryFunc
where
    F: Fn(AnyRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<AnyResponse, Status>> + Send + 'static,
{
    Arc::new(move |req| Box::pin(f(req)))
}

/// Builds a [`StreamingFunc`] from an async closure.
pub fn streaming_fn<F, Fut>(f: F) -> StreamingFunc
where
    F: Fn(MethodSpec, Metadata, Option<Duration>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<StreamingConn, Status>> + Send + 'static,
{
    Arc::new(move |spec, header, timeout| Box::pin(f(spec, header, timeout)))
}

pub(crate) fn chain_unary(interceptors: &[Arc<dyn Interceptor>], terminal: UnaryFunc) -> UnaryFunc {
    interceptors
        .iter()
        .rev()
        .fold(terminal, |next, i| i.wrap_unary(next))
}

pub(crate) fn chain_streaming(
    interceptors: &[Arc<dyn Interceptor>],
    terminal: StreamingFunc,
) -> StreamingFunc {
    interceptors
        .iter()
        .rev()
        .fold(terminal, |next, i| i.wrap_streaming(next))
}

/// Interceptor from a closure wrapping only the unary path.
pub struct UnaryInterceptorFn<F>(pub F);

impl<F> Interceptor for UnaryInterceptorFn<F>
where
    F: Fn(UnaryFunc) -> UnaryFunc + Send + Sync + 'static,
{
    fn wrap_unary(&self, next: UnaryFunc) -> UnaryFunc {
        (self.0)(next)
    }
}

/// Interceptor from a closure wrapping only the streaming path.
pub struct StreamInterceptorFn<F>(pub F);

impl<F> Interceptor for StreamInterceptorFn<F>
where
    F: Fn(StreamingFunc) -> StreamingFunc + Send + Sync + 'static,
{
    fn wrap_streaming(&self, next: StreamingFunc) -> StreamingFunc {
        (self.0)(next)
    }
}
