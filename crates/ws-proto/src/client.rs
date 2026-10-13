//! Client connection: opens streams on a [`Mux`], runs calls through the
//! interceptor chain and (feature `tungstenite`) dials `ws://` / `wss://`
//! URLs negotiating `wsrpc.v1`, with optional auto-reconnect.
//!
//! Reconnect (Go `WithReconnect`): on connection loss a controller task
//! redials with capped exponential backoff and full jitter and installs a
//! fresh mux. In-flight streams on the dropped socket fail with
//! `Unavailable`; there is no stream resume. `new_stream` callers arriving
//! during the gap wait for the new socket (bounded by their own timeout).

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use prost::Message;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    call::{Request, Response},
    conn::WsConn,
    interceptor::{
        chain_streaming, chain_unary, AnyRequest, AnyResponse, BoxFuture, Interceptor, MethodSpec,
        StreamingClientConn, StreamingConn, StreamingFunc, UnaryFunc,
    },
    mux::{Keepalive, Mux, MuxConfig},
    status::{Code, Status},
    stream::{Metadata, Stream},
};

/// Backoff for auto-reconnect (Go `WithBackoff`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reconnect {
    /// First retry delay ceiling (Go default 100ms).
    pub initial: Duration,
    /// Delay ceiling after doubling (Go default 30s).
    pub max: Duration,
}

impl Default for Reconnect {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
        }
    }
}

impl Reconnect {
    /// Delay before retry `attempt` (0-based): `initial * 2^attempt`, capped
    /// at `max`, then uniformly jittered over `[0, d]` like Go.
    #[cfg_attr(not(feature = "tungstenite"), allow(dead_code))] // only dialers reconnect
    fn next_backoff(&self, attempt: u32, rng: &mut u64) -> Duration {
        let mut d = self.initial.as_nanos() as f64;
        for _ in 0..attempt {
            d *= 2.0;
            if d >= self.max.as_nanos() as f64 {
                d = self.max.as_nanos() as f64;
                break;
            }
        }
        let d = d.min(self.max.as_nanos() as f64) as u64;
        // xorshift64*: good enough for jitter, no extra dependency.
        *rng ^= *rng >> 12;
        *rng ^= *rng << 25;
        *rng ^= *rng >> 27;
        let r = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
        Duration::from_nanos(if d == 0 { 0 } else { r % (d + 1) })
    }
}

/// Dial tuning. Zero/default values keep the Go defaults.
#[derive(Clone, Default)]
pub struct ClientConfig {
    /// Per-stream flow-control window in bytes (default 256 KiB).
    pub initial_window: u32,
    /// Extra HTTP headers for the upgrade request (auth, tenant routing).
    pub headers: Metadata,
    /// Keepalive pings on dialed sockets; `None` = Go default (20s / 10s).
    /// Not applied to [`Client::from_conn`].
    pub keepalive: Option<Keepalive>,
    /// Inbound WebSocket message size limit (default 16 MiB).
    pub read_limit: Option<usize>,
    /// Auto-reconnect; `None` disables (default).
    pub reconnect: Option<Reconnect>,
    /// Client interceptors, outermost first.
    pub interceptors: Vec<Arc<dyn Interceptor>>,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("initial_window", &self.initial_window)
            .field("headers", &self.headers)
            .field("keepalive", &self.keepalive)
            .field("read_limit", &self.read_limit)
            .field("reconnect", &self.reconnect)
            .field("interceptors", &self.interceptors.len())
            .finish()
    }
}

impl ClientConfig {
    pub fn with_interceptor(mut self, i: impl Interceptor) -> Self {
        self.interceptors.push(Arc::new(i));
        self
    }
}

/// A multiplexed client connection. Cheap to clone; all clones share the
/// underlying connection.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    state: std::sync::Mutex<State>,
    /// Woken whenever `state.mux` changes.
    changed: Notify,
    /// Cancelled by [`Client::close`].
    close: CancellationToken,
    interceptors: Vec<Arc<dyn Interceptor>>,
}

#[derive(Default)]
struct State {
    /// `None` while a reconnect is in progress or after close.
    mux: Option<Arc<Mux>>,
    closed: bool,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = self.inner.state.lock().unwrap();
        f.debug_struct("Client")
            .field("connected", &st.mux.is_some())
            .field("closed", &st.closed)
            .finish()
    }
}

/// Dial attempt used by the reconnect controller.
#[cfg_attr(not(feature = "tungstenite"), allow(dead_code))]
type Redial = Arc<dyn Fn() -> BoxFuture<'static, Result<Mux, Status>> + Send + Sync>;

impl Client {
    /// Wraps an already-open connection (e.g. one end of [`conn::pipe`]).
    /// No keepalive, no reconnect; interceptors do apply.
    ///
    /// [`conn::pipe`]: crate::conn::pipe
    pub fn from_conn<C: WsConn + 'static>(conn: C, cfg: ClientConfig) -> Self {
        let mux = Mux::new(
            conn,
            MuxConfig {
                initial_window: cfg.initial_window,
                max_streams: 0,
                keepalive: None,
                stats: None,
            },
            None,
        );
        Self::with_mux(mux, cfg.interceptors)
    }

    fn with_mux(mux: Mux, interceptors: Vec<Arc<dyn Interceptor>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: std::sync::Mutex::new(State {
                    mux: Some(Arc::new(mux)),
                    closed: false,
                }),
                changed: Notify::new(),
                close: CancellationToken::new(),
                interceptors,
            }),
        }
    }

    /// Current mux, waiting out a reconnect gap (bounded by `timeout`).
    async fn mux(&self, timeout: Option<Duration>) -> Result<Arc<Mux>, Status> {
        let wait = async {
            loop {
                let notified = self.inner.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let st = self.inner.state.lock().unwrap();
                    if st.closed {
                        return Err(Status::new(Code::Unavailable, "wsrpc: connection closed"));
                    }
                    if let Some(m) = &st.mux {
                        return Ok(Arc::clone(m));
                    }
                }
                notified.await;
            }
        };
        match timeout {
            Some(t) => tokio::time::timeout(t, wait).await.map_err(|_| {
                Status::new(
                    Code::DeadlineExceeded,
                    "wsrpc: timed out waiting for reconnect",
                )
            })?,
            None => wait.await,
        }
    }

    /// Opens a raw stream for `method`, bypassing interceptors.
    pub async fn new_stream(
        &self,
        method: &str,
        headers: Metadata,
        timeout: Option<Duration>,
    ) -> Result<Stream, Status> {
        self.mux(timeout)
            .await?
            .open_stream(method, headers, timeout)
            .await
    }

    /// Runs a unary RPC through the interceptor chain: open, send,
    /// half-close, receive one message, then drain the terminal END so a
    /// trailing error status is surfaced.
    pub async fn invoke_unary<Req, Resp>(
        &self,
        spec: MethodSpec,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let any = AnyRequest {
            spec,
            header: req.header,
            timeout: req.timeout,
            msg: Bytes::from(req.msg.encode_to_vec()),
        };
        let this = self.clone();
        let terminal: UnaryFunc = Arc::new(move |r: AnyRequest| {
            let this = this.clone();
            Box::pin(async move { this.unary_terminal(r).await })
        });
        let resp = chain_unary(&self.inner.interceptors, terminal)(any).await?;
        let msg = Resp::decode(resp.msg)
            .map_err(|e| Status::new(Code::Internal, format!("wsrpc: decode: {e}")))?;
        Ok(Response {
            msg,
            header: resp.header,
            trailer: resp.trailer,
        })
    }

    async fn unary_terminal(&self, r: AnyRequest) -> Result<AnyResponse, Status> {
        let s = self.new_stream(r.spec.route, r.header, r.timeout).await?;
        s.send_raw(r.msg).await?;
        s.close_send().await?;
        let Some(msg) = s.recv_raw().await? else {
            return Err(Status::new(
                Code::Internal,
                "wsrpc: unary call ended without a response message",
            ));
        };
        if s.recv_raw().await?.is_some() {
            return Err(Status::new(
                Code::Internal,
                "wsrpc: unary call received more than one response message",
            ));
        }
        Ok(AnyResponse {
            spec: r.spec,
            header: s.header(),
            trailer: s.trailer(),
            msg,
        })
    }

    /// Opens a streaming RPC through the interceptor chain. Generated stream
    /// wrappers drive the returned connection.
    pub async fn open_streaming(
        &self,
        spec: MethodSpec,
        header: Metadata,
        timeout: Option<Duration>,
    ) -> Result<StreamingConn, Status> {
        let this = self.clone();
        let terminal: StreamingFunc = Arc::new(move |spec, header: Metadata, timeout| {
            let this = this.clone();
            Box::pin(async move {
                let s = this.new_stream(spec.route, header.clone(), timeout).await?;
                Ok(Arc::new(StreamConn { s, spec, header }) as StreamingConn)
            })
        });
        chain_streaming(&self.inner.interceptors, terminal)(spec, header, timeout).await
    }

    /// Deliberately closes the connection and stops any reconnect; in-flight
    /// streams fail with `Cancelled`, later calls with `Unavailable`.
    pub fn close(&self) {
        let mux = {
            let mut st = self.inner.state.lock().unwrap();
            st.closed = true;
            st.mux.take()
        };
        self.inner.close.cancel();
        self.inner.changed.notify_waiters();
        if let Some(m) = mux {
            m.close();
        }
    }

    /// Resolves once the current connection is gone. With reconnect enabled
    /// this fires per socket loss; use [`Client::closed_for_good`] to wait for
    /// [`Client::close`].
    pub async fn closed(&self) {
        let mux = self.inner.state.lock().unwrap().mux.clone();
        match mux {
            Some(m) => m.closed().await,
            None => self.inner.close.cancelled().await,
        }
    }

    /// Resolves once [`Client::close`] has been called.
    pub async fn closed_for_good(&self) {
        self.inner.close.cancelled().await
    }

    /// True while a socket is installed (false during a reconnect gap).
    pub fn is_connected(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap()
            .mux
            .as_ref()
            .is_some_and(|m| !m.is_closed())
    }

    /// Reconnect controller: waits for the live mux to die, redials with
    /// backoff, installs the new mux, repeats until closed.
    #[cfg_attr(not(feature = "tungstenite"), allow(dead_code))]
    fn spawn_reconnector(&self, cfg: Reconnect, redial: Redial) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut rng = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
                | 1;
            loop {
                let mux = this.inner.state.lock().unwrap().mux.clone();
                let Some(mux) = mux else { return };
                tokio::select! {
                    _ = this.inner.close.cancelled() => return,
                    _ = mux.closed() => {}
                }
                {
                    let mut st = this.inner.state.lock().unwrap();
                    if st.closed {
                        return;
                    }
                    st.mux = None;
                }
                this.inner.changed.notify_waiters();

                let mut attempt = 0u32;
                let new_mux = loop {
                    let dial = redial();
                    let res = tokio::select! {
                        _ = this.inner.close.cancelled() => return,
                        r = dial => r,
                    };
                    match res {
                        Ok(m) => break m,
                        Err(_) => {
                            let sleep = cfg.next_backoff(attempt, &mut rng);
                            attempt = attempt.saturating_add(1);
                            tokio::select! {
                                _ = this.inner.close.cancelled() => return,
                                _ = tokio::time::sleep(sleep) => {}
                            }
                        }
                    }
                };
                {
                    let mut st = this.inner.state.lock().unwrap();
                    if st.closed {
                        drop(st);
                        new_mux.close();
                        return;
                    }
                    st.mux = Some(Arc::new(new_mux));
                }
                this.inner.changed.notify_waiters();
            }
        });
    }
}

/// Terminal [`StreamingClientConn`] over a raw [`Stream`].
struct StreamConn {
    s: Stream,
    spec: MethodSpec,
    header: Metadata,
}

impl StreamingClientConn for StreamConn {
    fn spec(&self) -> MethodSpec {
        self.spec
    }
    fn request_header(&self) -> Metadata {
        self.header.clone()
    }
    fn send(&self, msg: Bytes) -> BoxFuture<'_, Result<(), Status>> {
        Box::pin(self.s.send_raw(msg))
    }
    fn close_request(&self) -> BoxFuture<'_, Result<(), Status>> {
        Box::pin(self.s.close_send())
    }
    fn receive(&self) -> BoxFuture<'_, Result<Option<Bytes>, Status>> {
        Box::pin(self.s.recv_raw())
    }
    fn response_header(&self) -> Metadata {
        self.s.header()
    }
    fn response_trailer(&self) -> Metadata {
        self.s.trailer()
    }
}

/// Marshals and sends one message on a [`StreamingConn`] (generated wrappers).
pub async fn send_msg<M: Message>(conn: &StreamingConn, msg: &M) -> Result<(), Status> {
    conn.send(Bytes::from(msg.encode_to_vec())).await
}

/// Receives and unmarshals the next message from a [`StreamingConn`].
pub async fn recv_msg<M: Message + Default>(conn: &StreamingConn) -> Result<Option<M>, Status> {
    match conn.receive().await? {
        None => Ok(None),
        Some(b) => M::decode(b)
            .map(Some)
            .map_err(|e| Status::new(Code::Internal, format!("wsrpc: decode: {e}"))),
    }
}

#[cfg(feature = "tungstenite")]
mod ws {
    use std::sync::Arc;

    use http::HeaderValue;
    use tokio_tungstenite::tungstenite::{
        self, client::IntoClientRequest, protocol::WebSocketConfig,
    };

    use super::{Client, ClientConfig};
    use crate::{
        conn::tungstenite::TungsteniteConn,
        mux::{Keepalive, Mux, MuxConfig},
        server::DEFAULT_READ_LIMIT,
        Code, Status, SUBPROTOCOL,
    };

    impl Client {
        /// Dials `url` (`ws://` or `wss://`), offering `wsrpc.v1` and failing
        /// with `Unavailable` if the server does not select it. With
        /// `cfg.reconnect` set, a controller keeps the connection alive.
        pub async fn dial(url: &str, cfg: ClientConfig) -> Result<Self, Status> {
            let url = url.to_owned();
            let dial_cfg = Arc::new(ClientConfig {
                interceptors: Vec::new(),
                ..cfg.clone()
            });
            let mux = dial_mux(&url, &dial_cfg).await?;
            let client = Self::with_mux(mux, cfg.interceptors);
            if let Some(rc) = cfg.reconnect {
                let redial: super::Redial = Arc::new(move || {
                    let url = url.clone();
                    let cfg = Arc::clone(&dial_cfg);
                    Box::pin(async move { dial_mux(&url, &cfg).await })
                });
                client.spawn_reconnector(rc, redial);
            }
            Ok(client)
        }
    }

    async fn dial_mux(url: &str, cfg: &ClientConfig) -> Result<Mux, Status> {
        let mut req = url
            .into_client_request()
            .map_err(|e| Status::new(Code::InvalidArgument, format!("wsrpc: dial: {e}")))?;
        req.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
        for (k, v) in &cfg.headers {
            let name = http::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Status::new(Code::InvalidArgument, format!("wsrpc: header: {e}")))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| Status::new(Code::InvalidArgument, format!("wsrpc: header: {e}")))?;
            req.headers_mut().insert(name, value);
        }
        let limit = cfg.read_limit.unwrap_or(DEFAULT_READ_LIMIT);
        let ws_cfg = WebSocketConfig::default()
            .max_message_size(Some(limit))
            .max_frame_size(Some(limit));
        let (ws, resp) = tokio_tungstenite::connect_async_with_config(req, Some(ws_cfg), false)
            .await
            .map_err(|e| Status::new(Code::Unavailable, format!("wsrpc: dial: {e}")))?;
        let got = resp
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if got != SUBPROTOCOL {
            let mut ws = ws;
            let _ = ws
                .close(Some(tungstenite::protocol::CloseFrame {
                    code: tungstenite::protocol::frame::coding::CloseCode::Protocol,
                    reason: "wsrpc: subprotocol not negotiated".into(),
                }))
                .await;
            return Err(Status::new(
                Code::Unavailable,
                format!(
                    "wsrpc: server did not negotiate subprotocol {SUBPROTOCOL:?} (got {got:?})"
                ),
            ));
        }
        Ok(Mux::new(
            TungsteniteConn::new(ws),
            MuxConfig {
                initial_window: cfg.initial_window,
                max_streams: 0,
                keepalive: Some(cfg.keepalive.unwrap_or(Keepalive::DEFAULT)),
                stats: None,
            },
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_caps_and_jitters() {
        let rc = Reconnect {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(350),
        };
        let mut rng = 12345u64;
        for attempt in 0..6 {
            let cap = (100u64 << attempt).min(350);
            for _ in 0..20 {
                let d = rc.next_backoff(attempt, &mut rng);
                assert!(
                    d.as_millis() as u64 <= cap,
                    "attempt {attempt}: {d:?} > {cap}ms"
                );
            }
        }
    }
}
