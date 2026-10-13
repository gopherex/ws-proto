//! Server: a registry of per-method handlers served over any [`WsConn`], plus
//! (feature `tungstenite`) the WebSocket upgrade with the same origin policy
//! and subprotocol negotiation as the Go `wsrpc.Server`.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use prost::Message;

use crate::{
    call::{Request, Response},
    conn::WsConn,
    mux::{Keepalive, Mux, MuxConfig, OnOpen},
    stats::{ServerStats, Stats},
    status::{Code, Status},
    stream::Stream,
};

/// Default cap on concurrent streams per connection (Go `defaultMaxStreams`).
pub const DEFAULT_MAX_STREAMS: usize = 1000;
/// Default inbound WebSocket message limit (Go `defaultReadLimit`, 16 MiB).
pub const DEFAULT_READ_LIMIT: usize = 16 << 20;

/// Runs one server-side RPC stream. Returning `Err` ends the stream with that
/// status; `Ok(())` ends it with `Code::Ok`.
pub type Handler =
    Arc<dyn Fn(Stream) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send>> + Send + Sync>;

/// Wraps a [`Handler`] (Go `wsrpc.Middleware`). The first middleware added
/// is outermost. Runs for registered methods and the unknown-method fallback.
pub type Middleware = Arc<dyn Fn(Handler) -> Handler + Send + Sync>;

/// Browser origin policy for the WebSocket upgrade. Fail-closed like Go: with
/// [`OriginPolicy::Unconfigured`] every upgrade is rejected with 403.
#[derive(Debug, Clone, Default)]
pub enum OriginPolicy {
    /// Reject all upgrades (operator must choose a policy).
    #[default]
    Unconfigured,
    /// Accept requests without an `Origin`, same-origin requests
    /// (`Origin` host == `Host`), and origins whose host matches one of the
    /// glob patterns (`app.example.com`, `*.example.com`, `*`). An empty list
    /// is "same-origin only".
    Patterns(Vec<String>),
    /// Accept any origin. Only safe when auth is enforced on the upgrade.
    InsecureSkip,
}

impl OriginPolicy {
    pub fn same_origin_only() -> Self {
        Self::Patterns(Vec::new())
    }

    /// `origin`: the `Origin` header if present; `host`: the `Host` header.
    pub fn allows(&self, origin: Option<&str>, host: Option<&str>) -> bool {
        match self {
            Self::Unconfigured => false,
            Self::InsecureSkip => true,
            Self::Patterns(patterns) => {
                let Some(origin) = origin else { return true };
                let origin_host = origin
                    .split_once("://")
                    .map(|(_, rest)| rest)
                    .unwrap_or(origin)
                    .split('/')
                    .next()
                    .unwrap_or("");
                if Some(origin_host) == host {
                    return true;
                }
                patterns.iter().any(|p| glob_match(p, origin_host))
            }
        }
    }
}

/// Go `path.Match` semantics on a host: `*` matches any run of non-`/`
/// characters (dots included), `?` one such character, the rest literal.
fn glob_match(pattern: &str, s: &str) -> bool {
    fn rec(p: &[u8], s: &[u8]) -> bool {
        match p.split_first() {
            None => s.is_empty(),
            Some((b'*', rest)) => (0..=s.len())
                .take_while(|&i| i == 0 || s[i - 1] != b'/')
                .any(|i| rec(rest, &s[i..])),
            Some((b'?', rest)) => !s.is_empty() && s[0] != b'/' && rec(rest, &s[1..]),
            Some((c, rest)) => s.first() == Some(c) && rec(rest, &s[1..]),
        }
    }
    rec(pattern.as_bytes(), s.as_bytes())
}

/// Server tuning. Zero/`None` values keep the defaults.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub origin: OriginPolicy,
    /// Per-stream flow-control window in bytes (default 256 KiB).
    pub initial_window: u32,
    /// Cap on concurrent streams per connection (default 1000; `Some(0)` =
    /// unlimited).
    pub max_streams: Option<usize>,
    /// Inbound WebSocket message size limit (default 16 MiB).
    pub read_limit: usize,
    /// Keepalive pings on upgraded sockets (Go default 20s / 10s). Not applied
    /// to [`Server::serve_conn`].
    pub keepalive: Keepalive,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            origin: OriginPolicy::Unconfigured,
            initial_window: 0,
            max_streams: None,
            read_limit: DEFAULT_READ_LIMIT,
            keepalive: Keepalive::DEFAULT,
        }
    }
}

/// Dispatches streams to handlers keyed by fully-qualified method.
#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: ServerConfig,
    handlers: std::sync::RwLock<HashMap<String, Handler>>,
    /// Route-prefix handlers (`/pkg.Service/`), consulted after exact matches;
    /// longest prefix wins. Used by `register_grpc`, handy for proxies.
    prefixes: std::sync::RwLock<Vec<(String, Handler)>>,
    unknown: std::sync::RwLock<Option<Handler>>,
    middleware: std::sync::RwLock<Vec<Middleware>>,
    stats: std::sync::RwLock<Stats>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("config", &self.inner.cfg)
            .field("methods", &self.inner.handlers.read().unwrap().len())
            .field("prefixes", &self.inner.prefixes.read().unwrap().len())
            .finish()
    }
}

impl Server {
    pub fn new(cfg: ServerConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                cfg,
                handlers: Default::default(),
                prefixes: Default::default(),
                unknown: Default::default(),
                middleware: Default::default(),
                stats: Default::default(),
            }),
        }
    }

    pub fn config(&self) -> &ServerConfig {
        &self.inner.cfg
    }

    /// Binds a raw stream handler to `/pkg.Service/Method`.
    pub fn register<F, Fut>(&self, method: impl Into<String>, handler: F)
    where
        F: Fn(Stream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let h: Handler = Arc::new(move |s| Box::pin(handler(s)));
        self.inner
            .handlers
            .write()
            .unwrap()
            .insert(method.into(), h);
    }

    /// Binds a handler to every method whose route starts with `prefix`
    /// (e.g. `/pkg.Service/`). Exact registrations win; among prefixes the
    /// longest wins.
    pub fn register_prefix<F, Fut>(&self, prefix: impl Into<String>, handler: F)
    where
        F: Fn(Stream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let h: Handler = Arc::new(move |s| Box::pin(handler(s)));
        let mut p = self.inner.prefixes.write().unwrap();
        p.push((prefix.into(), h));
        p.sort_by_key(|(k, _)| std::cmp::Reverse(k.len()));
    }

    /// Fallback for methods with no registered handler (proxies). Without it
    /// unknown methods end with `Unimplemented`.
    pub fn set_unknown_handler<F, Fut>(&self, handler: F)
    where
        F: Fn(Stream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let h: Handler = Arc::new(move |s| Box::pin(handler(s)));
        *self.inner.unknown.write().unwrap() = Some(h);
    }

    /// Appends a middleware; the first added is outermost.
    pub fn add_middleware<F>(&self, mw: F)
    where
        F: Fn(Handler) -> Handler + Send + Sync + 'static,
    {
        self.inner.middleware.write().unwrap().push(Arc::new(mw));
    }

    /// Installs observability hooks.
    pub fn set_stats(&self, stats: Arc<dyn ServerStats>) {
        *self.inner.stats.write().unwrap() = Some(stats);
    }

    fn stats(&self) -> Stats {
        self.inner.stats.read().unwrap().clone()
    }

    /// Registers a unary method: receive one request, run `f`, send header /
    /// response / trailers. Mirrors the Go generated registrar.
    pub fn register_unary<Req, Resp, F, Fut>(&self, method: impl Into<String>, f: F)
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Send + 'static,
        F: Fn(Request<Req>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
    {
        let f = Arc::new(f);
        self.register(method, move |s: Stream| {
            let f = Arc::clone(&f);
            async move {
                // A half-close with no message yields the default request,
                // as Go tolerates io.EOF here.
                let msg = s.recv::<Req>().await?.unwrap_or_default();
                let req = Request {
                    msg,
                    header: s.header(),
                    timeout: s.timeout(),
                };
                let resp = f(req).await?;
                if !resp.header.is_empty() {
                    let _ = s.send_header(resp.header).await;
                }
                s.set_trailer(resp.trailer);
                s.send(&resp.msg).await
            }
        });
    }

    /// Serves one connection until it closes. Streams are dispatched to
    /// handlers on their own tasks; origin/subprotocol checks and keepalive
    /// are skipped (no handshake, no proxy idle timeout on a handed-off conn).
    pub async fn serve_conn<C: WsConn + 'static>(&self, conn: C) {
        self.serve_mux(conn, None).await
    }

    async fn serve_mux<C: WsConn + 'static>(&self, conn: C, keepalive: Option<Keepalive>) {
        let stats = self.stats();
        if let Some(st) = &stats {
            st.conn_opened();
        }
        let srv = self.clone();
        let on_open: OnOpen = Box::new(move |s| {
            let srv = srv.clone();
            tokio::spawn(async move { srv.serve_stream(s).await });
        });
        let mux = Mux::new(
            conn,
            MuxConfig {
                initial_window: self.inner.cfg.initial_window,
                max_streams: self.inner.cfg.max_streams.unwrap_or(DEFAULT_MAX_STREAMS),
                keepalive,
                stats: stats.clone(),
            },
            Some(on_open),
        );
        mux.closed().await;
        if let Some(st) = &stats {
            st.conn_closed();
        }
    }

    async fn serve_stream(&self, stream: Stream) {
        let handler = {
            let handlers = self.inner.handlers.read().unwrap();
            handlers
                .get(stream.method())
                .cloned()
                .or_else(|| {
                    self.inner
                        .prefixes
                        .read()
                        .unwrap()
                        .iter()
                        .find(|(p, _)| stream.method().starts_with(p.as_str()))
                        .map(|(_, h)| h.clone())
                })
                .or_else(|| self.inner.unknown.read().unwrap().clone())
        };
        let Some(handler) = handler else {
            let _ = stream
                .end(Status::new(
                    Code::Unimplemented,
                    format!("unknown method {}", stream.method()),
                ))
                .await;
            return;
        };
        let handler = {
            let mw = self.inner.middleware.read().unwrap();
            mw.iter().rev().fold(handler, |h, m| m(h))
        };
        let stats = self.stats();
        let method = stream.method().to_owned();
        if let Some(st) = &stats {
            st.stream_started(&method);
        }

        // Run the handler on its own task so a panic is contained and becomes
        // Internal instead of killing the connection task.
        let fut = handler(stream.clone());
        let joined = match stream.timeout() {
            Some(t) => match tokio::time::timeout(t, tokio::spawn(fut)).await {
                Ok(j) => j,
                Err(_elapsed) => Ok(Err(Status::new(
                    Code::DeadlineExceeded,
                    "wsrpc: deadline exceeded",
                ))),
            },
            None => tokio::spawn(fut).await,
        };
        let st = match joined {
            Ok(Ok(())) => Status::ok(),
            Ok(Err(st)) => st,
            Err(e) if e.is_panic() => Status::new(Code::Internal, "wsrpc: panic in handler"),
            Err(_) => Status::new(Code::Cancelled, "wsrpc: handler cancelled"),
        };
        if let Some(s) = &stats {
            s.stream_ended(&method, st.code);
        }
        let _ = stream.end(st).await;
    }
}

#[cfg(feature = "tungstenite")]
mod ws {
    //! WebSocket upgrade for the server over `tokio-tungstenite`.

    use http::{HeaderValue, StatusCode};
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio_tungstenite::tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        protocol::WebSocketConfig,
    };

    use super::Server;
    use crate::{conn::tungstenite::TungsteniteConn, stats::reason, SUBPROTOCOL};

    impl Server {
        /// Performs the WebSocket handshake on a raw stream (e.g. an accepted
        /// `TcpStream`), applying the origin policy and negotiating
        /// `wsrpc.v1`, then serves it until it closes. Returns the handshake
        /// error if the upgrade was rejected.
        pub async fn accept<S>(
            &self,
            stream: S,
        ) -> Result<(), tokio_tungstenite::tungstenite::Error>
        where
            S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        {
            let policy = self.inner.cfg.origin.clone();
            let stats = self.stats();
            let rejected = std::sync::Arc::new(std::sync::Mutex::new(None::<&'static str>));
            let rejected_cb = std::sync::Arc::clone(&rejected);
            #[allow(clippy::result_large_err)] // tungstenite's Callback signature
            let callback = move |req: &Request, mut resp: Response| {
                let hdr = |n: &str| req.headers().get(n).and_then(|v| v.to_str().ok());
                if !policy.allows(hdr("origin"), hdr("host")) {
                    *rejected_cb.lock().unwrap() = Some(reason::ORIGIN_POLICY);
                    return Err(reject(
                        StatusCode::FORBIDDEN,
                        "wsrpc: origin not allowed (or origin policy not configured)",
                    ));
                }
                let offered = req
                    .headers()
                    .get_all("sec-websocket-protocol")
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .flat_map(|v| v.split(','))
                    .any(|p| p.trim() == SUBPROTOCOL);
                if !offered {
                    *rejected_cb.lock().unwrap() = Some(reason::UPGRADE);
                    return Err(reject(
                        StatusCode::BAD_REQUEST,
                        "wsrpc: subprotocol not offered",
                    ));
                }
                resp.headers_mut().insert(
                    "sec-websocket-protocol",
                    HeaderValue::from_static(SUBPROTOCOL),
                );
                Ok(resp)
            };
            let config = WebSocketConfig::default()
                .max_message_size(Some(self.inner.cfg.read_limit))
                .max_frame_size(Some(self.inner.cfg.read_limit));
            let ws = match tokio_tungstenite::accept_hdr_async_with_config(
                stream,
                callback,
                Some(config),
            )
            .await
            {
                Ok(ws) => ws,
                Err(e) => {
                    if let Some(st) = &stats {
                        st.conn_rejected(rejected.lock().unwrap().unwrap_or(reason::UPGRADE));
                    }
                    return Err(e);
                }
            };
            self.serve_mux(TungsteniteConn::new(ws), Some(self.inner.cfg.keepalive))
                .await;
            Ok(())
        }

        /// Accept loop over a bound listener: one task per connection.
        pub async fn serve_listener(
            &self,
            listener: tokio::net::TcpListener,
        ) -> std::io::Result<()> {
            loop {
                let (tcp, _) = listener.accept().await?;
                let srv = self.clone();
                tokio::spawn(async move {
                    let _ = srv.accept(tcp).await;
                });
            }
        }
    }

    fn reject(code: StatusCode, msg: &str) -> ErrorResponse {
        let mut r = ErrorResponse::new(Some(msg.to_owned()));
        *r.status_mut() = code;
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_policy() {
        let host = Some("api.example.com");
        assert!(!OriginPolicy::Unconfigured.allows(None, host));
        assert!(OriginPolicy::InsecureSkip.allows(Some("https://evil.com"), host));

        let same = OriginPolicy::same_origin_only();
        assert!(same.allows(None, host), "no Origin = non-browser");
        assert!(same.allows(Some("https://api.example.com"), host));
        assert!(!same.allows(Some("https://app.example.com"), host));

        let pats = OriginPolicy::Patterns(vec!["*.example.com".into()]);
        assert!(pats.allows(Some("https://app.example.com"), host));
        assert!(pats.allows(Some("https://deep.app.example.com"), host));
        assert!(!pats.allows(Some("https://example.com"), host));
        assert!(!pats.allows(Some("https://example.com.evil"), host));
        assert!(OriginPolicy::Patterns(vec!["*".into()]).allows(Some("https://x.y"), host));
    }
}
