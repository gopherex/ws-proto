//! Keepalive, reconnect, interceptors, middleware and stats.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::StreamExt;
use ws_proto::{
    conn::{pipe, Message as WsMessage},
    interceptor::{
        streaming_fn, unary_fn, AnyRequest, StreamInterceptorFn, StreamingClientConn,
        StreamingConn, StreamingFunc, UnaryFunc, UnaryInterceptorFn,
    },
    server::OriginPolicy,
    transport::Frame,
    Client, ClientConfig, Code, Keepalive, Metadata, MethodSpec, Mux, MuxConfig, Reconnect,
    Request, Response, Server, ServerConfig, ServerStats, Status, StreamKind,
};

fn m(id: u32) -> Frame {
    Frame {
        stream_id: id,
        ..Default::default()
    }
}

const UNARY: MethodSpec = MethodSpec {
    route: "/t/Unary",
    kind: StreamKind::Unary,
};
const BIDI: MethodSpec = MethodSpec {
    route: "/t/Bidi",
    kind: StreamKind::BidiStream,
};

fn echo_server(origin: OriginPolicy) -> Server {
    let s = Server::new(ServerConfig {
        origin,
        keepalive: Keepalive::DISABLED,
        ..Default::default()
    });
    s.register_unary("/t/Unary", |req: Request<Frame>| async move {
        let mut r = Response::new(m(req.msg.stream_id + 1));
        if let Some(v) = req.header.get("authorization") {
            r = r.with_trailer("saw-auth", v.clone());
        }
        Ok(r)
    });
    s.register("/t/Bidi", |st| async move {
        while let Some(b) = st.recv_raw().await? {
            st.send_raw(b).await?;
        }
        Ok(())
    });
    s
}

// ------------------------------------------------------------------ keepalive

#[tokio::test]
async fn keepalive_fails_mux_when_peer_never_pongs() {
    let (a, mut raw) = pipe();
    let mux = Mux::new(
        a,
        MuxConfig {
            keepalive: Some(Keepalive {
                interval: Duration::from_millis(30),
                timeout: Duration::from_millis(50),
            }),
            ..Default::default()
        },
        None,
    );
    // The raw end sees the ping but never answers.
    let ping = tokio::time::timeout(Duration::from_secs(1), raw.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(ping, WsMessage::Ping(_)), "{ping:?}");
    tokio::time::timeout(Duration::from_secs(1), mux.closed())
        .await
        .expect("mux fails after pong timeout");
    assert!(mux.is_closed());
}

#[tokio::test]
async fn keepalive_survives_with_ponging_peer() {
    let (a, b) = pipe();
    let k = Keepalive {
        interval: Duration::from_millis(20),
        timeout: Duration::from_millis(50),
    };
    let client = Mux::new(
        a,
        MuxConfig {
            keepalive: Some(k),
            ..Default::default()
        },
        None,
    );
    // A mux peer answers pings on its reader task.
    let server = Mux::new(b, MuxConfig::default(), Some(Box::new(|_| {})));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!client.is_closed(), "pongs keep the connection alive");
    assert!(!server.is_closed());
}

// ------------------------------------------------------------------ reconnect

/// A TCP server that can be stopped and restarted on the same port.
struct Restartable {
    addr: std::net::SocketAddr,
    server: Server,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Restartable {
    async fn start(server: Server) -> Self {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let mut r = Self {
            addr,
            server,
            stop: None,
        };
        r.spawn(l);
        r
    }

    fn spawn(&mut self, l: tokio::net::TcpListener) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.stop = Some(tx);
        let srv = self.server.clone();
        tokio::spawn(async move {
            // Each connection is served on a child task that dies with us.
            let conns = tokio::task::JoinSet::<()>::new();
            let conns = Arc::new(Mutex::new(conns));
            let accept = {
                let conns = Arc::clone(&conns);
                async move {
                    loop {
                        let (tcp, _) = l.accept().await.unwrap();
                        let srv = srv.clone();
                        conns.lock().unwrap().spawn(async move {
                            let _ = srv.accept(tcp).await;
                        });
                    }
                }
            };
            tokio::select! {
                _ = accept => {}
                _ = rx => {}
            }
            conns.lock().unwrap().abort_all();
        });
    }

    fn stop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
    }

    async fn restart(&mut self) {
        let l = loop {
            match tokio::net::TcpListener::bind(self.addr).await {
                Ok(l) => break l,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        self.spawn(l);
    }

    fn url(&self) -> String {
        format!("ws://{}/", self.addr)
    }
}

#[tokio::test]
async fn reconnect_resumes_service_after_server_restart() {
    let mut rs = Restartable::start(echo_server(OriginPolicy::InsecureSkip)).await;
    let client = Client::dial(
        &rs.url(),
        ClientConfig {
            keepalive: Some(Keepalive::DISABLED),
            reconnect: Some(Reconnect {
                initial: Duration::from_millis(20),
                max: Duration::from_millis(100),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let r: Response<Frame> = client
        .invoke_unary(UNARY, Request::new(m(1)))
        .await
        .unwrap();
    assert_eq!(r.msg.stream_id, 2);

    // An in-flight stream fails Unavailable when the socket drops ...
    let inflight = client
        .new_stream("/t/Bidi", Metadata::new(), None)
        .await
        .unwrap();
    rs.stop();
    let err = tokio::time::timeout(Duration::from_secs(5), inflight.recv_raw())
        .await
        .expect("in-flight stream fails on disconnect")
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable);

    // ... and new calls wait for the fresh socket.
    rs.restart().await;
    let r: Response<Frame> = tokio::time::timeout(
        Duration::from_secs(5),
        client.invoke_unary(
            UNARY,
            Request::new(m(2)).with_timeout(Duration::from_secs(5)),
        ),
    )
    .await
    .expect("reconnect resumes service")
    .unwrap();
    assert_eq!(r.msg.stream_id, 3);
    assert!(client.is_connected());
}

#[tokio::test]
async fn close_stops_reconnect_loop() {
    let mut rs = Restartable::start(echo_server(OriginPolicy::InsecureSkip)).await;
    let client = Client::dial(
        &rs.url(),
        ClientConfig {
            keepalive: Some(Keepalive::DISABLED),
            reconnect: Some(Reconnect {
                initial: Duration::from_millis(50),
                max: Duration::from_millis(200),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    rs.stop();
    tokio::time::sleep(Duration::from_millis(100)).await; // controller is retrying
    client.close();
    tokio::time::timeout(Duration::from_secs(2), client.closed_for_good())
        .await
        .expect("close returns promptly");
    let err = client
        .new_stream("/t/Unary", Metadata::new(), None)
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable);
    assert_eq!(err.message, "wsrpc: connection closed");
}

#[tokio::test]
async fn new_stream_waits_bounded_by_timeout_during_gap() {
    let mut rs = Restartable::start(echo_server(OriginPolicy::InsecureSkip)).await;
    let client = Client::dial(
        &rs.url(),
        ClientConfig {
            keepalive: Some(Keepalive::DISABLED),
            reconnect: Some(Reconnect::default()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    rs.stop();
    client.closed().await; // socket gone, controller redialing a dead port
    let err = client
        .new_stream(
            "/t/Unary",
            Metadata::new(),
            Some(Duration::from_millis(100)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    client.close();
}

// --------------------------------------------------------------- interceptors

fn pair(cfg: ClientConfig) -> (Server, Client) {
    let server = echo_server(OriginPolicy::Unconfigured);
    let (a, b) = pipe();
    let srv = server.clone();
    tokio::spawn(async move { srv.serve_conn(a).await });
    (server, Client::from_conn(b, cfg))
}

#[tokio::test]
async fn unary_interceptors_run_in_order_and_inject_headers() {
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let mk = |name: &'static str, log: Arc<Mutex<Vec<&'static str>>>| {
        UnaryInterceptorFn(move |next: UnaryFunc| {
            let log = Arc::clone(&log);
            unary_fn(move |mut req: AnyRequest| {
                let next = Arc::clone(&next);
                let log = Arc::clone(&log);
                async move {
                    log.lock().unwrap().push(name);
                    if name == "auth" {
                        req.header
                            .insert("authorization".into(), "Bearer xyz".into());
                    }
                    let resp = next(req).await;
                    log.lock().unwrap().push(name);
                    resp
                }
            })
        })
    };
    let cfg = ClientConfig::default()
        .with_interceptor(mk("outer", Arc::clone(&log)))
        .with_interceptor(mk("auth", Arc::clone(&log)));
    let (_s, c) = pair(cfg);
    let r: Response<Frame> = c.invoke_unary(UNARY, Request::new(m(1))).await.unwrap();
    assert_eq!(r.msg.stream_id, 2);
    assert_eq!(
        r.trailer.get("saw-auth").map(String::as_str),
        Some("Bearer xyz"),
        "server saw the injected header"
    );
    assert_eq!(*log.lock().unwrap(), ["outer", "auth", "auth", "outer"]);
}

#[tokio::test]
async fn unary_interceptor_can_short_circuit() {
    let cfg = ClientConfig::default().with_interceptor(UnaryInterceptorFn(|_next: UnaryFunc| {
        unary_fn(|req: AnyRequest| async move {
            Err(Status::new(
                Code::PermissionDenied,
                format!("blocked {}", req.spec.route),
            ))
        })
    }));
    let (_s, c) = pair(cfg);
    let err = c
        .invoke_unary::<Frame, Frame>(UNARY, Request::new(m(1)))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::PermissionDenied);
    assert_eq!(err.message, "blocked /t/Unary");
}

/// Wraps a streaming conn, counting received messages.
struct Counting {
    inner: StreamingConn,
    received: Arc<AtomicUsize>,
}

impl StreamingClientConn for Counting {
    fn spec(&self) -> MethodSpec {
        self.inner.spec()
    }
    fn request_header(&self) -> Metadata {
        self.inner.request_header()
    }
    fn send(&self, msg: Bytes) -> ws_proto::interceptor::BoxFuture<'_, Result<(), Status>> {
        self.inner.send(msg)
    }
    fn close_request(&self) -> ws_proto::interceptor::BoxFuture<'_, Result<(), Status>> {
        self.inner.close_request()
    }
    fn receive(&self) -> ws_proto::interceptor::BoxFuture<'_, Result<Option<Bytes>, Status>> {
        Box::pin(async move {
            let r = self.inner.receive().await;
            if matches!(r, Ok(Some(_))) {
                self.received.fetch_add(1, Ordering::Relaxed);
            }
            r
        })
    }
    fn response_header(&self) -> Metadata {
        self.inner.response_header()
    }
    fn response_trailer(&self) -> Metadata {
        self.inner.response_trailer()
    }
}

#[tokio::test]
async fn streaming_interceptor_wraps_conn_and_sees_spec() {
    let received = Arc::new(AtomicUsize::new(0));
    let seen_spec = Arc::new(Mutex::new(None));
    let (rc, ss) = (Arc::clone(&received), Arc::clone(&seen_spec));
    let cfg = ClientConfig::default().with_interceptor(StreamInterceptorFn(
        move |next: StreamingFunc| {
            let (rc, ss) = (Arc::clone(&rc), Arc::clone(&ss));
            streaming_fn(move |spec, mut header: Metadata, timeout| {
                let next = Arc::clone(&next);
                let (rc, ss) = (Arc::clone(&rc), Arc::clone(&ss));
                async move {
                    *ss.lock().unwrap() = Some(spec);
                    header.insert("authorization".into(), "Bearer s".into());
                    let inner = next(spec, header, timeout).await?;
                    Ok(Arc::new(Counting {
                        inner,
                        received: rc,
                    }) as StreamingConn)
                }
            })
        },
    ));
    let (_s, c) = pair(cfg);
    let conn = c.open_streaming(BIDI, Metadata::new(), None).await.unwrap();
    for i in 0..3u8 {
        conn.send(Bytes::from(vec![i])).await.unwrap();
        assert_eq!(conn.receive().await.unwrap(), Some(Bytes::from(vec![i])));
    }
    conn.close_request().await.unwrap();
    assert_eq!(conn.receive().await.unwrap(), None);
    assert_eq!(received.load(Ordering::Relaxed), 3);
    assert_eq!(*seen_spec.lock().unwrap(), Some(BIDI));
    assert_eq!(
        conn.request_header()
            .get("authorization")
            .map(String::as_str),
        Some("Bearer s")
    );
}

// ----------------------------------------------------------------- middleware

#[tokio::test]
async fn middleware_chain_order_and_method_visibility() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, c) = pair(ClientConfig::default());
    for name in ["first", "second"] {
        let log = Arc::clone(&log);
        server.add_middleware(move |next| {
            let log = Arc::clone(&log);
            Arc::new(move |s| {
                let next = Arc::clone(&next);
                let log = Arc::clone(&log);
                Box::pin(async move {
                    log.lock()
                        .unwrap()
                        .push(format!("{name}:{}:before", s.method()));
                    let r = next(s).await;
                    log.lock().unwrap().push(format!("{name}:after"));
                    r
                })
            })
        });
    }
    let _: Response<Frame> = c.invoke_unary(UNARY, Request::new(m(1))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        *log.lock().unwrap(),
        [
            "first:/t/Unary:before",
            "second:/t/Unary:before",
            "second:after",
            "first:after"
        ]
    );
}

#[tokio::test]
async fn middleware_short_circuits_and_sees_unknown_methods() {
    let (server, c) = pair(ClientConfig::default());
    server.set_unknown_handler(|_s| async { Ok(()) });
    server.add_middleware(|_next| {
        Arc::new(|s| {
            Box::pin(async move {
                Err(Status::new(
                    Code::Unauthenticated,
                    format!("no auth for {}", s.method()),
                ))
            })
        })
    });
    let err = c
        .invoke_unary::<Frame, Frame>(UNARY, Request::new(m(1)))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unauthenticated);
    assert_eq!(err.message, "no auth for /t/Unary");
    // Middleware wraps the unknown-method fallback too.
    let st = c.new_stream("/x/Y", Metadata::new(), None).await.unwrap();
    let err = st.recv_raw().await.unwrap_err();
    assert_eq!(err.message, "no auth for /x/Y");
}

// ---------------------------------------------------------------------- stats

#[derive(Default)]
struct Rec {
    events: Mutex<Vec<String>>,
}

impl ServerStats for Rec {
    fn conn_opened(&self) {
        self.events.lock().unwrap().push("conn_opened".into());
    }
    fn conn_closed(&self) {
        self.events.lock().unwrap().push("conn_closed".into());
    }
    fn conn_rejected(&self, reason: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("conn_rejected:{reason}"));
    }
    fn stream_started(&self, method: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("stream_started:{method}"));
    }
    fn stream_ended(&self, method: &str, code: Code) {
        self.events
            .lock()
            .unwrap()
            .push(format!("stream_ended:{method}:{code:?}"));
    }
    fn stream_rejected(&self, reason: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("stream_rejected:{reason}"));
    }
    fn msg_sent(&self, method: &str, bytes: usize) {
        self.events
            .lock()
            .unwrap()
            .push(format!("msg_sent:{method}:{bytes}"));
    }
    fn msg_received(&self, method: &str, bytes: usize) {
        self.events
            .lock()
            .unwrap()
            .push(format!("msg_received:{method}:{bytes}"));
    }
}

#[tokio::test]
async fn stats_observe_unary_flow_and_rejections() {
    let rec = Arc::new(Rec::default());
    let server = echo_server(OriginPolicy::Unconfigured);
    server.set_stats(rec.clone());
    let (a, b) = pipe();
    let srv = server.clone();
    tokio::spawn(async move { srv.serve_conn(a).await });
    let c = Client::from_conn(b, ClientConfig::default());

    let _: Response<Frame> = c.invoke_unary(UNARY, Request::new(m(1))).await.unwrap();
    let _ = c
        .invoke_unary::<Frame, Frame>(
            MethodSpec {
                route: "/t/Nope",
                kind: StreamKind::Unary,
            },
            Request::new(m(1)),
        )
        .await;
    c.close();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let ev = rec.events.lock().unwrap().clone();
    let has = |p: &str| ev.iter().any(|e| e.starts_with(p));
    assert_eq!(ev.first().map(String::as_str), Some("conn_opened"));
    assert!(has("stream_started:/t/Unary"), "{ev:?}");
    assert!(has("msg_received:/t/Unary:2"), "{ev:?}");
    assert!(has("msg_sent:/t/Unary:2"), "{ev:?}");
    assert!(has("stream_ended:/t/Unary:Ok"), "{ev:?}");
    assert!(
        !has("stream_started:/t/Nope"),
        "unknown methods are not started: {ev:?}"
    );
    assert_eq!(ev.last().map(String::as_str), Some("conn_closed"));
}

#[tokio::test]
async fn stats_conn_rejected_on_origin_policy() {
    let rec = Arc::new(Rec::default());
    let server = echo_server(OriginPolicy::Unconfigured);
    server.set_stats(rec.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", l.local_addr().unwrap());
    tokio::spawn(async move { server.serve_listener(l).await });
    assert!(Client::dial(&url, ClientConfig::default()).await.is_err());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(*rec.events.lock().unwrap(), ["conn_rejected:origin_policy"]);
}
