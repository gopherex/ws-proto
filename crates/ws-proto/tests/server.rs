//! Server / client semantics over the in-memory pipe and over real TCP.

use std::{sync::Arc, time::Duration};

use ws_proto::{
    conn::pipe, server::OriginPolicy, transport::Frame, CallOptions, Client, ClientConfig, Code,
    MethodSpec, Request, Response, Server, ServerConfig, Status, StreamKind,
};

const fn spec(route: &'static str) -> MethodSpec {
    MethodSpec {
        route,
        kind: StreamKind::Unary,
    }
}
const UNARY: MethodSpec = spec("/t/Unary");

/// Any prost message works as a payload; `Frame` is handy and already here.
fn m(id: u32) -> Frame {
    Frame {
        stream_id: id,
        ..Default::default()
    }
}

fn echo_server() -> Server {
    let s = Server::new(ServerConfig::default());
    s.register_unary("/t/Unary", |req: Request<Frame>| async move {
        let mut r = Response::new(m(req.msg.stream_id + 1));
        if let Some(v) = req.header.get("h") {
            r = r.with_header("h", v.clone()).with_trailer("t", v.clone());
        }
        Ok(r)
    });
    s.register("/t/Sleep", |_s| async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(())
    });
    s.register("/t/Panic", |_s| async move {
        panic!("boom");
    });
    s.register("/t/Fail", |_s| async move {
        Err(Status::new(Code::AlreadyExists, "dup"))
    });
    s
}

fn pair() -> (Server, Client) {
    let server = echo_server();
    let (a, b) = pipe();
    let srv = server.clone();
    tokio::spawn(async move { srv.serve_conn(a).await });
    (server, Client::from_conn(b, ClientConfig::default()))
}

#[tokio::test]
async fn unary_with_metadata() {
    let (_s, c) = pair();
    let r: Response<Frame> = c
        .invoke_unary(UNARY, Request::new(m(1)).with_header("h", "v"))
        .await
        .unwrap();
    assert_eq!(r.msg.stream_id, 2);
    assert_eq!(r.header.get("h").map(String::as_str), Some("v"));
    assert_eq!(r.trailer.get("t").map(String::as_str), Some("v"));
}

#[tokio::test]
async fn unary_half_close_without_message_yields_default_request() {
    let (_s, c) = pair();
    let st = c
        .new_stream("/t/Unary", Default::default(), None)
        .await
        .unwrap();
    st.close_send().await.unwrap();
    let r: Frame = st.recv().await.unwrap().unwrap();
    assert_eq!(r.stream_id, 1, "default request stream_id 0 + 1");
}

#[tokio::test]
async fn unknown_method_is_unimplemented() {
    let (_s, c) = pair();
    let err = c
        .invoke_unary::<Frame, Frame>(spec("/t/Nope"), Request::new(m(0)))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unimplemented);
    assert_eq!(err.message, "unknown method /t/Nope");
}

#[tokio::test]
async fn unknown_handler_fallback() {
    let (s, c) = pair();
    s.set_unknown_handler(|st| async move {
        st.set_trailer([("m".to_owned(), st.method().to_owned())].into());
        Ok(())
    });
    let st = c
        .new_stream("/any/Thing", Default::default(), None)
        .await
        .unwrap();
    assert_eq!(st.recv_raw().await.unwrap(), None);
    assert_eq!(
        st.trailer().get("m").map(String::as_str),
        Some("/any/Thing")
    );
}

#[tokio::test]
async fn handler_error_status_reaches_client() {
    let (_s, c) = pair();
    let err = c
        .invoke_unary::<Frame, Frame>(spec("/t/Fail"), Request::new(m(0)))
        .await
        .unwrap_err();
    assert_eq!(
        (err.code, err.message.as_str()),
        (Code::AlreadyExists, "dup")
    );
}

#[tokio::test]
async fn handler_panic_is_internal() {
    let (_s, c) = pair();
    let st = c
        .new_stream("/t/Panic", Default::default(), None)
        .await
        .unwrap();
    let err = st.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::Internal);
    assert!(err.message.contains("panic"), "{err}");
}

#[tokio::test]
async fn deadline_ends_stream_with_deadline_exceeded() {
    let (_s, c) = pair();
    let st = c
        .new_stream(
            "/t/Sleep",
            Default::default(),
            Some(Duration::from_millis(50)),
        )
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(2), st.recv_raw())
        .await
        .expect("ended by deadline")
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
}

#[tokio::test]
async fn max_streams_default_is_1000() {
    let (_s, c) = pair();
    let mut open = Vec::new();
    for _ in 0..1000 {
        open.push(
            c.new_stream("/t/Sleep", Default::default(), None)
                .await
                .unwrap(),
        );
    }
    // Let the server register them all before the 1001st OPEN arrives.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let extra = c
        .new_stream("/t/Sleep", Default::default(), None)
        .await
        .unwrap();
    let err = extra.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::ResourceExhausted);
    assert_eq!(err.message, "max concurrent streams exceeded");
}

// ---- real TCP + WebSocket handshake ----

async fn listen(server: Server) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", l.local_addr().unwrap());
    tokio::spawn(async move { server.serve_listener(l).await });
    url
}

#[tokio::test]
async fn tcp_roundtrip_with_subprotocol() {
    let s = Server::new(ServerConfig {
        origin: OriginPolicy::InsecureSkip,
        ..Default::default()
    });
    s.register_unary("/t/Unary", |req: Request<Frame>| async move {
        Ok(Response::new(m(req.msg.stream_id * 2)))
    });
    let url = listen(s).await;
    let c = Client::dial(&url, ClientConfig::default()).await.unwrap();
    let r: Response<Frame> = c.invoke_unary(UNARY, Request::new(m(21))).await.unwrap();
    assert_eq!(r.msg.stream_id, 42);
    let _ = CallOptions::new(); // API smoke
}

#[tokio::test]
async fn tcp_origin_unconfigured_rejects_upgrade() {
    let url = listen(Server::new(ServerConfig::default())).await;
    let err = Client::dial(&url, ClientConfig::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable);
    assert!(err.message.contains("403"), "{err}");
}

#[tokio::test]
async fn tcp_origin_patterns_enforced() {
    let url = listen(Server::new(ServerConfig {
        origin: OriginPolicy::Patterns(vec!["*.ok.test".into()]),
        ..Default::default()
    }))
    .await;

    let with_origin = |o: &str| ClientConfig {
        headers: [("origin".to_owned(), o.to_owned())].into(),
        ..Default::default()
    };
    assert!(Client::dial(&url, with_origin("https://app.ok.test"))
        .await
        .is_ok());
    assert!(
        Client::dial(&url, ClientConfig::default()).await.is_ok(),
        "no Origin = non-browser"
    );
    let err = Client::dial(&url, with_origin("https://evil.test"))
        .await
        .unwrap_err();
    assert!(err.message.contains("403"), "{err}");
}

#[tokio::test]
async fn deadline_aborts_the_running_handler() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let ticks = Arc::new(AtomicUsize::new(0));
    let t2 = Arc::clone(&ticks);
    let server = Server::new(ServerConfig::default());
    server.register("/t/Tick", move |_s| {
        let t2 = Arc::clone(&t2);
        async move {
            loop {
                t2.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });
    let (a, b) = pipe();
    tokio::spawn(async move { server.serve_conn(a).await });
    let c = Client::from_conn(b, ClientConfig::default());
    let st = c
        .new_stream(
            "/t/Tick",
            Default::default(),
            Some(Duration::from_millis(50)),
        )
        .await
        .unwrap();
    assert_eq!(
        st.recv_raw().await.unwrap_err().code,
        Code::DeadlineExceeded
    );
    let after_deadline = ticks.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        ticks.load(Ordering::Relaxed),
        after_deadline,
        "handler task must be aborted, not left running"
    );
}
