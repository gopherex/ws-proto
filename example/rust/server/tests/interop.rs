//! Cross-implementation checks for the echo service:
//!
//! - Rust client ↔ Rust server over a real TCP WebSocket (every RPC kind,
//!   metadata, unknown method, flow control under oversized payloads,
//!   deadlines, many concurrent streams);
//! - Rust client ↔ **Go** server (`example/server`, built with `go build`)
//!   for the same echo scenarios. Set `WS_PROTO_SKIP_GO=1` to skip.

use std::{process::Stdio, time::Duration};

use echo_proto::echo::v1::{
    echo_service_ws::EchoServiceClient, BidiRequest, ClientStreamRequest, ServerStreamRequest,
    UnaryRequest,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use ws_proto::{CallOptions, Client, ClientConfig, Code, Request};
use ws_proto_example_server::{ECHO_MD_HEADER, ECHO_MD_TRAILER};

async fn rust_server() -> String {
    rust_server_with(ws_proto_example_server::Impl::Native).await
}

async fn rust_server_with(which: ws_proto_example_server::Impl) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let server = ws_proto_example_server::new_server_with(which);
    tokio::spawn(async move { server.serve_listener(listener).await });
    url
}

struct GoServer {
    child: tokio::process::Child,
    url: String,
}

impl Drop for GoServer {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Builds and starts `example/server`; waits for its `LISTENING` line.
async fn go_server() -> Option<GoServer> {
    if std::env::var_os("WS_PROTO_SKIP_GO").is_some() {
        return None;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let bin = std::env::temp_dir().join(format!("ws-proto-go-server-{}", std::process::id()));
    let status = tokio::process::Command::new("go")
        .args(["build", "-o"])
        .arg(&bin)
        .arg("./example/server")
        .current_dir(&root)
        .status()
        .await
        .expect("go toolchain available (set WS_PROTO_SKIP_GO=1 to skip)");
    assert!(status.success(), "go build failed");

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    let mut child = tokio::process::Command::new(&bin)
        .env("WS_PROTO_TEST_ADDR", &addr)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(20), lines.next_line())
        .await
        .expect("go server ready within 20s")
        .unwrap()
        .unwrap_or_default();
    assert!(line.starts_with("LISTENING"), "unexpected: {line}");
    Some(GoServer {
        child,
        url: format!("ws://{addr}/"),
    })
}

async fn connect(url: &str) -> EchoServiceClient {
    EchoServiceClient::new(Client::dial(url, ClientConfig::default()).await.unwrap())
}

/// The scenario set shared by both servers.
async fn echo_scenarios(url: &str) {
    let client = connect(url).await;

    // unary
    let res = client
        .unary(UnaryRequest { name: "bob".into() })
        .await
        .unwrap();
    assert_eq!(res.msg.greeting, "hello bob");

    // server streaming
    let s = client
        .server_stream(ServerStreamRequest { count: 3 })
        .await
        .unwrap();
    let mut got = vec![];
    while let Some(r) = s.recv().await.unwrap() {
        got.push(r.index);
    }
    assert_eq!(got, [0, 1, 2]);

    // client streaming
    let s = client.client_stream(CallOptions::new()).await.unwrap();
    for value in 1..=4 {
        s.send(&ClientStreamRequest { value }).await.unwrap();
    }
    assert_eq!(s.close_and_recv().await.unwrap().msg.sum, 10);

    // bidi
    let s = client.bidi(CallOptions::new()).await.unwrap();
    s.send(&BidiRequest { text: "a".into() }).await.unwrap();
    assert_eq!(s.recv().await.unwrap().unwrap().echo, "echo:a");
    s.send(&BidiRequest { text: "b".into() }).await.unwrap();
    assert_eq!(s.recv().await.unwrap().unwrap().echo, "echo:b");
    s.close_send().await.unwrap();
    assert_eq!(s.recv().await.unwrap(), None);

    // unknown method -> Unimplemented with the Go-identical message
    let raw = client
        .conn()
        .new_stream("/echo.v1.EchoService/Nope", Default::default(), None)
        .await
        .unwrap();
    raw.close_send().await.unwrap();
    let err = raw.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::Unimplemented);
    // ws-proto's own registry says "unknown method /x"; a tonic service behind
    // register_grpc routes internally and answers Unimplemented with no message.
    assert!(
        err.message.is_empty() || err.message == "unknown method /echo.v1.EchoService/Nope",
        "{err}"
    );

    // flow control: payloads far beyond the 256 KiB window in both directions
    let big = "x".repeat(300 * 1024);
    let s = client.bidi(CallOptions::new()).await.unwrap();
    for _ in 0..4 {
        s.send(&BidiRequest { text: big.clone() }).await.unwrap();
        let r = s.recv().await.unwrap().unwrap();
        assert_eq!(r.echo.len(), big.len() + 5);
    }
    s.close_send().await.unwrap();
    assert_eq!(s.recv().await.unwrap(), None);

    // many small messages: exercises coalesced WINDOW_UPDATEs
    let s = client
        .server_stream(ServerStreamRequest { count: 20_000 })
        .await
        .unwrap();
    let mut n = 0;
    while let Some(r) = s.recv().await.unwrap() {
        assert_eq!(r.index, n);
        n += 1;
    }
    assert_eq!(n, 20_000);

    // deadline: ws-timeout-ms makes the server end a stream the client never feeds.
    // Go reports its context error (Unknown "context deadline exceeded"), Rust
    // DeadlineExceeded; both carry "deadline".
    let s = client
        .bidi(CallOptions::new().with_timeout(Duration::from_millis(100)))
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(5), s.recv())
        .await
        .expect("server ends the stream on deadline")
        .unwrap_err();
    assert!(err.message.contains("deadline"), "{err}");

    // concurrent streams on one connection
    let mut tasks = vec![];
    for i in 0..50 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            c.unary(UnaryRequest {
                name: i.to_string(),
            })
            .await
            .unwrap()
            .msg
            .greeting
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        assert_eq!(t.await.unwrap(), format!("hello {i}"));
    }

    client.conn().close();
    client.conn().closed().await;
}

#[tokio::test]
async fn rust_client_rust_server() {
    let url = rust_server().await;
    echo_scenarios(&url).await;
}

#[tokio::test]
async fn rust_client_rust_server_tonic_bridge() {
    let url = rust_server_with(ws_proto_example_server::Impl::TonicBridge).await;
    echo_scenarios(&url).await;
    // Response metadata arrives as the leading header (tonic puts it in HTTP headers).
    let client = connect(&url).await;
    let res = client
        .unary(Request::new(UnaryRequest { name: "md".into() }).with_header(ECHO_MD_HEADER, "v1"))
        .await
        .unwrap();
    assert_eq!(
        res.header.get(ECHO_MD_HEADER).map(String::as_str),
        Some("v1")
    );
    // Trailers on a successful stream via the Status(Ok)+metadata workaround
    // (grpc/grpc-rust#1370): register_grpc maps it to END(OK) + trailers.
    let s = client
        .server_stream(
            Request::new(ServerStreamRequest { count: 2 }).with_header(ECHO_MD_HEADER, "v2"),
        )
        .await
        .unwrap();
    assert_eq!(s.recv().await.unwrap().map(|r| r.index), Some(0));
    assert_eq!(s.recv().await.unwrap().map(|r| r.index), Some(1));
    assert_eq!(
        s.recv().await.unwrap(),
        None,
        "Status(Ok) ends the stream cleanly"
    );
    assert_eq!(
        s.trailer().get(ECHO_MD_TRAILER).map(String::as_str),
        Some("v2")
    );
}

#[tokio::test]
async fn rust_client_rust_server_metadata() {
    let url = rust_server().await;
    let client = connect(&url).await;
    let res = client
        .unary(Request::new(UnaryRequest { name: "md".into() }).with_header(ECHO_MD_HEADER, "v1"))
        .await
        .unwrap();
    assert_eq!(
        res.header.get(ECHO_MD_HEADER).map(String::as_str),
        Some("v1")
    );
    assert_eq!(
        res.trailer.get(ECHO_MD_TRAILER).map(String::as_str),
        Some("v1")
    );
    // Without the header neither is set.
    let res = client
        .unary(UnaryRequest { name: "x".into() })
        .await
        .unwrap();
    assert!(res.header.is_empty() && res.trailer.is_empty());
}

#[tokio::test]
async fn rust_client_rust_server_deadline() {
    let url = rust_server().await;
    let client = connect(&url).await;
    // A bidi stream the client never feeds: the server-side deadline fires.
    let s = client
        .bidi(CallOptions::new().with_timeout(Duration::from_millis(100)))
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(5), s.recv())
        .await
        .expect("server ends the stream on deadline")
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
}

#[tokio::test]
async fn rust_client_rust_server_rejects_without_subprotocol() {
    let url = rust_server().await;
    // A plain tungstenite client that offers no Sec-WebSocket-Protocol is rejected (400).
    let err = tokio_tungstenite_plain_connect(&url).await;
    assert!(err.contains("400"), "{err}");
}

async fn tokio_tungstenite_plain_connect(url: &str) -> String {
    // Re-use ws-proto's dependency through a tiny raw handshake over TCP so this
    // test does not need its own tungstenite dependency.
    let addr = url.trim_start_matches("ws://").trim_end_matches('/');
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let req = format!(
        "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    tcp.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 1024];
    let n = tcp.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned()
}

#[tokio::test]
async fn rust_client_go_server() {
    let Some(go) = go_server().await else {
        eprintln!("WS_PROTO_SKIP_GO set: skipping Go interop");
        return;
    };
    echo_scenarios(&go.url).await;
}
