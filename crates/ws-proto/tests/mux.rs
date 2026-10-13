//! End-to-end mux/stream behaviour over an in-memory pipe, plus wire-level
//! checks driving raw frames at a server mux.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use tokio::sync::{mpsc, Mutex};
use ws_proto::{
    conn::{pipe, Message as WsMessage, Pipe},
    transport::{Frame, Kind},
    Code, Metadata, Mux, MuxConfig, Status, Stream,
};

/// Server mux whose streams are handed to `handler` on their own task.
fn serve<F, Fut>(conn: Pipe, cfg: MuxConfig, handler: F) -> Mux
where
    F: Fn(Stream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), Status>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Mux::new(
        conn,
        cfg,
        Some(Box::new(move |s: Stream| {
            let h = Arc::clone(&handler);
            tokio::spawn(async move {
                let st = match h(s.clone()).await {
                    Ok(()) => Status::ok(),
                    Err(st) => st,
                };
                let _ = s.end(st).await;
            });
        })),
    )
}

fn msg(s: &str) -> Bytes {
    Bytes::from(s.to_owned())
}

async fn echo_server(s: Stream) -> Result<(), Status> {
    while let Some(b) = s.recv_raw().await? {
        s.send_raw(b).await?;
    }
    Ok(())
}

#[tokio::test]
async fn unary_roundtrip_with_metadata() {
    let (srv, cli) = pipe();
    let _server = serve(srv, MuxConfig::default(), |s| async move {
        assert_eq!(s.method(), "/echo.v1.EchoService/Unary");
        assert_eq!(s.header().get("k").map(String::as_str), Some("v"));
        s.send_header([("lead".to_owned(), "1".to_owned())].into())
            .await?;
        let req = s.recv_raw().await?.expect("request");
        assert_eq!(s.recv_raw().await?, None, "half-close after one msg");
        s.set_trailer([("trail".to_owned(), "2".to_owned())].into());
        s.send_raw(req).await
    });
    let client = Mux::new(cli, MuxConfig::default(), None);

    let st = client
        .open_stream(
            "/echo.v1.EchoService/Unary",
            Metadata::from([("k".to_owned(), "v".to_owned())]),
            None,
        )
        .await
        .unwrap();
    st.send_raw(msg("hi")).await.unwrap();
    st.close_send().await.unwrap();
    assert_eq!(st.recv_raw().await.unwrap(), Some(msg("hi")));
    assert_eq!(st.recv_raw().await.unwrap(), None);
    assert_eq!(st.header().get("lead").map(String::as_str), Some("1"));
    assert_eq!(st.trailer().get("trail").map(String::as_str), Some("2"));
    assert_eq!(
        client.stream_count(),
        0,
        "terminal END unregisters the stream"
    );
}

#[tokio::test]
async fn non_ok_end_surfaces_as_status() {
    let (srv, cli) = pipe();
    let _server = serve(srv, MuxConfig::default(), |_s| async move {
        Err(Status::new(Code::NotFound, "nope"))
    });
    let client = Mux::new(cli, MuxConfig::default(), None);
    let st = client
        .open_stream("/m", Metadata::new(), None)
        .await
        .unwrap();
    let err = st.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::NotFound);
    assert_eq!(err.message, "nope");
    // A send after the terminal frame fails with that same status.
    assert_eq!(
        st.send_raw(msg("x")).await.unwrap_err().code,
        Code::NotFound
    );
}

#[tokio::test]
async fn typed_send_recv() {
    let (srv, cli) = pipe();
    let _server = serve(srv, MuxConfig::default(), echo_server);
    let client = Mux::new(cli, MuxConfig::default(), None);
    let st = client
        .open_stream("/m", Metadata::new(), None)
        .await
        .unwrap();
    // Reuse the Frame message type as an arbitrary prost message.
    let m = Frame {
        stream_id: 42,
        method: "x".into(),
        ..Default::default()
    };
    st.send(&m).await.unwrap();
    assert_eq!(st.recv::<Frame>().await.unwrap(), Some(m));
}

#[tokio::test]
async fn deadline_header_propagates() {
    let (srv, cli) = pipe();
    let (tx, mut rx) = mpsc::channel(1);
    let _server = serve(srv, MuxConfig::default(), move |s| {
        let tx = tx.clone();
        async move {
            tx.send((s.timeout(), s.header().contains_key("ws-timeout-ms")))
                .await
                .unwrap();
            Ok(())
        }
    });
    let client = Mux::new(cli, MuxConfig::default(), None);
    let st = client
        .open_stream("/m", Metadata::new(), Some(Duration::from_millis(1500)))
        .await
        .unwrap();
    assert_eq!(st.timeout(), Some(Duration::from_millis(1500)));
    let (seen, in_header) = rx.recv().await.unwrap();
    assert_eq!(seen, Some(Duration::from_millis(1500)));
    assert!(in_header, "ws-timeout-ms rides in OPEN headers");
}

#[tokio::test]
async fn send_blocks_until_credit_returned() {
    let window = MuxConfig {
        initial_window: 16,
        ..Default::default()
    };
    let (srv, cli) = pipe();
    // Server consumes only when told to.
    let (go_tx, go_rx) = mpsc::channel::<()>(8);
    let go_rx = Arc::new(Mutex::new(go_rx));
    let _server = serve(srv, window.clone(), move |s| {
        let go_rx = Arc::clone(&go_rx);
        async move {
            loop {
                go_rx.lock().await.recv().await;
                if s.recv_raw().await?.is_none() {
                    return Ok(());
                }
            }
        }
    });
    let client = Mux::new(cli, window, None);
    let st = client
        .open_stream("/m", Metadata::new(), None)
        .await
        .unwrap();
    let ten = Bytes::from(vec![0u8; 10]);

    st.send_raw(ten.clone()).await.unwrap(); // window 16 -> 6
    st.send_raw(ten.clone()).await.unwrap(); // window 6 > 0 -> sends, now -4
    let third = tokio::time::timeout(Duration::from_millis(100), st.send_raw(ten.clone()));
    assert!(third.await.is_err(), "third send must block without credit");

    // Consuming one 10-byte message crosses the 8-byte threshold -> WINDOW_UPDATE(10).
    go_tx.send(()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), st.send_raw(ten))
        .await
        .expect("send resumes after credit")
        .unwrap();
}

#[tokio::test]
async fn connection_loss_fails_streams_unavailable() {
    let (srv, cli) = pipe();
    let server = serve(srv, MuxConfig::default(), |s| async move {
        s.ended().await;
        Ok(())
    });
    let client = Mux::new(cli, MuxConfig::default(), None);
    let st = client
        .open_stream("/m", Metadata::new(), None)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    drop(server); // server mux closes its conn -> client reader sees EOF
    let err = st.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::Unavailable);
    assert_eq!(err.message, "wsrpc: connection lost");
    client.closed().await;
}

#[tokio::test]
async fn local_close_fails_streams_cancelled() {
    let (srv, cli) = pipe();
    let _server = serve(srv, MuxConfig::default(), |s| async move {
        s.ended().await;
        Ok(())
    });
    let client = Mux::new(cli, MuxConfig::default(), None);
    let st = client
        .open_stream("/m", Metadata::new(), None)
        .await
        .unwrap();
    client.close();
    let err = st.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::Cancelled);
    assert_eq!(err.message, "wsrpc: transport closed");
}

// ---- wire level: drive raw frames at a server mux ----

fn frame(stream_id: u32, kind: Kind) -> Frame {
    Frame {
        stream_id,
        kind: kind as i32,
        ..Default::default()
    }
}

async fn raw_send(p: &mut Pipe, f: Frame) {
    p.send(WsMessage::Binary(Bytes::from(f.encode_to_vec())))
        .await
        .unwrap();
}

async fn raw_recv(p: &mut Pipe) -> Frame {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(2), p.next())
            .await
            .expect("frame within 2s")
            .expect("conn open")
            .unwrap();
        if let WsMessage::Binary(b) = m {
            return Frame::decode(b).unwrap();
        }
    }
}

#[tokio::test]
async fn peer_ignoring_window_gets_rst_resource_exhausted() {
    let (srv, mut raw) = pipe();
    let cfg = MuxConfig {
        initial_window: 16,
        ..Default::default()
    };
    // Handler never consumes.
    let _server = serve(srv, cfg, |s| async move {
        s.ended().await;
        Ok(())
    });
    let mut open = frame(1, Kind::Open);
    open.method = "/m".into();
    raw_send(&mut raw, open).await;
    // 16-byte window: first MSG (20 B) admitted (queue was empty), second
    // arrives with recv_bytes 20 > 16 and a non-empty queue -> RST.
    for _ in 0..2 {
        let mut m = frame(1, Kind::Msg);
        m.payload = Bytes::from(vec![0u8; 20]);
        raw_send(&mut raw, m).await;
    }
    let rst = raw_recv(&mut raw).await;
    assert_eq!(rst.kind, Kind::Rst as i32);
    assert_eq!(rst.stream_id, 1);
    let st = rst.status.unwrap();
    assert_eq!(st.code, Code::ResourceExhausted as i32);
    assert_eq!(st.message, "receive buffer overflow");
}

#[tokio::test]
async fn max_streams_rejects_with_rst() {
    let (srv, mut raw) = pipe();
    let cfg = MuxConfig {
        max_streams: 1,
        ..Default::default()
    };
    let _server = serve(srv, cfg, |s| async move {
        s.ended().await;
        Ok(())
    });
    for id in [1u32, 3] {
        let mut open = frame(id, Kind::Open);
        open.method = "/m".into();
        raw_send(&mut raw, open).await;
    }
    let rst = raw_recv(&mut raw).await;
    assert_eq!(rst.kind, Kind::Rst as i32);
    assert_eq!(rst.stream_id, 3);
    assert_eq!(
        rst.status.unwrap().message,
        "max concurrent streams exceeded"
    );
}

#[tokio::test]
async fn server_end_frame_carries_status_and_trailers() {
    let (srv, mut raw) = pipe();
    let _server = serve(srv, MuxConfig::default(), |s| async move {
        s.set_trailer([("t".to_owned(), "1".to_owned())].into());
        Err(Status::new(Code::PermissionDenied, "no"))
    });
    let mut open = frame(1, Kind::Open);
    open.method = "/m".into();
    raw_send(&mut raw, open).await;
    let end = raw_recv(&mut raw).await;
    assert_eq!(end.kind, Kind::End as i32);
    assert_eq!(end.headers.get("t").map(String::as_str), Some("1"));
    let st = end.status.unwrap();
    assert_eq!(st.code, Code::PermissionDenied as i32);
    assert_eq!(st.message, "no");
}

#[tokio::test]
async fn unknown_stream_frames_are_ignored() {
    let (srv, mut raw) = pipe();
    let _server = serve(srv, MuxConfig::default(), echo_server);
    raw_send(&mut raw, frame(99, Kind::Msg)).await;
    raw_send(&mut raw, frame(99, Kind::End)).await;
    // Still alive: a real stream works afterwards.
    let mut open = frame(1, Kind::Open);
    open.method = "/m".into();
    raw_send(&mut raw, open).await;
    let mut m = frame(1, Kind::Msg);
    m.payload = msg("x");
    raw_send(&mut raw, m).await;
    let back = raw_recv(&mut raw).await;
    assert_eq!(back.kind, Kind::Msg as i32);
    assert_eq!(back.payload, msg("x"));
}

#[tokio::test]
async fn duplicate_open_is_rejected_not_replaced() {
    let (srv, mut raw) = pipe();
    let opened = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let o = Arc::clone(&opened);
    let _server = serve(srv, MuxConfig::default(), move |s| {
        let o = Arc::clone(&o);
        async move {
            o.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            s.ended().await;
            Ok(())
        }
    });
    for _ in 0..3 {
        let mut open = frame(1, Kind::Open);
        open.method = "/m".into();
        raw_send(&mut raw, open).await;
    }
    let rst = raw_recv(&mut raw).await;
    assert_eq!(rst.kind, Kind::Rst as i32);
    assert_eq!(rst.status.unwrap().message, "duplicate stream id");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        opened.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "only the first OPEN starts a handler"
    );
}
