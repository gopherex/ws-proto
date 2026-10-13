//! Frame multiplexer: one [`WsConn`] carrying many [`Stream`]s.
//!
//! Mirrors the Go `wsrpc.Mux`. Two tasks per connection:
//!
//! - the **reader** decodes frames and routes them to streams. It never
//!   performs a blocking write: control messages it must emit (RST on
//!   overflow or over the stream cap, pong) go through a bounded channel
//!   with `try_send`.
//! - the **writer** owns the sink and drains two channels, control first.
//!
//! Stream `send` awaits the data channel, so socket backpressure propagates to
//! callers exactly as Go's `writeMu` + blocking write does. An optional
//! keepalive task pings the peer and fails the mux when no pong arrives.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    conn::{Message, WsConn},
    stats::{reason, Stats},
    status::{Code, Status},
    stream::{Metadata, Stream, StreamInner},
    transport::{Frame, Kind},
    DEFAULT_INITIAL_WINDOW,
};

/// Header carrying the caller's remaining deadline (decimal milliseconds) on
/// the OPEN frame.
pub const TIMEOUT_HEADER: &str = "ws-timeout-ms";

const CTRL_BUFFER: usize = 256;
const DATA_BUFFER: usize = 64;

/// Keepalive pinging. `interval` of zero disables it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keepalive {
    /// Time between pings (Go default 20s).
    pub interval: Duration,
    /// Time to wait for the pong before failing the connection (Go default 10s).
    pub timeout: Duration,
}

impl Keepalive {
    pub const DEFAULT: Keepalive = Keepalive {
        interval: Duration::from_secs(20),
        timeout: Duration::from_secs(10),
    };
    pub const DISABLED: Keepalive = Keepalive {
        interval: Duration::ZERO,
        timeout: Duration::ZERO,
    };

    pub fn enabled(&self) -> bool {
        !self.interval.is_zero()
    }
}

impl Default for Keepalive {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Mux tuning. Zero values keep the defaults.
#[derive(Clone, Default)]
pub struct MuxConfig {
    /// Per-stream credit window in bytes (default 256 KiB). Both peers must
    /// agree; there is no handshake.
    pub initial_window: u32,
    /// Cap on concurrent server streams; 0 = unlimited (always 0 on clients).
    pub max_streams: usize,
    /// Ping the peer; `None` (or a zero interval) disables.
    pub keepalive: Option<Keepalive>,
    /// Server-side observability hooks.
    pub stats: Stats,
}

impl std::fmt::Debug for MuxConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MuxConfig")
            .field("initial_window", &self.initial_window)
            .field("max_streams", &self.max_streams)
            .field("keepalive", &self.keepalive)
            .field("stats", &self.stats.is_some())
            .finish()
    }
}

/// Server-side hook invoked on the reader task for every accepted OPEN.
pub type OnOpen = Box<dyn Fn(Stream) + Send + Sync + 'static>;

/// Handle to a running multiplexer. Dropping it closes the connection.
pub struct Mux {
    shared: Arc<MuxShared>,
}

pub(crate) struct MuxShared {
    streams: Mutex<HashMap<u32, Arc<StreamInner>>>,
    next_id: AtomicU32,
    data_tx: mpsc::Sender<Message>,
    ctrl_tx: mpsc::Sender<Message>,
    on_open: Option<OnOpen>,
    pub(crate) initial_window: u32,
    max_streams: usize,
    pub(crate) stats: Stats,
    /// Set by a deliberate [`Mux::close`]; decides Cancelled vs Unavailable
    /// for in-flight streams when the reader exits.
    closed: AtomicBool,
    /// Cancelled once either task exits; streams fail, no more I/O.
    done: CancellationToken,
    /// Cancelled by [`Mux::close`] / drop; all tasks exit.
    shutdown: CancellationToken,
    pongs: AtomicU64,
    pong_notify: Notify,
}

impl Mux {
    /// Starts the reader, writer (and keepalive) tasks on the current tokio
    /// runtime. `on_open` is `Some` on servers (dispatch) and `None` on clients.
    pub fn new<C: WsConn + 'static>(conn: C, cfg: MuxConfig, on_open: Option<OnOpen>) -> Self {
        let initial_window = if cfg.initial_window == 0 {
            DEFAULT_INITIAL_WINDOW
        } else {
            cfg.initial_window
        };
        let (data_tx, data_rx) = mpsc::channel(DATA_BUFFER);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(CTRL_BUFFER);
        let shared = Arc::new(MuxShared {
            streams: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
            data_tx,
            ctrl_tx,
            on_open,
            initial_window,
            max_streams: cfg.max_streams,
            stats: cfg.stats,
            closed: AtomicBool::new(false),
            done: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            pongs: AtomicU64::new(0),
            pong_notify: Notify::new(),
        });

        let (sink, source) = conn.split();
        tokio::spawn(write_loop(Arc::clone(&shared), sink, data_rx, ctrl_rx));
        tokio::spawn(read_loop(Arc::clone(&shared), source));
        if let Some(k) = cfg.keepalive.filter(Keepalive::enabled) {
            tokio::spawn(keepalive_loop(Arc::clone(&shared), k));
        }

        Self { shared }
    }

    /// Opens a client stream: allocates an odd id, registers it and sends
    /// OPEN with `headers` (plus `ws-timeout-ms` when `timeout` is set).
    pub async fn open_stream(
        &self,
        method: &str,
        mut headers: Metadata,
        timeout: Option<Duration>,
    ) -> Result<Stream, Status> {
        if let Some(t) = timeout {
            let ms = t.as_millis();
            if ms > 0 {
                headers.insert(TIMEOUT_HEADER.to_owned(), ms.to_string());
            }
        }
        let id = self.shared.next_id.fetch_add(2, Ordering::Relaxed);
        let inner = StreamInner::new(id, method.to_owned(), Arc::clone(&self.shared), timeout);
        self.shared
            .streams
            .lock()
            .unwrap()
            .insert(id, Arc::clone(&inner));
        let open = Frame {
            stream_id: id,
            kind: Kind::Open as i32,
            method: method.to_owned(),
            headers,
            ..Default::default()
        };
        if let Err(st) = self.shared.write(open).await {
            self.shared.remove(id);
            return Err(st);
        }
        Ok(Stream::from_inner(inner))
    }

    /// Deliberately shuts the connection down. In-flight streams fail with
    /// `Cancelled` ("transport closed") rather than `Unavailable`.
    pub fn close(&self) {
        self.shared.close();
    }

    /// Resolves once the connection is gone (peer closed, I/O error,
    /// keepalive timeout or [`close`](Self::close)).
    pub async fn closed(&self) {
        self.shared.closed().await
    }

    /// True once the connection is gone.
    pub fn is_closed(&self) -> bool {
        self.shared.done.is_cancelled()
    }

    /// Number of currently registered streams.
    pub fn stream_count(&self) -> usize {
        self.shared.streams.lock().unwrap().len()
    }
}

impl Drop for Mux {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl MuxShared {
    /// Queues a frame for the writer, awaiting channel space (backpressure).
    pub(crate) async fn write(&self, f: Frame) -> Result<(), Status> {
        self.write_msg(Message::Binary(Bytes::from(f.encode_to_vec())))
            .await
    }

    async fn write_msg(&self, m: Message) -> Result<(), Status> {
        if self.done.is_cancelled() {
            return Err(self.disconnect_status());
        }
        self.data_tx
            .send(m)
            .await
            .map_err(|_| self.disconnect_status())
    }

    /// Queues a control frame without ever blocking (reader task).
    fn write_ctrl(&self, f: Frame) {
        let _ = self
            .ctrl_tx
            .try_send(Message::Binary(Bytes::from(f.encode_to_vec())));
    }

    pub(crate) fn remove(&self, id: u32) {
        self.streams.lock().unwrap().remove(&id);
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.shutdown.cancel();
    }

    async fn closed(&self) {
        self.done.cancelled().await
    }

    fn disconnect_status(&self) -> Status {
        if self.closed.load(Ordering::Acquire) {
            Status::new(Code::Cancelled, "wsrpc: transport closed")
        } else {
            Status::new(Code::Unavailable, "wsrpc: connection lost")
        }
    }

    /// Called by whichever task exits first; the others follow via `shutdown`.
    fn finish(&self) {
        if !self.done.is_cancelled() {
            self.done.cancel();
            self.fail_all(self.disconnect_status());
            self.shutdown.cancel();
        }
    }

    fn fail_all(&self, st: Status) {
        let drained: Vec<_> = self
            .streams
            .lock()
            .unwrap()
            .drain()
            .map(|(_, s)| s)
            .collect();
        for s in drained {
            s.fail_with(st.clone());
        }
    }

    fn route(self: &Arc<Self>, f: Frame) {
        let kind = Kind::try_from(f.kind).unwrap_or(Kind::Unspecified);
        if kind == Kind::Open {
            self.route_open(f);
            return;
        }
        let Some(s) = self.streams.lock().unwrap().get(&f.stream_id).cloned() else {
            return;
        };
        match kind {
            Kind::HalfClose => s.half_close(),
            Kind::Header => {
                if self.on_open.is_none() {
                    s.set_leading_header(f.headers);
                }
            }
            Kind::WindowUpdate => s.credit_send(f.window),
            Kind::End | Kind::Rst => {
                s.signal_end(f);
                self.remove(s.id);
            }
            Kind::Msg => {
                let n = f.payload.len();
                if s.try_deliver(f.payload) {
                    if let Some(st) = &self.stats {
                        st.msg_received(s.method(), n);
                    }
                } else {
                    if let Some(st) = &self.stats {
                        st.stream_rejected(reason::RECV_OVERFLOW);
                    }
                    s.fail_with(Status::new(
                        Code::ResourceExhausted,
                        "wsrpc: receive buffer overflow",
                    ));
                    self.write_ctrl(rst(
                        s.id,
                        Status::new(Code::ResourceExhausted, "receive buffer overflow"),
                    ));
                    self.remove(s.id);
                }
            }
            Kind::Open | Kind::Unspecified => {}
        }
    }

    fn route_open(self: &Arc<Self>, f: Frame) {
        let Some(on_open) = &self.on_open else {
            return; // clients ignore OPEN
        };
        if self.max_streams > 0 && self.streams.lock().unwrap().len() >= self.max_streams {
            if let Some(st) = &self.stats {
                st.stream_rejected(reason::MAX_STREAMS);
            }
            self.write_ctrl(rst(
                f.stream_id,
                Status::new(Code::ResourceExhausted, "max concurrent streams exceeded"),
            ));
            return;
        }
        let timeout = f
            .headers
            .get(TIMEOUT_HEADER)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis);
        let inner = StreamInner::new(f.stream_id, f.method, Arc::clone(self), timeout);
        inner.set_leading_header(f.headers);
        self.streams
            .lock()
            .unwrap()
            .insert(f.stream_id, Arc::clone(&inner));
        on_open(Stream::from_inner(inner));
    }
}

fn rst(stream_id: u32, st: Status) -> Frame {
    Frame {
        stream_id,
        kind: Kind::Rst as i32,
        status: Some(st.to_proto()),
        ..Default::default()
    }
}

async fn read_loop<S>(shared: Arc<MuxShared>, mut source: S)
where
    S: futures_core::Stream<Item = Result<Message, crate::Error>> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            _ = shared.shutdown.cancelled() => break,
            next = source.next() => match next {
                Some(Ok(Message::Binary(b))) => match Frame::decode(b) {
                    Ok(f) => shared.route(f),
                    Err(_) => break, // undecodable frame: protocol violation
                },
                Some(Ok(Message::Ping(b))) => {
                    let _ = shared.ctrl_tx.try_send(Message::Pong(b));
                }
                Some(Ok(Message::Pong(_))) => {
                    shared.pongs.fetch_add(1, Ordering::Release);
                    shared.pong_notify.notify_waiters();
                }
                _ => break, // clean close or transport error
            },
        }
    }
    shared.finish();
}

async fn write_loop<K>(
    shared: Arc<MuxShared>,
    mut sink: K,
    mut data_rx: mpsc::Receiver<Message>,
    mut ctrl_rx: mpsc::Receiver<Message>,
) where
    K: futures_sink::Sink<Message, Error = crate::Error> + Unpin,
{
    loop {
        let msg = tokio::select! {
            biased;
            _ = shared.shutdown.cancelled() => break,
            Some(m) = ctrl_rx.recv() => m,
            Some(m) = data_rx.recv() => m,
            else => break,
        };
        if sink.send(msg).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
    shared.finish();
}

/// Pings every `interval`; a pong must arrive within `timeout` or the
/// connection is failed (streams see `Unavailable`).
async fn keepalive_loop(shared: Arc<MuxShared>, k: Keepalive) {
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = tokio::time::sleep(k.interval) => {}
        }
        let before = shared.pongs.load(Ordering::Acquire);
        if shared.write_msg(Message::Ping(Bytes::new())).await.is_err() {
            return;
        }
        let pong = async {
            loop {
                let notified = shared.pong_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if shared.pongs.load(Ordering::Acquire) != before {
                    return;
                }
                notified.await;
            }
        };
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = pong => {}
            _ = tokio::time::sleep(k.timeout) => {
                shared.finish();
                return;
            }
        }
    }
}
