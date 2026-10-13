//! One multiplexed RPC stream. Untyped: payloads are marshaled messages;
//! the generated bindings add typed `send`/`recv` on top.
//!
//! Mirrors the Go `wsrpc.Stream`, including its flow-control model:
//!
//! - **send window**: starts at `initial_window`, decremented per MSG, may go
//!   negative once (an oversized message is still delivered), replenished by
//!   WINDOW_UPDATE. `send` awaits credit; the reader task only ever signals.
//! - **receive side**: inbound MSGs queue in a byte-bounded backlog; credit is
//!   returned on *consumption* (`recv`), coalesced once pending credit crosses
//!   `initial_window / 2`.
//! - **terminal frames** (END/RST) bypass the queue; `recv` drains buffered
//!   MSGs first, then observes the terminal status.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use prost::Message;
use tokio::sync::Notify;

use crate::{
    mux::MuxShared,
    status::{Code, Status},
    transport::{Frame, Kind},
};

/// Per-RPC metadata (OPEN headers, HEADER frame, END trailers).
pub type Metadata = BTreeMap<String, String>;

/// A cheap, cloneable handle to one RPC stream.
#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
}

pub(crate) struct StreamInner {
    pub(crate) id: u32,
    method: String,
    mux: Arc<MuxShared>,
    /// Caller deadline carried on OPEN (`ws-timeout-ms`), server side; the
    /// client's own timeout on the client side.
    timeout: Option<Duration>,
    initial_window: i64,
    st: Mutex<State>,
    /// Woken when a MSG is queued, the peer half-closes, or the stream ends.
    recv_notify: Notify,
    /// Woken when send credit arrives or the stream ends.
    send_notify: Notify,
}

#[derive(Default)]
struct State {
    header: Metadata,
    trailer: Metadata,
    out_trailer: Metadata,
    header_sent: bool,
    msg_sent: bool,
    send_done: bool,

    recv_items: VecDeque<Bytes>,
    recv_bytes: i64,
    half_closed: bool,

    /// Terminal: set once by `signal_end` (frame) or `fail_with` (status).
    ended: bool,
    end_frame: Option<Frame>,
    end_status: Option<Status>,

    send_window: i64,
    pending_credit: i64,
}

impl StreamInner {
    pub(crate) fn new(
        id: u32,
        method: String,
        mux: Arc<MuxShared>,
        timeout: Option<Duration>,
    ) -> Arc<Self> {
        let w = i64::from(mux.initial_window);
        Arc::new(Self {
            id,
            method,
            mux,
            timeout,
            initial_window: w,
            st: Mutex::new(State {
                send_window: w,
                ..Default::default()
            }),
            recv_notify: Notify::new(),
            send_notify: Notify::new(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap()
    }

    // ---- reader-task entry points (never block) ----

    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn set_leading_header(&self, md: Metadata) {
        self.lock().header = md;
    }

    pub(crate) fn credit_send(&self, delta: u32) {
        if delta == 0 {
            return;
        }
        self.lock().send_window += i64::from(delta);
        self.send_notify.notify_waiters();
    }

    pub(crate) fn half_close(&self) {
        self.lock().half_closed = true;
        self.recv_notify.notify_waiters();
    }

    /// Enqueues a MSG. Returns false only when the byte-bounded backlog is
    /// already over its ceiling (peer ignoring the window). "Already over"
    /// rather than "would exceed" always admits at least one message, even
    /// one larger than the whole window.
    pub(crate) fn try_deliver(&self, payload: Bytes) -> bool {
        {
            let mut s = self.lock();
            if s.ended {
                return true; // already ending; dropping is fine
            }
            if !s.recv_items.is_empty() && s.recv_bytes > self.initial_window {
                return false;
            }
            s.recv_bytes += payload.len() as i64;
            s.recv_items.push_back(payload);
        }
        self.recv_notify.notify_waiters();
        true
    }

    /// Records a terminal END/RST frame.
    pub(crate) fn signal_end(&self, f: Frame) {
        {
            let mut s = self.lock();
            if s.ended {
                return;
            }
            s.ended = true;
            s.end_frame = Some(f);
        }
        self.wake_all();
    }

    /// Terminates locally with `st` (local end, connection loss, overflow).
    pub(crate) fn fail_with(&self, st: Status) {
        {
            let mut s = self.lock();
            s.end_status = Some(st);
            s.ended = true;
        }
        self.wake_all();
    }

    fn wake_all(&self) {
        self.recv_notify.notify_waiters();
        self.send_notify.notify_waiters();
    }
}

/// Wait on `notify` until `check` yields a value. The `Notified` future is
/// registered *before* the state is inspected, so a wake between the check
/// and the await is never lost.
async fn wait_until<T>(notify: &Notify, mut check: impl FnMut() -> Option<T>) -> T {
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(v) = check() {
            return v;
        }
        notified.await;
    }
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.inner.lock();
        f.debug_struct("Stream")
            .field("id", &self.inner.id)
            .field("method", &self.inner.method)
            .field("ended", &s.ended)
            .finish()
    }
}

impl Stream {
    pub(crate) fn from_inner(inner: Arc<StreamInner>) -> Self {
        Self { inner }
    }

    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Fully-qualified method: `/pkg.Service/Method`.
    pub fn method(&self) -> &str {
        &self.inner.method
    }

    /// Deadline budget: on the server, the caller's `ws-timeout-ms`; on the
    /// client, the timeout passed to `open_stream`.
    pub fn timeout(&self) -> Option<Duration> {
        self.inner.timeout
    }

    /// Leading headers: request headers from OPEN (server side) or the
    /// optional leading response metadata from a HEADER frame (client side).
    pub fn header(&self) -> Metadata {
        self.inner.lock().header.clone()
    }

    /// Trailers from the END frame (client side). Empty until ended.
    pub fn trailer(&self) -> Metadata {
        self.inner.lock().trailer.clone()
    }

    /// Resolves once the stream has ended (terminal frame or local failure).
    pub async fn ended(&self) {
        wait_until(&self.inner.recv_notify, || {
            self.inner.lock().ended.then_some(())
        })
        .await
    }

    // ---- send side ----

    /// Server only: sends one leading HEADER frame with response metadata.
    /// Must precede any `send`; otherwise `FailedPrecondition`.
    pub async fn send_header(&self, md: Metadata) -> Result<(), Status> {
        {
            let mut s = self.inner.lock();
            if s.header_sent || s.msg_sent {
                return Err(Status::new(
                    Code::FailedPrecondition,
                    "wsrpc: headers already sent",
                ));
            }
            s.header_sent = true;
        }
        self.inner
            .mux
            .write(Frame {
                stream_id: self.inner.id,
                kind: Kind::Header as i32,
                headers: md,
                ..Default::default()
            })
            .await
    }

    /// Server only: merges `md` into the trailers flushed with END.
    pub fn set_trailer(&self, md: Metadata) {
        if md.is_empty() {
            return;
        }
        self.inner.lock().out_trailer.extend(md);
    }

    /// Marshals and sends one message (see [`send_raw`](Self::send_raw)).
    pub async fn send<M: Message>(&self, msg: &M) -> Result<(), Status> {
        self.send_raw(Bytes::from(msg.encode_to_vec())).await
    }

    /// Sends an already-marshaled message as a MSG frame, awaiting
    /// flow-control credit first. A MSG may go whenever the window is
    /// positive; the window is then debited and may go negative.
    pub async fn send_raw(&self, payload: Bytes) -> Result<(), Status> {
        let n = payload.len() as i64;
        let inner = &self.inner;
        wait_until(&inner.send_notify, || {
            let mut s = inner.lock();
            if let Some(err) = send_block_err(&s) {
                return Some(Err(err));
            }
            if s.send_window <= 0 {
                return None;
            }
            s.send_window -= n;
            s.msg_sent = true;
            Some(Ok(()))
        })
        .await?;
        inner
            .mux
            .write(Frame {
                stream_id: inner.id,
                kind: Kind::Msg as i32,
                payload,
                ..Default::default()
            })
            .await?;
        if let Some(st) = &inner.mux.stats {
            st.msg_sent(&inner.method, n as usize);
        }
        Ok(())
    }

    /// Client: signals we are done sending (HALF_CLOSE). Idempotent.
    pub async fn close_send(&self) -> Result<(), Status> {
        {
            let mut s = self.inner.lock();
            if s.send_done {
                return Ok(());
            }
            s.send_done = true;
        }
        self.inner
            .mux
            .write(Frame {
                stream_id: self.inner.id,
                kind: Kind::HalfClose as i32,
                ..Default::default()
            })
            .await
    }

    /// Server: finishes the stream with `st` and the accumulated trailers,
    /// then terminates locally even if the write failed. Idempotent on the
    /// local side.
    pub async fn end(&self, st: Status) -> Result<(), Status> {
        let trailers = std::mem::take(&mut self.inner.lock().out_trailer);
        let res = self
            .inner
            .mux
            .write(Frame {
                stream_id: self.inner.id,
                kind: Kind::End as i32,
                headers: trailers,
                status: Some(st.to_proto()),
                ..Default::default()
            })
            .await;
        self.inner.fail_with(st);
        self.inner.mux.remove(self.inner.id);
        res
    }

    // ---- receive side ----

    /// Receives and unmarshals the next message. `Ok(None)` on a clean end
    /// (peer half-closed, or END with OK); `Err` on a non-OK END / RST.
    pub async fn recv<M: Message + Default>(&self) -> Result<Option<M>, Status> {
        match self.recv_raw().await? {
            None => Ok(None),
            Some(b) => M::decode(b)
                .map(Some)
                .map_err(|e| Status::new(Code::Internal, format!("wsrpc: decode: {e}"))),
        }
    }

    /// Receives the next payload without unmarshaling. Buffered MSGs are
    /// always delivered before a half-close or terminal status is reported.
    pub async fn recv_raw(&self) -> Result<Option<Bytes>, Status> {
        let inner = &self.inner;
        enum Step {
            Msg(Bytes),
            HalfClosed,
            Ended,
        }
        let step = wait_until(&inner.recv_notify, || {
            let mut s = inner.lock();
            if let Some(b) = s.recv_items.pop_front() {
                s.recv_bytes -= b.len() as i64;
                return Some(Step::Msg(b));
            }
            if s.ended {
                return Some(Step::Ended);
            }
            if s.half_closed {
                return Some(Step::HalfClosed);
            }
            None
        })
        .await;
        match step {
            Step::Msg(b) => {
                self.return_credit(b.len()).await;
                Ok(Some(b))
            }
            Step::HalfClosed => Ok(None),
            Step::Ended => self.recv_ended(),
        }
    }

    /// Credit is returned on consumption, coalesced at `initial_window / 2`.
    async fn return_credit(&self, n: usize) {
        if n == 0 {
            return;
        }
        let delta = {
            let mut s = self.inner.lock();
            s.pending_credit += n as i64;
            let threshold = (self.inner.initial_window / 2).max(1);
            if s.pending_credit >= threshold {
                std::mem::take(&mut s.pending_credit)
            } else {
                0
            }
        };
        if delta == 0 {
            return;
        }
        // Best effort: a failed write means the connection is going away.
        let _ = self
            .inner
            .mux
            .write(Frame {
                stream_id: self.inner.id,
                kind: Kind::WindowUpdate as i32,
                window: u32::try_from(delta).unwrap_or(u32::MAX),
                ..Default::default()
            })
            .await;
    }

    fn recv_ended(&self) -> Result<Option<Bytes>, Status> {
        let mut s = self.inner.lock();
        let Some(f) = s.end_frame.take() else {
            // Frameless terminal: local end, connection loss or overflow.
            return match &s.end_status {
                Some(st) if !st.is_ok() => Err(st.clone()),
                _ => Ok(None),
            };
        };
        let st = Status::from_proto(f.status.as_ref());
        if !f.headers.is_empty() {
            s.trailer = f.headers;
        }
        s.end_status = Some(st.clone());
        match Kind::try_from(f.kind) {
            Ok(Kind::Rst) => Err(if st.is_ok() {
                Status::new(Code::Cancelled, "stream reset")
            } else {
                st
            }),
            _ => {
                if st.is_ok() {
                    Ok(None)
                } else {
                    Err(st)
                }
            }
        }
    }
}

/// What a blocked `send` should return instead of waiting, if anything.
fn send_block_err(s: &State) -> Option<Status> {
    if !s.ended {
        return None;
    }
    Some(match &s.end_status {
        Some(st) if !st.is_ok() => st.clone(),
        // Terminal frame arrived but not yet applied by recv, or clean end.
        _ => Status::new(Code::Cancelled, "wsrpc: stream ended"),
    })
}
