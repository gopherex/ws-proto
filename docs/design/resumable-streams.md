# Resumable In-Flight RPC Streams Across Client Reconnect

**Status:** Draft
**Owners:** wsrpc maintainers
**Affects:** `wsrpc/` (Go runtime), `packages/ws-proto-transport/` (TS runtime), `transport/transport.proto` (wire)
**Depends on / interacts with:** Phase G opt-in reconnect (`wsrpc.WithReconnect()` / TS `{reconnect:true}`), the in-flight WINDOW_UPDATE / flow-control work being added separately.

---

## Summary

Today the wire protocol multiplexes one RPC per stream over a single WebSocket. Streams are client-initiated; the client allocates monotonic **odd** stream ids per connection (`wsrpc/mux.go:113`, `packages/ws-proto-transport/src/mux.ts:112-115`). Phase G added **opt-in auto-reconnect**: on socket loss the client redials with exponential backoff + full jitter and swaps in a fresh `Mux` (`wsrpc/reconnect.go:97-141`), but **every in-flight stream fails with `codes.Unavailable` / `CODE_UNAVAILABLE`** (`wsrpc/mux.go:229-236` `failAll`, `packages/ws-proto-transport/src/mux.ts:101-109` `onTransportDrop`). The server keeps **no per-stream session state** — when the socket drops, the server `Mux` read loop ends, every `serveStream` goroutine's stream context is cancelled, and the handler unwinds. There is no resume: the caller must retry the whole RPC.

This spec designs **true resume**: when the socket drops and reconnects, in-flight RPCs continue transparently. The hard, unavoidable cost is that **the server can no longer be stateless** — it must retain per-stream session state (live handler goroutines, unacked outbound buffers) across the connection gap, for a bounded TTL, with explicit memory limits. The design is **opt-in and capability-negotiated**; non-resumable peers behave exactly as today.

The spec is concrete about wire changes, server memory cost, phasing, and — in the final section — about whether this is worth building at all versus caller-level retry.

---

## Goals

- In-flight RPCs survive a transport disconnect + redial without surfacing an error to the caller, when both peers opted in.
- Exactly-once delivery semantics across the gap: no lost messages, no double-delivery, in both directions (client→server and server→client).
- Bounded, predictable server memory cost with a hard resume window (TTL) and eviction.
- Strict backward compatibility: default off; a resumable peer talking to a non-resumable peer behaves exactly as today.
- A realistic, reviewable v1 that a maintainer can scope down or reject.

## Non-goals

- Surviving a **server process crash** or restart (session state is in-process memory; a crash loses all sessions — those streams fail with `Unavailable`, same as today). Cross-replica resume behind a load balancer is explicitly out of scope (see §7, Open Questions).
- Resuming across a change of **server identity** (a redial that lands on a different backend in a pool without sticky routing cannot resume).
- Changing the one-RPC-per-stream model, the odd-stream-id allocation, or unary/streaming semantics.
- Client-side persistence across a full **page reload / process restart** (the replay buffers live in memory; a reload loses them). Resume covers a transport drop within one client process lifetime.

---

## Background: what actually happens on disconnect today

Grounding the design in the real code:

- **Client (Go):** `reconnector.run` waits on `mux.ctx.Done()`, drops the dead mux, redials (`redial`/`dialOnce`, `wsrpc/reconnect.go:145-191`), installs a fresh mux, and broadcasts to `NewStream` callers blocked in `waitForMux` (`wsrpc/reconnect.go:197-218`). The fresh mux starts `nextID` at 1 again — **stream ids are per-connection**, so a "resumed" stream would collide with a brand-new one.
- **Client (TS):** `scheduleReconnect` builds a replacement `Mux` that buffers sends until its socket opens, then flushes (`transport.ts:125-153`, `mux.ts:158-178`). Same per-connection id reset.
- **Both clients fail in-flight streams immediately** on drop (`failAll` / `onTransportDrop`) — the new mux never knows about the old streams.
- **Server:** `ServeHTTP` blocks on `<-mux.ctx.Done()` (`wsrpc/server.go:56`); when the read loop dies, the mux context cancels, each handler's `stream.ctx` is cancelled, and `serveStream` finishes and calls `mux.remove`. Nothing is retained.

So resume requires four new capabilities the code does not have: (1) a session identity that outlives one socket, (2) stream identity stable across connections, (3) server-side retention of handler + buffers across the gap, (4) sequencing + ack so each side knows what to replay.

---

## 1. Session identity

A **session** is the logical client identity that survives socket replacement. It must be recognizable by the server on the new Upgrade.

**Issuance.** On the first successful connect, if the client offered the resumable capability (see §8) and the server supports it, the server generates a **session token**: a cryptographically random 256-bit value (32 bytes, `crypto/rand`), base64url-encoded. It is delivered to the client in a new `KIND_RESUME_OK` control frame sent immediately after the handshake completes (frame, not HTTP header, so it works uniformly across both runtimes and survives proxies that strip response headers). The server stores it as the key of an in-memory `session` record.

**Carriage on reconnect.** On redial, the client presents the token. Two options, with a recommendation:

- **(A) HTTP header on the Upgrade request** (`Sec-WebSocket-Protocol` extension token, or a dedicated header like `Ws-Resume-Token`). Pro: server can reject at the HTTP layer before allocating a mux; works with `WithConnContext` (`wsrpc/options.go:81`) which already exposes the Upgrade request. Con: tokens land in proxy/access logs; some proxies normalize/strip custom headers.
- **(B) First frame after open.** The client sends a `KIND_RESUME` frame (§5) as the very first frame on the new socket, before any OPEN. Pro: never logged as a URL/header; uniform across runtimes. Con: the server must briefly accept a not-yet-authenticated socket.

**Recommendation: (B) first-frame `KIND_RESUME`**, because the token is a bearer credential and keeping it out of HTTP logs matters more than early rejection. Auth headers (`WithHeader`, `transport.ts` has no header analog — browser WS can't set arbitrary headers) still flow on the Upgrade as today for the *connection's* authn; the resume token authorizes *reattaching to an existing session*, a separate concern.

**Security of the token.**
- Treat it as a **bearer credential**: anyone holding it can reattach to (and read/inject into) the session's in-flight streams. It MUST be high-entropy (≥256 bits), generated with a CSPRNG, and compared in **constant time** (`subtle.ConstantTimeCompare`) to avoid timing oracles on lookup.
- It MUST only ever travel over **wss://** (TLS). Sending a resume token over plaintext ws:// is a spec violation; the client refuses to attach a resume token to a `ws://` redial.
- **Bind the token to the authenticated principal.** The session record stores the principal derived from the original Upgrade's auth (whatever `WithConnContext` extracted). On reconnect the new Upgrade is independently authenticated as today; the server then checks the resume token AND that the new connection's principal matches the session's. A stolen token without valid connection-level auth still cannot resume. This makes the token a *second factor* for reattachment, not a sole credential.
- **Single-use rotation (optional, recommended):** issue a fresh token in `KIND_RESUME_OK` on every successful resume, invalidating the prior one. Limits replay of a captured token to one use.
- The token is never reused across sessions and is wiped on session eviction.

---

## 2. Stream identity across connections

Stream ids are per-connection (`m.nextID` resets to 1 on every fresh mux). For resume, a stream must map back to the **same server-side handler invocation**, so we need a session-stable identity.

**Decision: keep odd, monotonic stream ids, but scope their uniqueness to the *session*, not the socket.** Concretely:

- The client's `nextID` allocator is **owned by the session, not the mux.** When a fresh mux is installed during a resumable reconnect, it inherits the session's current `nextID` rather than resetting to 1. Thus stream id 7 means the same RPC before and after the gap.
- Because ids no longer reset, the server can use `(session_token, stream_id)` as the stable handler key. A resumed `KIND_RESUME` carries the set of stream ids the client still considers live; the server looks each up in the session's `streams` map.
- New streams opened *after* resume simply continue the monotonic sequence (next odd id after the highest used), so they never collide with resumed ones.

This is a small but load-bearing change: today `newMuxBuffered` hard-codes `nextID: 1` (`wsrpc/mux.go:55`) and TS `Mux` sets `nextId = 1` (`mux.ts:64`). Under resume, the id allocator is hoisted to the `ClientConn` / `WsTransport` (the session owner) and threaded into each new mux.

**Wrap-around:** `nextID` is `uint32`; odd ids exhaust at ~2^31 streams per session. A long-lived session approaching exhaustion must be torn down and re-established (the client closes and reopens — no resume). Documented limit, not handled in v1.

---

## 3. Server-side session state

This is the central cost. Under resume the server retains, per session:

| State | Why | Lifetime |
|---|---|---|
| **`session` record** (token, principal, `streams` map, `lastSeen`, resume window timer) | Identity + lookup on reattach | Until TTL after last disconnect, or explicit END of all streams + idle |
| **Live handler goroutine** per in-flight stream | The handler must *keep running* across the gap so its in-progress work (DB query, upstream call) isn't thrown away — that's the whole point of resume | Until the handler returns OR the resume window expires |
| **Outbound replay buffer** per stream (server→client) | Unacked MSG/HEADER/END frames the client may not have received before the drop; replayed on resume | Until cumulatively acked by the client, or window expiry |
| **Inbound dedupe high-water** per stream (client→server) | Highest contiguous client→server seq the server has accepted, so replayed client frames are deduped | Lifetime of the stream |
| **Detached `frameConn`** | None — the old socket is gone; writes during the gap go nowhere and are *buffered*, not sent | n/a |

**The handler stays alive but cannot write.** When the socket drops, instead of cancelling `stream.ctx` and unwinding `serveStream`, a resumable session **detaches** the stream from the dead mux and **parks** it: the handler goroutine keeps running; its `Send`/`SendHeader`/`end` calls no longer hit a live conn — they append to the stream's **outbound replay buffer** and block (or apply backpressure) if the buffer hits its cap. `stream.ctx` is **not** cancelled (see deadline handling, §7). On resume, the stream is re-attached to the new mux, unacked frames are replayed, and buffered-during-gap frames flush.

**Resume window / TTL.** Each session has a `resumeWindow` (e.g. default 30s, configurable via a new `ServerOption`). On disconnect the session starts a timer. If the client reattaches before it fires, the timer is cancelled and streams resume. If it fires first, the session is **evicted**: every parked handler's `stream.ctx` is cancelled (handlers unwind exactly as on a hard disconnect today), buffers are freed, the token is invalidated. A late `KIND_RESUME` for an evicted session is answered with `KIND_RESUME_REJECT` and the client fails those streams with `Unavailable` (same as today — graceful degradation).

**Memory bounds & eviction.**
- Per-stream outbound replay buffer is capped (bytes, e.g. default 1 MiB or N messages, configurable). When full, the handler's `Send` blocks — this is back-pressure, and it interacts with the separate WINDOW_UPDATE flow-control work (a sender already shouldn't outrun the receiver's window; the replay buffer is bounded by the same outstanding-window budget). If a handler *must* produce faster than the cap and the client is gone, the session is failed rather than growing unbounded.
- Per-session cap on number of parked streams and total buffered bytes; a global cap on number of resumable sessions and aggregate buffered memory. Exceeding the global cap evicts the **oldest-disconnected** sessions first (they're closest to TTL anyway).
- A disconnected session counts a live goroutine *per in-flight stream* for up to `resumeWindow`. This is the scariest line item: a flood of clients that connect, start a long stream, and vanish can pin goroutines + buffers for the whole window. Mitigations: short default window, per-IP/principal session caps, and refusing resume capability for unauthenticated connections.

---

## 4. Message sequencing + acknowledgement

Resume requires each side to know exactly which frames the peer received, so it can replay the rest without gaps or duplicates. We add **per-stream, per-direction sequence numbers** on data-bearing frames and **cumulative acks**.

- **`Frame.seq` (uint64):** monotonic per `(stream_id, direction)`, starting at 1, on every **MSG** and **HEADER** and **END** frame (the frames a receiver must not lose). HALF_CLOSE and RST also get a seq so their ordering relative to MSGs is preserved on replay. OPEN does not need a seq (it *establishes* the stream; its delivery is implied by the server having the stream).
- **`Frame.ack` (uint64):** the highest **contiguous** seq the sender has received from the peer on that stream. Carried two ways:
  - **Piggyback:** any outgoing frame on a stream may set `ack` to the current receive high-water for that stream (cheap, no extra frames in steady bidi traffic).
  - **Standalone `KIND_ACK`:** for one-way streams (server-streaming with a silent client, or vice versa) where there's no return traffic to piggyback on, the receiver periodically emits a `KIND_ACK` frame `{stream_id, ack}`. Sent on a timer and/or every K messages, not per-message (acking every message doubles frame count).
- **Replay buffer (sender side, both directions):** each sender retains sent MSG/HEADER/END frames in an ordered buffer until the peer's cumulative `ack` covers them, then prunes. On resume the sender replays everything with `seq > peer_acked_seq`.
- **Receiver dedupe:** the receiver tracks the highest contiguous seq accepted per stream and **drops any replayed frame with `seq <= high-water`** (idempotent re-delivery, §6). Because seq is contiguous and frames are ordered on one stream, a simple high-water integer suffices — no per-seq bitmap needed.

Both directions are symmetric. Client→server: client retains unacked request MSGs / HALF_CLOSE and replays them; server dedupes. Server→client: server retains unacked response MSGs / HEADER / END (this is the common case — server-streaming) and replays them; client dedupes. The replay buffer is what makes the server stateful in §3.

---

## 5. Resume handshake

Exact frame exchange on reconnect (capability already negotiated, §8):

```
client redials, socket opens
client  → server:  KIND_RESUME {
                     session_token,
                     resume: repeated StreamResume { stream_id, last_recv_seq }
                       // last_recv_seq = highest contiguous server→client seq the
                       // client has accepted for this stream (its receive high-water)
                   }
server validates token (constant-time, principal match):
  ─ session unknown / evicted / TTL expired:
       server → client:  KIND_RESUME_REJECT { reason }
       client fails the listed streams with codes.Unavailable  (== today's behavior)
  ─ session valid:
       server → client:  KIND_RESUME_OK { session_token' (rotated, optional),
                                          ack: repeated StreamAck { stream_id, last_recv_seq } }
                         // server's receive high-water per stream, so the client
                         // knows which client→server frames to replay
       both sides then, per resumed stream:
         - prune own replay buffer up to the peer-reported last_recv_seq
         - replay all retained frames with seq > peer_last_recv_seq, in order
         - resume normal MSG/HALF_CLOSE/END flow on the new mux
new streams opened after resume continue the session's monotonic odd-id sequence
```

Notes:
- The server validates **before** dispatching anything. A stream id in `resume` that the server has no record of (already ended + pruned, or never existed) is reported back as terminated (server replies with the END it already sent, or a synthetic END if it was pruned — see §6 unary case).
- If the new socket *also* drops mid-handshake, the client simply redials again and re-sends `KIND_RESUME`; the handshake is idempotent (it's just exchanging high-water marks).
- Ordering with new streams: the client SHOULD send `KIND_RESUME` as the first frame and wait for `KIND_RESUME_OK` before opening brand-new streams, so the server never sees an OPEN for an id it's about to reconcile.

---

## 6. Idempotency & exactly-once

- **No double-delivery:** guaranteed by receiver dedupe on `seq` (§4). Replayed frames at or below the high-water are dropped. The application sees each MSG exactly once.
- **No loss:** guaranteed by the sender retaining everything above the peer's ack until acked, and replaying it.
- **HALF_CLOSE / END / RST across resume:**
  - HALF_CLOSE and END carry `seq` and are replayed like data, so a HALF_CLOSE lost in the gap is re-sent and applied exactly once (the `halfCloseOne` / `endOne` `sync.Once` in `wsrpc/stream.go:42-44` already make application idempotent).
  - **RST is terminal and abortive.** A stream that was RST before the drop is simply gone; if a `KIND_RESUME` lists it, the server reports it terminated. RST is not replayed for resume — an aborted stream stays aborted.
- **The unary "response sent but END lost" case** (the canonical exactly-once hazard): server finished the handler, sent the single response MSG (seq 1) and END (seq 2), then the socket dropped before the client read them. Two sub-cases:
  1. **Server hasn't pruned** (still within window, END not yet acked): on resume the server replays MSG(1) + END(2); client delivers them once. Correct.
  2. **Server already pruned** the stream (END was acked, or stream fully closed and reaped): the server has no record. But if the END was acked, the client *did* receive it — so the client won't list this stream in `KIND_RESUME` at all. The only lost-END scenario is one where the client did NOT ack, which means the server has NOT pruned (it prunes only on ack). So the dangerous "server forgot, client didn't get it" window cannot occur **as long as pruning is strictly ack-driven.** This is a key invariant: **never prune a frame the peer hasn't cumulatively acked.** A stream is only reaped from the session once its END is acked AND any client→server frames are acked.
  - Edge: client sends ack for END, then drops before the new socket — but it already has the response, so it doesn't list the stream; fine.

---

## 7. Failure modes

- **Server crash / restart:** all in-memory sessions vanish. Client redials, sends `KIND_RESUME`, gets `KIND_RESUME_REJECT` (unknown session), fails those streams with `Unavailable`. Identical to today's behavior — resume is a best-effort optimization layered on top of the existing failure path, never a correctness dependency.
- **Multiple concurrent resumes for one session:** only one socket may own a session at a time. The session record holds the **currently-attached mux**. A second `KIND_RESUME` (e.g. a zombie old client, or a duplicate redial) is handled by: the latest valid resume **wins and fences** the previous attachment (the older socket is closed, its mux detached). Implemented with a per-session generation counter / attach mutex. Replay is safe to repeat because it's high-water-driven.
- **Resume of an already-ended stream:** server reports it terminated (replays the retained END if unpruned, else a synthetic `END{Unavailable}` if pruned — but per §6 a pruned stream was acked, so the client won't ask). The client closes it locally.
- **Window / flow-control interaction:** the outbound replay buffer is bounded by the same outstanding-bytes budget as the separate WINDOW_UPDATE work — a sender already may not have more than one window's worth of unacked data outstanding, which *is* exactly the replay buffer's contents. So flow control caps replay-buffer size for free. On resume, window state is re-established: the receiver's available window is whatever it was (the receiver hasn't consumed the un-replayed frames), so the sender replays up to the window and then respects WINDOW_UPDATEs as normal. Care: a WINDOW_UPDATE in flight when the socket dropped is *not* replayed (it's control, not data) — instead the receiver re-advertises its current window in / right after `KIND_RESUME_OK`.
- **Deadlines across the gap:** the original per-call deadline (`ws-timeout-ms`, `wsrpc/mux.go:16,169-173`) **still applies and keeps running** during the gap. The server derives `stream.ctx` from the timeout at OPEN; we do **not** cancel it on disconnect for a parked stream (that's the change in §3), but we also do **not** extend it. If the deadline fires while parked, the handler is cancelled normally and the resulting END is buffered for replay (and delivered if the client comes back in time, else dropped on eviction). Rationale: resume should be transparent, and transparency means the caller's deadline semantics are unchanged — a 5s unary call is still a 5s call whether or not the socket blipped.
- **Client gives up:** the client's own reconnect backoff is bounded only by `Close`; but a caller-supplied stream `ctx`/deadline still fails the stream locally if it expires during the gap, and the client stops trying to resume that stream.
- **Resume after partial replay then second drop:** idempotent — high-water marks advance monotonically; a second `KIND_RESUME` just resumes from the new high-water.

---

## 8. Backward compatibility & opt-in

Resume is **off by default** and **negotiated**, so a resumable peer talking to a non-resumable peer behaves exactly as today.

**Capability negotiation — recommended: WebSocket subprotocol token.** Today both sides offer `wsrpc.v1` (`wsrpc/options.go:32`, `transport.ts:13`). Add a second token `wsrpc.v1.resume`:
- A resume-capable client offers **both** `["wsrpc.v1.resume", "wsrpc.v1"]` (preference order).
- A resume-capable server that also opted in selects `wsrpc.v1.resume`; otherwise it selects `wsrpc.v1`.
- The client reads the negotiated `protocol` (`coder/websocket` exposes it; browser `WebSocket.protocol` is already surfaced via `WebSocketLike.protocol`, `mux.ts:51`). If it's not `wsrpc.v1.resume`, the client runs in **today's mode**: reconnect (if enabled) fails in-flight streams; no seq/ack/replay overhead.
- Subprotocol-based negotiation has the bonus that intermediary proxies can route/identify resumable traffic (already a stated benefit of the subprotocol token).

Wire fields (`seq`, `ack`) are **additive** proto3 fields; an old peer ignores unknown fields, and a new peer treats absent `seq`/`ack` as 0 / non-resumable. New `Kind`s (RESUME, RESUME_OK, RESUME_REJECT, ACK) are only ever sent **after** `wsrpc.v1.resume` was negotiated, so an old peer never receives a kind it doesn't understand. Opt-in is also gated by explicit options:
- Go: `wsrpc.WithReconnect(wsrpc.WithResume())` (client) and `wsrpc.WithResumableSessions(WithResumeWindow(30*time.Second), WithMaxSessions(...), WithMaxBufferedBytes(...))` (server `ServerOption`).
- TS: `{ reconnect: true, resume: true }` on `WsTransportOptions`; server-side resume lives in the Go server.

If `WithResume()` is set but the negotiated subprotocol is plain `wsrpc.v1`, the client logs once and degrades to non-resumable. No hard failure.

---

## Wire changes

New `Kind`s and two new `Frame` fields. Sketch (additive to `transport/transport.proto`):

```proto
message Frame {
  uint32 stream_id = 1;
  Kind   kind      = 2;
  map<string, string> headers = 3;
  bytes  payload   = 4;
  Status status    = 5;
  string method    = 6;

  // --- resume additions (only meaningful after wsrpc.v1.resume negotiated) ---
  uint64 seq = 7;   // per (stream_id, direction) seq on MSG/HEADER/HALF_CLOSE/END; 0 = unset
  uint64 ack = 8;   // cumulative highest-contiguous seq received from peer on this stream

  // Control-frame bodies (set only on the matching Kind). Kept as nested
  // messages to avoid overloading headers/payload:
  Resume        resume        = 9;   // KIND_RESUME
  ResumeOk      resume_ok      = 10;  // KIND_RESUME_OK
  ResumeReject  resume_reject  = 11;  // KIND_RESUME_REJECT
  // KIND_ACK uses only stream_id + ack (no body).
}

enum Kind {
  KIND_UNSPECIFIED = 0;
  KIND_OPEN        = 1;
  KIND_MSG         = 2;
  KIND_HALF_CLOSE  = 3;
  KIND_END         = 4;
  KIND_RST         = 5;
  KIND_HEADER      = 6;
  // resume additions:
  KIND_RESUME        = 7;  // client->server, first frame on a redialed socket
  KIND_RESUME_OK     = 8;  // server->client, accepts + reports per-stream ack
  KIND_RESUME_REJECT = 9;  // server->client, session unknown/evicted -> fail streams
  KIND_ACK           = 10; // either direction, standalone cumulative ack (stream_id 0 reserved? no: per-stream)
}

message Resume {
  string token = 1;                      // session token (bearer; wss:// only)
  repeated StreamResume streams = 2;
}
message StreamResume { uint32 stream_id = 1; uint64 last_recv_seq = 2; }

message ResumeOk {
  string token = 1;                      // rotated session token (optional)
  repeated StreamAck streams = 2;        // server's receive high-water per stream
}
message StreamAck { uint32 stream_id = 1; uint64 last_recv_seq = 2; }

message ResumeReject { int32 reason = 1; string message = 2; }
```

Also: the initial session token is delivered on the first connect via a `KIND_RESUME_OK` frame with an empty `streams` list and the freshly-issued `token` (reusing the message rather than adding a "session established" kind).

---

## Server state & memory

Per-session record (Go sketch, in-memory map keyed by token, guarded by a mutex; lives on the `Server`):

```go
type session struct {
    token      string                 // constant-time compared
    principal  string                 // bound at first connect; checked on resume
    streams    map[uint32]*resumable  // session-stable id -> parked stream state
    attach     *Mux                   // currently attached mux (nil while disconnected)
    gen        uint64                 // fencing generation for concurrent resumes
    lastSeen   time.Time
    windowT    *time.Timer            // fires -> evict
    bufferedBytes int64               // sum across streams, against per-session cap
}

type resumable struct {
    stream       *Stream
    outReplay    []*transport.Frame   // unacked server->client frames (seq-ordered)
    outAckedSeq  uint64               // pruned up to here
    inHighWater  uint64               // highest contiguous client->server seq accepted (dedupe)
}
```

Cost model (the honest part):
- **Goroutines:** 1 per in-flight stream, kept alive for up to `resumeWindow` after a disconnect. A stateless server reaps these instantly today; resume pins them.
- **Memory:** outbound replay buffer per stream, capped by the flow-control window (e.g. 64 KiB–1 MiB), plus small fixed per-session overhead. Worst case ≈ `maxSessions × maxStreamsPerSession × windowBytes`. With sane caps (e.g. 10k sessions × 64 KiB) that's ~640 MiB ceiling — must be a configured, enforced limit, not a hope.
- **New server config (`ServerOption`s):** `WithResumeWindow(d)`, `WithMaxResumableSessions(n)`, `WithMaxBufferedBytesPerStream(n)`, `WithMaxBufferedBytesTotal(n)`, `WithSessionsPerPrincipal(n)`. Defaults conservative (window 30s, modest caps).
- **Eviction order under pressure:** oldest-disconnected sessions first; then refuse new resumable sessions (degrade new connections to non-resumable rather than OOM).

---

## Phased implementation plan

A realistic build order, smallest useful increment first.

**Phase R0 — Plumbing & negotiation (no behavior change).**
Add `seq`/`ack` fields and the new `Kind`s to the proto and both codecs. Add the `wsrpc.v1.resume` subprotocol token and the opt-in options (off by default). Negotiate but do nothing yet. Ships dark.

**Phase R1 — Server→client resume only (the 80% case).**
Resume **server-streaming** responses, where the server is the buffering sender and the client is the receiver. This is where resume pays off most (long-lived server pushes) and is simplest: only the server holds a replay buffer; the client holds only a receive high-water + dedupe. Client→server (request) frames in this phase are assumed already fully sent before the gap (true for server-streaming and unary). Implement: session record, parked handler goroutines, server outbound replay buffer, `KIND_RESUME`/`KIND_RESUME_OK`/`KIND_RESUME_REJECT`, client-side dedupe, the session-scoped id allocator (§2), resume window + eviction. **This is a credible v1 on its own.**

**Phase R2 — Full bidirectional resume.**
Add client→server replay buffers + server-side dedupe, so client-streaming and bidi RPCs resume too. More state on the client; symmetric to R1.

**Phase R3 — Hardening.**
Token rotation, per-principal session caps, standalone `KIND_ACK` cadence tuning, metrics (buffered bytes, parked goroutines, resume success/reject rates), fencing for concurrent resumes, fuzz/chaos tests that drop the socket at every frame boundary.

**Defer / maybe-never:** cross-replica resume (needs externalized session state — Redis/sticky routing, large project), client persistence across page reload, server-crash survival.

---

## Open questions

1. **Token carriage** — first-frame `KIND_RESUME` (recommended) vs. Upgrade header. Header is friendlier to edge rejection and `WithConnContext`; frame is safer for logs. Decide per security review.
2. **Cross-replica** — explicitly out of scope for v1, but does the team need it soon? If yes, the whole session-state design should be externalized from day one rather than retrofitted, which changes R1 substantially.
3. **Deadline policy** — keep the original deadline running through the gap (recommended, transparent) vs. pause it during disconnect (more forgiving but surprising). Confirm with API consumers.
4. **Ack cadence** — timer interval and per-K-message threshold for standalone `KIND_ACK` on one-way streams; tune against frame-count overhead vs. replay-buffer size.
5. **Browser header limitation** — browsers can't set arbitrary Upgrade headers, which is another reason to prefer first-frame token carriage for symmetry between the Go and TS clients.
6. **Interaction ordering with the in-flight WINDOW_UPDATE work** — replay-buffer sizing leans on flow control; these two features should land in a coordinated order (flow control first, or at least its window accounting).
7. **Resume + middleware/interceptors** — a parked handler holds open middleware/interceptor scopes (`wsrpc/server.go:81`, bridge interceptors) across the gap; confirm no middleware assumes a live connection (e.g. logging that reads peer addr).

---

## Recommendation: should we even build this?

**Honest assessment: build Phase R1 (server→client resume) only if there is a concrete product need for long-lived server-push streams that must survive flaky networks; otherwise prefer caller-level retry.**

The reasons to be skeptical:

- **It destroys the stateless-server property.** Today the server holds zero per-stream state after a disconnect; handlers reap instantly. Resume pins a goroutine *and* a replay buffer per in-flight stream for the whole resume window, and forces hard memory caps, eviction policy, per-principal quotas, and a new abuse surface (a client can cheaply pin server resources by connecting, starting streams, and vanishing). That is a large, permanent increase in operational complexity for an RPC layer that is currently pleasantly simple.
- **Caller-level retry already covers most cases correctly.** Unary and idempotent RPCs can just be retried by the caller on `Unavailable` (Phase G's reconnect already keeps the socket alive for the retry). For these, resume buys only the avoidance of redoing in-progress server work — real but often cheap.
- **The genuinely hard case is non-idempotent or long-running streams** (a server-streaming feed you don't want to restart from the beginning, or a client-streaming upload you don't want to re-send). *That* is where resume earns its keep — and it's exactly Phase R1/R2.

**Therefore:**
- If the product is request/response and mostly idempotent: **do not build resume.** Document caller-level retry with `WithReconnect`, and stop. The cost is not justified.
- If the product has long-lived server-streaming pushes over mobile/flaky networks where restarting the stream is expensive or user-visible: **build R0 + R1**, keep it strictly opt-in and capability-negotiated, ship conservative caps, and defer R2/cross-replica until a second concrete need appears.
- **Do not build full bidi resume (R2) speculatively.** It roughly doubles the state and the test surface for a narrower set of RPC shapes.

The architecture above is designed so this decision is reversible and incremental: R0 is inert, R1 is independently useful, and the non-resumable fallback path is *the same code path that exists today* — so even with resume enabled, every failure mode degrades to "fail the stream with `Unavailable`," which callers already handle.
