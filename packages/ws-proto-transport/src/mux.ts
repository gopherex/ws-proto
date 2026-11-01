import { encodeFrame, decodeFrame, Kind } from "./frame.js";
import { StreamImpl, DEFAULT_INITIAL_WINDOW, DEFAULT_MAX_SEND_BUFFER } from "./stream.js";
import type { StreamHooks } from "./stream.js";
import {
  WsStatusError,
  statusErrorFromProto,
  CODE_CANCELLED,
  CODE_DEADLINE_EXCEEDED,
  CODE_RESOURCE_EXHAUSTED,
  CODE_UNAVAILABLE,
  abortError,
} from "./status.js";

/**
 * DEFAULT_MAX_RECEIVE_QUEUE is retained for API compatibility only. The
 * per-stream inbound backlog is now bounded by BYTES (aligned with the
 * flow-control window), not by this frame count.
 */
export const DEFAULT_MAX_RECEIVE_QUEUE = 256;

/**
 * MAX_PREOPEN_BUFFER_BYTES bounds how much frame data the mux buffers before its
 * socket opens (or during a reconnect gap). Past this, the connection is failed
 * rather than buffering without limit — a safety net against a producer that
 * keeps sending while the socket never opens.
 */
const MAX_PREOPEN_BUFFER_BYTES = 4 * 1024 * 1024; // 4 MiB

export { DEFAULT_INITIAL_WINDOW } from "./stream.js";

/**
 * StreamInit configures a new RPC stream opened via openStream: request
 * metadata (headers carried on the OPEN frame) and an optional AbortSignal that
 * aborts the RPC (sends RST; recv()/iteration reject) when triggered.
 */
export interface StreamInit {
  /** Request metadata sent as headers on the OPEN frame. */
  headers?: Record<string, string>;
  /** Aborts the RPC when triggered: sends RST and rejects recv()/iteration. */
  signal?: AbortSignal;
  /**
   * Per-call deadline in milliseconds. When set (>0) the OPEN frame carries a
   * `ws-timeout-ms` header so the server derives a matching deadline, and a
   * local timer aborts the stream with CODE_DEADLINE_EXCEEDED on expiry.
   */
  timeoutMs?: number;
}

/**
 * WebSocketLike is the minimal browser-WebSocket surface the mux drives. The
 * real browser `WebSocket` satisfies it, as does the test FakeSocket.
 */
export interface WebSocketLike {
  binaryType: string;
  send(data: ArrayBufferView | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onopen: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  /** The subprotocol selected by the server during the RFC 6455 handshake. Empty string if none was negotiated. */
  readonly protocol?: string;
}

/** WebSocket.readyState OPEN constant; defined locally to avoid DOM coupling. */
const WS_OPEN = 1;

/**
 * NOOP_HOOKS backs a stream that is failed at birth (openStream on a defunct
 * mux): it never writes frames, so its cancel/abort paths touch no socket.
 */
const NOOP_HOOKS: StreamHooks = {
  sendMsg: () => {},
  halfClose: () => {},
  reset: () => {},
  windowUpdate: () => {},
};

/**
 * SUBPROTOCOL is the WebSocket subprotocol the client offers and the server must
 * echo. Defined here (rather than in transport.ts) so the Mux can validate the
 * negotiated value without importing transport.ts (which would be a cycle).
 */
export const SUBPROTOCOL = "wsrpc.v1";

/**
 * MuxLifecycle lets the owner (WsTransport) observe the socket. `open` fires
 * once the socket opened with the right subprotocol. `drop` fires at most once
 * when the connection dies by itself (never for close()): `redial` is true for
 * a transport loss worth redialing, false for a permanent failure that would
 * just recur (subprotocol mismatch, oversize inbound frame, pre-open overflow).
 */
export interface MuxLifecycle {
  open(): void;
  drop(err: WsStatusError, redial: boolean): void;
}

/** failedStream returns a stream already ended with `err`; it never touches a socket. */
export function failedStream(
  id: number,
  err: WsStatusError,
  initialWindow?: number,
  maxSendBuffer?: number,
): StreamImpl {
  const stream = new StreamImpl(id, NOOP_HOOKS, initialWindow, maxSendBuffer);
  stream.endError(err);
  return stream;
}

/**
 * Mux owns one WebSocketLike and multiplexes many streams over it. Client only:
 * it allocates monotonic ODD stream ids (1, 3, 5, ...) and never reacts to OPEN.
 */
export class Mux {
  private readonly ws: WebSocketLike;
  private readonly streams = new Map<number, StreamImpl>();
  private nextId = 1;
  private readonly initialWindow: number;
  /** Byte ceiling for a stream's inbound backlog (aligned with the window). */
  private readonly maxRecvBytes: number;
  /** Byte ceiling for a stream's unsent outbound backlog. */
  private readonly maxSendBuffer: number;
  /** Optional cap (bytes) on a single inbound message; 0 disables it. */
  private readonly maxFrameBytes: number;

  /** Frames produced before the socket reaches OPEN are buffered here. */
  private readonly sendBuffer: Uint8Array[] = [];
  /** Total bytes currently held in sendBuffer (bounded by MAX_PREOPEN_BUFFER_BYTES). */
  private sendBufferBytes = 0;
  private open = false;
  private closed = false;
  /**
   * dead marks a mux whose socket dropped (not via close()). Together with
   * `closed` it forms the "gone for good" state: no frame may be buffered or
   * written, and new streams fail fast with `terminalError`. This is distinct
   * from "not yet open" (`!open`), where frames legitimately buffer.
   */
  private dead = false;
  /** The error that killed this mux; reused to fail late openStream() calls. */
  private terminalError?: WsStatusError;
  /** Owner hooks; drop is cleared after it fires so it fires at most once. */
  private lifecycle?: MuxLifecycle;

  constructor(
    ws: WebSocketLike,
    maxReceiveQueue: number = DEFAULT_MAX_RECEIVE_QUEUE,
    initialWindow: number = DEFAULT_INITIAL_WINDOW,
    maxSendBuffer: number = DEFAULT_MAX_SEND_BUFFER,
    maxFrameBytes = 0,
  ) {
    this.ws = ws;
    void maxReceiveQueue; // deprecated frame-count bound; retained for API compat
    this.initialWindow = initialWindow > 0 ? initialWindow : DEFAULT_INITIAL_WINDOW;
    this.maxRecvBytes = this.initialWindow;
    this.maxSendBuffer = maxSendBuffer > 0 ? maxSendBuffer : DEFAULT_MAX_SEND_BUFFER;
    this.maxFrameBytes = maxFrameBytes > 0 ? maxFrameBytes : 0;
    this.ws.binaryType = "arraybuffer";

    // If the socket is already open (e.g. fromSocket on a live socket), flush.
    if ((this.ws as { readyState?: number }).readyState === WS_OPEN) {
      if (this.validateProtocol()) {
        this.open = true;
      }
    }

    this.ws.onopen = () => {
      if (this.closed || this.dead || !this.validateProtocol()) {
        return;
      }
      this.open = true;
      this.flush();
      this.lifecycle?.open();
    };
    this.ws.onmessage = (ev) => this.handleMessage(ev.data);
    // A socket close/error that is NOT a deliberate close() is a transport
    // disconnect: in-flight streams fail with CODE_UNAVAILABLE ("connection
    // lost") and any reconnect controller is notified to redial.
    this.ws.onclose = () => this.onTransportDrop();
    this.ws.onerror = () => this.onTransportDrop();
  }

  /** setLifecycle registers the owner's open/drop hooks. */
  setLifecycle(lifecycle: MuxLifecycle): void {
    this.lifecycle = lifecycle;
  }

  /** isOpen reports whether the socket is open and carrying frames. */
  get isOpen(): boolean {
    return this.open && !this.closed && !this.dead;
  }

  /**
   * validateProtocol verifies the server selected the wsrpc subprotocol. A
   * mismatch (empty or other value) means the peer or an intermediary proxy is
   * not speaking this framing protocol, so all frames would be mis-interpreted.
   * It permanently fails the connection (no reconnect — the misconfiguration
   * would just recur) and returns false. Returns true when the protocol is
   * absent (a minimal WebSocketLike that does not expose it) or matches.
   */
  private validateProtocol(): boolean {
    const p = this.ws.protocol;
    if (p !== undefined && p !== SUBPROTOCOL) {
      this.fail(
        new WsStatusError(
          CODE_UNAVAILABLE,
          `server did not negotiate subprotocol "${SUBPROTOCOL}" (got "${p}")`,
        ),
        1002,
        "subprotocol not negotiated",
      );
      return false;
    }
    return true;
  }

  /**
   * markDefunct transitions the mux to its terminal state: remembers the cause
   * for late openStream() calls, releases the pre-open frame buffer (those
   * frames can never be delivered), and fails every registered stream. Safe to
   * call more than once; the first cause wins.
   */
  private markDefunct(err: WsStatusError): void {
    this.terminalError ??= err;
    this.sendBuffer.length = 0;
    this.sendBufferBytes = 0;
    this.failAll(err);
  }

  private onTransportDrop(): void {
    if (this.closed || this.dead) {
      return; // deliberate close / earlier drop already failed the streams
    }
    this.dead = true;
    const err = new WsStatusError(CODE_UNAVAILABLE, "connection lost");
    this.markDefunct(err);
    this.notifyDrop(err, true);
  }

  /**
   * fail permanently kills the connection from inside the mux (a failure that
   * redialing would only repeat): terminal state, socket closed with `code`,
   * owner told not to redial.
   */
  private fail(err: WsStatusError, code: number, reason: string): void {
    this.closed = true; // terminal: suppress further sends and socket events
    this.markDefunct(err);
    try {
      this.ws.close(code, reason);
    } catch {
      // closing an already-closed socket is fine
    }
    this.notifyDrop(err, false);
  }

  private notifyDrop(err: WsStatusError, redial: boolean): void {
    const lifecycle = this.lifecycle;
    this.lifecycle = undefined; // fire at most once
    lifecycle?.drop(err, redial);
  }

  /** openStream allocates an odd stream id, registers the stream, and sends OPEN. */
  openStream(method: string, init?: StreamInit): StreamImpl {
    const id = this.nextId;
    this.nextId += 2;

    if (this.closed || this.dead) {
      // A defunct mux can never carry a new RPC. Decide the call's fate NOW:
      // fail it fast with the terminal cause (the caller's retry loop redials)
      // instead of buffering frames toward a socket that is gone forever.
      return failedStream(
        id,
        this.terminalError ?? new WsStatusError(CODE_UNAVAILABLE, "connection lost"),
        this.initialWindow,
        this.maxSendBuffer,
      );
    }

    const stream = new StreamImpl(
      id,
      {
        sendMsg: (sid, payload) =>
          this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_MSG, payload })),
        halfClose: (sid) =>
          this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_HALF_CLOSE })),
        reset: (sid) => {
          this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_RST }));
          this.streams.delete(sid);
        },
        windowUpdate: (sid, delta) =>
          this.writeFrame(
            encodeFrame({ streamId: sid, kind: Kind.KIND_WINDOW_UPDATE, window: delta }),
          ),
      },
      this.initialWindow,
      this.maxSendBuffer,
    );
    this.streams.set(id, stream);

    // Propagate a per-call deadline: carry ws-timeout-ms on OPEN so the server
    // derives a matching deadline, and locally abort the stream on expiry.
    const timeoutMs = init?.timeoutMs;
    const headers = { ...(init?.headers ?? {}) };
    if (timeoutMs !== undefined && timeoutMs > 0) {
      headers["ws-timeout-ms"] = String(timeoutMs);
    }

    this.writeFrame(encodeFrame({ streamId: id, kind: Kind.KIND_OPEN, method, headers }));

    if (timeoutMs !== undefined && timeoutMs > 0) {
      const timer = setTimeout(() => {
        stream.abort(new WsStatusError(CODE_DEADLINE_EXCEEDED, "deadline exceeded"));
      }, timeoutMs);
      stream.onClose(() => clearTimeout(timer));
    }

    const signal = init?.signal;
    if (signal) {
      if (signal.aborted) {
        stream.abort(abortError(signal));
      } else {
        const onAbort = () => stream.abort(abortError(signal));
        signal.addEventListener("abort", onAbort, { once: true });
        stream.onClose(() => signal.removeEventListener("abort", onAbort));
      }
    }

    return stream;
  }

  /**
   * writeFrame sends now if the socket is open, otherwise buffers until onopen.
   * Writing into a defunct mux (dropped or closed) throws: every public path
   * fails fast before reaching here, so a write that does arrive is a transport
   * bug that must surface loudly, never masquerade as a successful send.
   */
  private writeFrame(bytes: Uint8Array): void {
    if (this.closed || this.dead) {
      throw this.terminalError ?? new WsStatusError(CODE_UNAVAILABLE, "connection lost");
    }
    if (this.open) {
      this.ws.send(bytes);
      return;
    }
    // Pre-open backlog: bound it so a producer that keeps sending while the
    // socket never opens fails the connection instead of growing without limit.
    if (this.sendBufferBytes + bytes.length > MAX_PREOPEN_BUFFER_BYTES) {
      this.fail(
        new WsStatusError(CODE_UNAVAILABLE, "send buffer overflow before connect"),
        1011,
        "send buffer overflow",
      );
      return;
    }
    this.sendBuffer.push(bytes);
    this.sendBufferBytes += bytes.length;
  }

  private flush(): void {
    if (this.closed) {
      return;
    }
    for (const bytes of this.sendBuffer) {
      this.ws.send(bytes);
    }
    this.sendBuffer.length = 0;
    this.sendBufferBytes = 0;
  }

  /** handleMessage decodes one inbound binary WS message and routes by stream id. */
  private handleMessage(data: unknown): void {
    const bytes = toUint8Array(data);
    if (bytes === null) {
      return; // ignore non-binary frames
    }
    // Optional inbound size cap: the browser WebSocket has no built-in limit, so
    // reject an over-large message before decoding it and fail the connection.
    if (this.maxFrameBytes > 0 && bytes.length > this.maxFrameBytes) {
      this.fail(
        new WsStatusError(CODE_RESOURCE_EXHAUSTED, "inbound frame exceeds maxFrameBytes"),
        1009,
        "message too big",
      );
      return;
    }
    let frame: ReturnType<typeof decodeFrame>;
    try {
      frame = decodeFrame(bytes);
    } catch {
      return; // undecodable frame: ignore, like an unknown Kind — never crash the mux
    }

    switch (frame.kind) {
      case Kind.KIND_OPEN:
        // Server never opens streams in this protocol; ignore.
        return;

      case Kind.KIND_HEADER: {
        // Leading response metadata (server->client only). It is not terminal
        // and does not enqueue as a MSG; it resolves responseLeadingHeaders().
        const s = this.streams.get(frame.streamId);
        if (!s) {
          return;
        }
        s.setLeadingHeaders(frame.headers);
        return;
      }

      case Kind.KIND_WINDOW_UPDATE: {
        // Flow control: the peer returns send credit for what it has consumed.
        // Non-terminal, not enqueued — credit the stream and resume its pump.
        const s = this.streams.get(frame.streamId);
        if (!s) {
          return;
        }
        s.creditSend(frame.window);
        return;
      }

      case Kind.KIND_MSG: {
        const s = this.streams.get(frame.streamId);
        if (!s) {
          return;
        }
        // Bound the per-stream backlog by BYTES (aligned with the flow-control
        // window), not by frame count: a peer that obeys the window never has
        // more than ~initialWindow unconsumed bytes, so it is never falsely
        // reset; a peer that IGNORES the window overruns the byte ceiling and is
        // reset. The check is "already over" so at least one message — even an
        // oversized one — is always admitted (mirrors the send side). abort()
        // rejects pending recv()/iteration and, via its reset hook, writes an
        // RST to the peer and detaches the stream.
        if (s.queuedBytes() > this.maxRecvBytes) {
          s.abort(new WsStatusError(CODE_RESOURCE_EXHAUSTED, "receive buffer overflow"));
          this.streams.delete(frame.streamId); // idempotent; abort already detached
          return;
        }
        s.pushMsg(frame.payload);
        return;
      }

      case Kind.KIND_END: {
        const s = this.streams.get(frame.streamId);
        this.streams.delete(frame.streamId);
        if (!s) {
          return;
        }
        const err = statusErrorFromProto(frame.status);
        if (err === null) {
          s.endOk(frame.headers);
        } else {
          s.endError(err, frame.headers);
        }
        return;
      }

      case Kind.KIND_RST: {
        const s = this.streams.get(frame.streamId);
        this.streams.delete(frame.streamId);
        if (!s) {
          return;
        }
        const err =
          statusErrorFromProto(frame.status) ??
          new WsStatusError(CODE_CANCELLED, frame.status?.message || "stream reset");
        s.endError(err, frame.headers);
        return;
      }

      default:
        // KIND_UNSPECIFIED / KIND_HALF_CLOSE inbound: not expected from server; ignore.
        return;
    }
  }

  private failAll(err: WsStatusError): void {
    for (const [id, s] of this.streams) {
      s.endError(err);
      this.streams.delete(id);
    }
  }

  /**
   * close deliberately tears down all streams and the socket; streams fail with
   * `err` (default CODE_CANCELLED "transport closed"). The owner is not notified.
   */
  close(err: WsStatusError = new WsStatusError(CODE_CANCELLED, "transport closed")): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.lifecycle = undefined;
    this.markDefunct(err);
    try {
      this.ws.close(1000, "");
    } catch {
      // closing an already-closed socket is fine
    }
  }
}

/** toUint8Array normalizes the WS `data` payload to a Uint8Array, or null if not binary. */
function toUint8Array(data: unknown): Uint8Array | null {
  if (data instanceof Uint8Array) {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }
  return null; // string / Blob — not used by this protocol
}
