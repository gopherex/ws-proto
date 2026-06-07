import { encodeFrame, decodeFrame, Kind } from "./frame.js";
import { StreamImpl } from "./stream.js";
import {
  WsStatusError,
  statusErrorFromProto,
  CODE_CANCELLED,
  CODE_DEADLINE_EXCEEDED,
  abortError,
} from "./status.js";

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
 * Mux owns one WebSocketLike and multiplexes many streams over it. Client only:
 * it allocates monotonic ODD stream ids (1, 3, 5, ...) and never reacts to OPEN.
 */
export class Mux {
  private readonly ws: WebSocketLike;
  private readonly streams = new Map<number, StreamImpl>();
  private nextId = 1;

  /** Frames produced before the socket reaches OPEN are buffered here. */
  private readonly sendBuffer: Uint8Array[] = [];
  private open = false;
  private closed = false;

  constructor(ws: WebSocketLike) {
    this.ws = ws;
    this.ws.binaryType = "arraybuffer";

    // If the socket is already open (e.g. fromSocket on a live socket), flush.
    if ((this.ws as { readyState?: number }).readyState === WS_OPEN) {
      this.open = true;
    }

    this.ws.onopen = () => {
      this.open = true;
      this.flush();
    };
    this.ws.onmessage = (ev) => this.handleMessage(ev.data);
    this.ws.onclose = () => this.failAll(new WsStatusError(CODE_CANCELLED, "websocket closed"));
    this.ws.onerror = () => this.failAll(new WsStatusError(CODE_CANCELLED, "websocket error"));
  }

  /** openStream allocates an odd stream id, registers the stream, and sends OPEN. */
  openStream(method: string, init?: StreamInit): StreamImpl {
    const id = this.nextId;
    this.nextId += 2;

    const stream = new StreamImpl(id, {
      sendMsg: (sid, payload) =>
        this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_MSG, payload })),
      halfClose: (sid) => this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_HALF_CLOSE })),
      reset: (sid) => {
        this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_RST }));
        this.streams.delete(sid);
      },
    });
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

  /** writeFrame sends now if the socket is open, otherwise buffers until onopen. */
  private writeFrame(bytes: Uint8Array): void {
    if (this.closed) {
      return;
    }
    if (this.open) {
      this.ws.send(bytes);
      return;
    }
    this.sendBuffer.push(bytes);
  }

  private flush(): void {
    if (this.closed) {
      return;
    }
    for (const bytes of this.sendBuffer) {
      this.ws.send(bytes);
    }
    this.sendBuffer.length = 0;
  }

  /** handleMessage decodes one inbound binary WS message and routes by stream id. */
  private handleMessage(data: unknown): void {
    const bytes = toUint8Array(data);
    if (bytes === null) {
      return; // ignore non-binary frames
    }
    const frame = decodeFrame(bytes);

    switch (frame.kind) {
      case Kind.KIND_OPEN:
        // Server never opens streams in this protocol; ignore.
        return;

      case Kind.KIND_MSG: {
        const s = this.streams.get(frame.streamId);
        if (s) {
          s.pushMsg(frame.payload);
        }
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

  /** close tears down all streams and the socket. */
  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.failAll(new WsStatusError(CODE_CANCELLED, "transport closed"));
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
