import type { WebSocketLike } from "./mux.js";
import { decodeFrame, encodeFrame, Kind } from "./frame.js";
import type { Frame, FrameInit } from "./frame.js";

/**
 * FakeSocket is a WebSocketLike for tests. It does no networking; instead it:
 *   - captures every binary message the client sends (decoded to a Frame), and
 *   - lets the test inject inbound Frames as if from a server.
 *
 * By default it auto-opens on the next microtask so client writes flush; pass
 * { autoOpen: false } and call open() manually to exercise pre-open buffering.
 */
export class FakeSocket implements WebSocketLike {
  binaryType = "arraybuffer";

  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;

  /** Raw bytes the client has sent (in order). */
  readonly sentBytes: Uint8Array[] = [];
  /** Decoded Frames the client has sent (in order). */
  readonly sent: Frame[] = [];

  closedCode: number | undefined;
  closedReason: string | undefined;
  private isClosed = false;

  constructor(opts: { autoOpen?: boolean } = {}) {
    if (opts.autoOpen !== false) {
      // Open after the current synchronous setup so onopen is registered.
      queueMicrotask(() => this.open());
    }
  }

  send(data: ArrayBufferView | ArrayBuffer): void {
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    // Copy so later buffer reuse cannot mutate captured frames.
    const copy = bytes.slice();
    this.sentBytes.push(copy);
    this.sent.push(decodeFrame(copy));
  }

  close(code?: number, reason?: string): void {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    this.closedCode = code;
    this.closedReason = reason;
    this.onclose?.({ code, reason });
  }

  // ---- test-side controls ----

  /** open transitions the socket to OPEN, triggering the mux to flush buffered sends. */
  open(): void {
    this.onopen?.({});
  }

  /** inject delivers a server-built Frame to the client mux. */
  inject(init: FrameInit): void {
    const bytes = encodeFrame(init);
    this.onmessage?.({ data: bytes });
  }

  /** error simulates a socket error (mux fails all streams). */
  error(): void {
    this.onerror?.({});
  }

  /** lastSent returns the most recently sent Frame, or undefined. */
  lastSent(): Frame | undefined {
    return this.sent[this.sent.length - 1];
  }
}

// Re-export Kind so test files can import it from one place.
export { Kind };
