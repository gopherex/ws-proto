import { AsyncQueue } from "./queue.js";
import { WsStatusError, CODE_RESOURCE_EXHAUSTED } from "./status.js";

/**
 * ClientStream is one multiplexed RPC from the client's perspective. It is
 * UNTYPED at the payload level: send/recv move raw Uint8Array message bodies.
 * The Plan 4 generator wraps this with typed serialize/deserialize.
 */
export interface ClientStream {
  /** send writes one request MSG frame. */
  send(payload: Uint8Array): void;
  /** closeSend writes HALF_CLOSE: the client is done sending request messages. */
  closeSend(): void;
  /**
   * recv resolves with the next response MSG payload, with `null` on a clean
   * END (status OK), and rejects with a WsStatusError on a non-OK END or RST.
   * Do not call concurrently or interleave with async iteration — pending pulls
   * are served FIFO, so overlapping callers receive messages in registration
   * order rather than per-caller order.
   */
  recv(): Promise<Uint8Array | null>;
  /**
   * responseHeaders resolves with the headers/trailers carried on END (server
   * metadata), for BOTH clean and error completions; the error status itself is
   * surfaced through recv()/iteration, not here.
   */
  responseHeaders(): Promise<Record<string, string>>;
  /**
   * responseLeadingHeaders resolves with the optional leading response metadata
   * the server may send on a KIND_HEADER frame before the first message. If no
   * such frame is sent, it resolves with `{}` as soon as the first MSG or END
   * arrives, so it always settles promptly.
   */
  responseLeadingHeaders(): Promise<Record<string, string>>;
  /**
   * Async iteration yields each response MSG payload until clean END; throws on
   * error END/RST. Breaking out early (the iterator's return()) cancels the RPC
   * via RST so the stream detaches and the server stops producing.
   */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array>;
  /**
   * cancel aborts the RPC from the client: sends RST, detaches the stream from
   * the mux, and ends pending recv()/iteration. Safe to call after completion
   * (no-op). Use it to abort a streaming call whose request source failed.
   */
  cancel(): void;
}

/** Callbacks the mux supplies so the stream can write frames and detach itself. */
export interface StreamHooks {
  sendMsg(streamId: number, payload: Uint8Array): void;
  halfClose(streamId: number): void;
  /** reset writes an RST frame and detaches the stream from the mux (client cancel). */
  reset(streamId: number): void;
  /**
   * windowUpdate writes a KIND_WINDOW_UPDATE frame returning `delta` bytes of
   * receive credit to the peer (flow control). Called as the reader consumes
   * MSGs; the mux serializes it onto the socket.
   */
  windowUpdate(streamId: number, delta: number): void;
}

/** DEFAULT_INITIAL_WINDOW is the per-stream credit window (bytes) for flow control. */
export const DEFAULT_INITIAL_WINDOW = 256 * 1024; // 256 KiB

/**
 * DEFAULT_MAX_SEND_BUFFER bounds the unsent outbound backlog of a single stream.
 * It is deliberately much larger than the window: a caller may legitimately
 * enqueue many messages with send() and let the pump drain them as credit
 * arrives. This bound only catches a true runaway — a caller that keeps sending
 * while a stalled/malicious peer never returns credit.
 */
export const DEFAULT_MAX_SEND_BUFFER = 16 * 1024 * 1024; // 16 MiB

export class StreamImpl implements ClientStream {
  readonly id: number;
  private readonly hooks: StreamHooks;
  private readonly inbound = new AsyncQueue<Uint8Array>();
  // Bytes buffered in `inbound` but not yet pulled by recv()/iteration. The mux
  // bounds the backlog by THIS (not by frame count) so a peer that obeys the
  // flow-control window — and may have many tiny frames but few bytes in flight
  // — is never falsely reset.
  private bufferedBytes = 0;

  // ---- Flow control (credit windowing) ----
  // The public send() is synchronous (void): it appends to outbound and a pump
  // drains it as send-window credit allows. A MSG may be sent whenever
  // sendWindow > 0; sendWindow then decrements by payload length and is allowed
  // to go NEGATIVE, so a single message larger than the whole window is still
  // delivered (matching the Go side / gRPC-HTTP2 semantics) and never deadlocks.
  // Credit returns when the peer sends a KIND_WINDOW_UPDATE (creditSend).
  private readonly initialWindow: number;
  private sendWindow: number;
  private readonly outbound: Uint8Array[] = [];
  // Bytes buffered in `outbound` awaiting send-window credit. Bounded by
  // maxSendBuffer so a caller that floods send() while the window is closed
  // (peer not crediting) aborts its own stream instead of growing without limit.
  private outboundBytes = 0;
  private readonly maxSendBuffer: number;
  // (maxSendBuffer is supplied by the mux; defaults to DEFAULT_MAX_SEND_BUFFER.)
  // Receiver side: bytes consumed by recv()/iteration but not yet returned to the
  // peer; flushed as one KIND_WINDOW_UPDATE once it crosses initialWindow/2.
  private pendingCredit = 0;

  private sendClosed = false;
  private finished = false;

  /** Callbacks run exactly once on any terminal path (used to detach abort listeners). */
  private closeCbs: Array<() => void> = [];

  private headers: Record<string, string> = {};
  private headersResolve!: (h: Record<string, string>) => void;
  private readonly headersPromise: Promise<Record<string, string>>;
  private headersSettled = false;

  private leadingResolve!: (h: Record<string, string>) => void;
  private readonly leadingPromise: Promise<Record<string, string>>;
  private leadingSettled = false;

  constructor(
    id: number,
    hooks: StreamHooks,
    initialWindow: number = DEFAULT_INITIAL_WINDOW,
    maxSendBuffer: number = DEFAULT_MAX_SEND_BUFFER,
  ) {
    this.id = id;
    this.hooks = hooks;
    this.initialWindow = initialWindow > 0 ? initialWindow : DEFAULT_INITIAL_WINDOW;
    this.sendWindow = this.initialWindow;
    this.maxSendBuffer = maxSendBuffer > 0 ? maxSendBuffer : DEFAULT_MAX_SEND_BUFFER;
    // headersPromise only ever resolves (with trailers); the error status is
    // delivered through recv()/iteration, so there is no rejection path.
    this.headersPromise = new Promise<Record<string, string>>((resolve) => {
      this.headersResolve = resolve;
    });
    // leadingPromise resolves with the KIND_HEADER metadata, or with {} once the
    // first MSG/END arrives if the server never sent a leading header.
    this.leadingPromise = new Promise<Record<string, string>>((resolve) => {
      this.leadingResolve = resolve;
    });
  }

  send(payload: Uint8Array): void {
    if (this.sendClosed || this.finished) {
      return;
    }
    // Bound the outbound backlog: a caller that keeps sending while the window
    // is closed (no credit from the peer) aborts its own stream rather than
    // buffering without limit. The check is "already over" so at least one
    // message — even one larger than the window — is always queued.
    if (this.outboundBytes > this.maxSendBuffer) {
      this.abort(new WsStatusError(CODE_RESOURCE_EXHAUSTED, "send buffer overflow"));
      return;
    }
    // Buffer and let the pump flush as the send window allows. Keeps the public
    // signature synchronous while honoring per-stream backpressure.
    this.outbound.push(payload);
    this.outboundBytes += payload.length;
    this.pump();
  }

  /**
   * pump drains buffered outbound MSGs while the send window permits. A MSG is
   * sent whenever sendWindow > 0; the window is then decremented by the payload
   * length and may go negative (so an oversized message is still sent once, then
   * later sends wait for credit). Re-invoked by creditSend when credit arrives.
   */
  private pump(): void {
    while (this.outbound.length > 0 && this.sendWindow > 0 && !this.finished) {
      const payload = this.outbound.shift()!;
      this.outboundBytes -= payload.length;
      this.sendWindow -= payload.length;
      this.hooks.sendMsg(this.id, payload);
    }
  }

  /**
   * creditSend adds `delta` bytes of send credit (an inbound KIND_WINDOW_UPDATE)
   * and resumes the pump so any buffered sends that were waiting can proceed.
   */
  creditSend(delta: number): void {
    if (delta <= 0) {
      return;
    }
    this.sendWindow += delta;
    this.pump();
  }

  closeSend(): void {
    if (this.sendClosed || this.finished) {
      return;
    }
    this.sendClosed = true;
    this.hooks.halfClose(this.id);
  }

  async recv(): Promise<Uint8Array | null> {
    const r = await this.inbound.pull();
    if (r.done) {
      return null;
    }
    this.bufferedBytes -= r.value!.length;
    this.returnCredit(r.value!.length);
    return r.value!;
  }

  /**
   * returnCredit accumulates consumed bytes and, once the pending total crosses
   * initialWindow/2, posts one coalesced KIND_WINDOW_UPDATE to the peer so its
   * blocked sender regains credit. Credit is returned on CONSUMPTION (here), not
   * on arrival — that is what makes the window real backpressure.
   */
  private returnCredit(n: number): void {
    if (n <= 0 || this.finished) {
      return;
    }
    this.pendingCredit += n;
    const threshold = Math.max(1, Math.floor(this.initialWindow / 2));
    if (this.pendingCredit >= threshold) {
      const delta = this.pendingCredit;
      this.pendingCredit = 0;
      this.hooks.windowUpdate(this.id, delta);
    }
  }

  responseHeaders(): Promise<Record<string, string>> {
    return this.headersPromise;
  }

  responseLeadingHeaders(): Promise<Record<string, string>> {
    return this.leadingPromise;
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    const self = this;
    return {
      async next(): Promise<IteratorResult<Uint8Array>> {
        const r = await self.inbound.pull();
        if (r.done) {
          return { done: true, value: undefined };
        }
        self.bufferedBytes -= r.value!.length;
        self.returnCredit(r.value!.length);
        return { done: false, value: r.value! };
      },
      async return(): Promise<IteratorResult<Uint8Array>> {
        // Early break/throw out of `for await`: cancel the RPC so the stream is
        // detached from the mux and the server stops producing into a queue
        // nobody drains.
        self.cancel();
        return { done: true, value: undefined };
      },
    };
  }

  /** cancel aborts the RPC from the client: sends RST, detaches, ends iteration. */
  cancel(): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.sendClosed = true;
    this.hooks.reset(this.id);
    this.resolveHeaders(this.headers);
    this.inbound.end();
    this.runClose();
  }

  /**
   * onClose registers a callback invoked once the stream reaches any terminal
   * state (clean END, error END/RST, cancel, or abort). If the stream is
   * already finished the callback runs immediately. Used to detach the abort
   * listener so it never leaks past stream completion.
   */
  onClose(cb: () => void): void {
    if (this.finished) {
      cb();
      return;
    }
    this.closeCbs.push(cb);
  }

  /** runClose invokes and clears the registered close callbacks (idempotent). */
  private runClose(): void {
    const cbs = this.closeCbs;
    this.closeCbs = [];
    for (const cb of cbs) {
      cb();
    }
  }

  /**
   * abort terminates the RPC due to an AbortSignal: sends RST and detaches (like
   * cancel), resolves trailers, and rejects pending recv()/iteration with the
   * abort error. No-op if the stream already finished.
   */
  abort(err: WsStatusError): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.sendClosed = true;
    this.hooks.reset(this.id);
    this.resolveHeaders(this.headers);
    this.inbound.fail(err);
    this.runClose();
  }

  // ---- mux-facing callbacks (not part of the public ClientStream surface) ----

  /**
   * setLeadingHeaders is called by the mux for an inbound KIND_HEADER frame
   * (leading response metadata). It resolves responseLeadingHeaders() once.
   */
  setLeadingHeaders(headers: Record<string, string>): void {
    this.resolveLeading(headers);
  }

  /** pushMsg is called by the mux for each inbound MSG frame. */
  pushMsg(payload: Uint8Array): void {
    // The first message guarantees no leading header is coming; settle to {}.
    this.resolveLeading({});
    this.bufferedBytes += payload.length;
    this.inbound.push(payload);
  }

  /**
   * queuedBytes reports how many inbound MSG bytes are buffered awaiting a
   * recv(). The mux uses it to enforce the byte-bounded receive backlog.
   */
  queuedBytes(): number {
    return this.bufferedBytes;
  }

  /** endOk is called by the mux on a clean END (status OK). Resolves recv()/iter to done. */
  endOk(headers: Record<string, string>): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.resolveHeaders(headers);
    this.inbound.end();
    this.runClose();
  }

  /**
   * endError is called by the mux on a non-OK END or RST. Trailers (if the
   * frame carried any) still resolve responseHeaders(); the status surfaces to
   * the caller through the failed recv()/iteration pulls.
   */
  endError(err: WsStatusError, headers: Record<string, string> = {}): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.resolveHeaders(headers);
    this.inbound.fail(err);
    this.runClose();
  }

  private resolveHeaders(headers: Record<string, string>): void {
    // Any terminal path also guarantees no (further) leading header is coming.
    this.resolveLeading({});
    if (this.headersSettled) {
      return;
    }
    this.headersSettled = true;
    this.headers = headers;
    this.headersResolve(headers);
  }

  private resolveLeading(headers: Record<string, string>): void {
    if (this.leadingSettled) {
      return;
    }
    this.leadingSettled = true;
    this.leadingResolve(headers);
  }
}
