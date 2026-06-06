import { AsyncQueue } from "./queue.js";
import { WsStatusError } from "./status.js";

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
}

export class StreamImpl implements ClientStream {
  readonly id: number;
  private readonly hooks: StreamHooks;
  private readonly inbound = new AsyncQueue<Uint8Array>();

  private sendClosed = false;
  private finished = false;

  /** Callbacks run exactly once on any terminal path (used to detach abort listeners). */
  private closeCbs: Array<() => void> = [];

  private headers: Record<string, string> = {};
  private headersResolve!: (h: Record<string, string>) => void;
  private readonly headersPromise: Promise<Record<string, string>>;
  private headersSettled = false;

  constructor(id: number, hooks: StreamHooks) {
    this.id = id;
    this.hooks = hooks;
    // headersPromise only ever resolves (with trailers); the error status is
    // delivered through recv()/iteration, so there is no rejection path.
    this.headersPromise = new Promise<Record<string, string>>((resolve) => {
      this.headersResolve = resolve;
    });
  }

  send(payload: Uint8Array): void {
    if (this.sendClosed || this.finished) {
      return;
    }
    this.hooks.sendMsg(this.id, payload);
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
    return r.value!;
  }

  responseHeaders(): Promise<Record<string, string>> {
    return this.headersPromise;
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    const self = this;
    return {
      async next(): Promise<IteratorResult<Uint8Array>> {
        const r = await self.inbound.pull();
        if (r.done) {
          return { done: true, value: undefined };
        }
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

  /** pushMsg is called by the mux for each inbound MSG frame. */
  pushMsg(payload: Uint8Array): void {
    this.inbound.push(payload);
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
    if (this.headersSettled) {
      return;
    }
    this.headersSettled = true;
    this.headers = headers;
    this.headersResolve(headers);
  }
}
