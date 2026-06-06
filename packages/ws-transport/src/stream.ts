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
   */
  recv(): Promise<Uint8Array | null>;
  /** responseHeaders resolves with the headers carried on END (server trailers/metadata). */
  responseHeaders(): Promise<Record<string, string>>;
  /** Async iteration yields each response MSG payload until clean END; throws on error END/RST. */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array>;
}

/** Callbacks the mux supplies so the stream can write frames and detach itself. */
export interface StreamHooks {
  sendMsg(streamId: number, payload: Uint8Array): void;
  halfClose(streamId: number): void;
}

export class StreamImpl implements ClientStream {
  readonly id: number;
  private readonly hooks: StreamHooks;
  private readonly inbound = new AsyncQueue<Uint8Array>();

  private sendClosed = false;
  private finished = false;

  private headers: Record<string, string> = {};
  private headersResolve!: (h: Record<string, string>) => void;
  private headersReject!: (e: unknown) => void;
  private readonly headersPromise: Promise<Record<string, string>>;
  private headersSettled = false;

  constructor(id: number, hooks: StreamHooks) {
    this.id = id;
    this.hooks = hooks;
    this.headersPromise = new Promise<Record<string, string>>((resolve, reject) => {
      this.headersResolve = resolve;
      this.headersReject = reject;
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
    };
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
  }

  /** endError is called by the mux on a non-OK END or RST. Rejects pending pulls. */
  endError(err: WsStatusError, headers: Record<string, string> = {}): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.rejectHeaders(err);
    this.inbound.fail(err);
  }

  private resolveHeaders(headers: Record<string, string>): void {
    if (this.headersSettled) {
      return;
    }
    this.headersSettled = true;
    this.headers = headers;
    this.headersResolve(headers);
  }

  private rejectHeaders(err: unknown): void {
    if (this.headersSettled) {
      return;
    }
    this.headersSettled = true;
    this.headersReject(err);
  }
}
