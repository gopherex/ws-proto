import { toBinary, fromBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { Mux, SUBPROTOCOL, failedStream } from "./mux.js";
import type { WebSocketLike, StreamInit } from "./mux.js";
import { WsStatusError, CODE_CANCELLED, CODE_UNAVAILABLE } from "./status.js";
import type { ClientStream } from "./stream.js";
import { applyInterceptors, route } from "./interceptor.js";
import type {
  Interceptor,
  MethodInfo,
  UnaryRequest,
  UnaryResponse,
  StreamRequest,
  StreamResponse,
} from "./interceptor.js";

export type { WebSocketLike, StreamInit };

/** CallShape carries the per-call options the generated client forwards. */
interface CallShape {
  headers?: Record<string, string>;
  signal?: AbortSignal;
  timeoutMs?: number;
}

/**
 * SUBPROTOCOL is the WebSocket subprotocol token offered during the RFC 6455
 * handshake (`Sec-WebSocket-Protocol: wsrpc.v1`). The server must echo this
 * token; the client rejects a connection that negotiated anything else. Defined
 * in mux.ts and re-exported here for the public API surface.
 */
export { SUBPROTOCOL };

/**
 * ConnectionState is the observable lifecycle of a transport's connection:
 *   - paused: dialing is suspended (pause() or the `paused` option) and no
 *     socket is live; resume() dials.
 *   - waiting: a redial is scheduled at `retryAt` (epoch ms, Date.now clock).
 *   - connecting: a socket is being dialed (WebSocket handshake in progress).
 *   - connected: the socket is open and carrying frames.
 *   - disconnected: the connection died and no redial is scheduled (reconnect
 *     off, or a permanent failure such as a subprotocol mismatch or the server
 *     ending the connection with a status); `error` is the cause — for a server
 *     end, its status with code, message and details. retryNow() dials again.
 *   - closed: close() was called; terminal.
 */
export type ConnectionState =
  | { readonly phase: "paused" }
  | { readonly phase: "waiting"; readonly retryAt: number }
  | { readonly phase: "connecting" }
  | { readonly phase: "connected" }
  | { readonly phase: "disconnected"; readonly error: WsStatusError }
  | { readonly phase: "closed" };

/** Reconnect backoff defaults (full jitter applied on top). */
const DEFAULT_RECONNECT_INITIAL_MS = 100;
const DEFAULT_RECONNECT_MAX_MS = 30_000;
const RECONNECT_FACTOR = 2;

/**
 * WsTransportOptions tunes a transport instance.
 *
 * maxReceiveQueue is DEPRECATED and ignored. The per-stream inbound backlog is
 * now bounded by BYTES, aligned with initialWindow, rather than by a fixed frame
 * count — so a peer that obeys the flow-control window is never falsely reset.
 *
 * reconnect (opt-in) makes the transport redial on connection loss with
 * exponential backoff + jitter. In-flight streams still fail with
 * CODE_UNAVAILABLE ("connection lost") and must be retried by the caller — there
 * is no stream resume. Without it the transport behaves as before: on loss
 * in-flight streams fail and the socket stays closed. Only WsTransport (which
 * owns dialing) reconnects; fromSocket() never reconnects.
 *
 * createSocket is a test seam: it overrides how the transport opens a socket
 * (defaults to `new WebSocket(url, SUBPROTOCOL)`), letting tests inject a fake.
 */
export interface WsTransportOptions {
  maxReceiveQueue?: number;
  /**
   * initialWindow is the per-stream flow-control credit window in bytes
   * (default 256*1024). A sender's buffered send() drains only as the peer
   * returns credit (KIND_WINDOW_UPDATE) by consuming MSGs, so a well-behaved
   * sender never overruns a slow receiver. Both peers must assume the same
   * value (no handshake). maxReceiveQueue remains a backstop for peers that
   * ignore the window.
   */
  initialWindow?: number;
  /**
   * maxSendBuffer bounds the unsent outbound backlog of each stream in bytes
   * (default 16 MiB). A stream that buffers more than this via send() while the
   * peer withholds flow-control credit is aborted (CODE_RESOURCE_EXHAUSTED)
   * rather than growing without limit. Much larger than the window: normal
   * buffering of many queued messages is unaffected.
   */
  maxSendBuffer?: number;
  /**
   * maxFrameBytes optionally caps the size of an inbound WebSocket message in
   * bytes. A message larger than this fails the connection (the browser
   * WebSocket has no built-in size limit, so without this a hostile server could
   * push an enormous frame and exhaust memory). Default: unset (no cap).
   */
  maxFrameBytes?: number;
  /**
   * interceptors are applied transport-wide to every typed call made through
   * unary()/stream() (and thus through generated clients). The first interceptor
   * is the outermost. They can read/modify request metadata and messages,
   * observe/transform responses, short-circuit, and read trailers.
   */
  interceptors?: Interceptor[];
  reconnect?: boolean;
  /**
   * backoff tunes the redial schedule: the n-th consecutive failed attempt
   * waits a random delay in [0, min(initialMs * 2^n, maxMs)] (full jitter).
   * stableMs: a connection must stay open this long before the attempt counter
   * resets, so a peer that accepts and immediately drops (a proxy with no live
   * backend) keeps backing off instead of being hammered. Default 0: every
   * successful open resets the counter.
   */
  backoff?: { initialMs?: number; maxMs?: number; stableMs?: number };
  /**
   * dialTimeoutMs aborts a socket that has not opened within this many ms
   * (hung WebSocket handshake): its buffered calls fail with CODE_UNAVAILABLE
   * "dial timeout" and, with reconnect, a redial is scheduled. Applies to every
   * dial. Default: unset (no timeout; the browser's own handshake timeout).
   */
  dialTimeoutMs?: number;
  /**
   * paused constructs the transport without dialing; the first dial happens on
   * resume(). Calls made meanwhile fail fast with CODE_UNAVAILABLE.
   */
  paused?: boolean;
  createSocket?: (url: string) => WebSocketLike;
}

/**
 * WsTransport dials (or wraps) one WebSocket and multiplexes RPC streams over
 * it. It is the untyped analog of connect-web's transport; Plan 4's generator
 * wraps openStream() with typed clients.
 */
export class WsTransport {
  private mux?: Mux;
  private readonly maxReceiveQueue?: number;
  private readonly initialWindow?: number;
  private readonly maxSendBuffer?: number;
  private readonly maxFrameBytes?: number;
  private readonly interceptors?: Interceptor[];

  // Dialing machinery: active only when the transport owns dialing (url set,
  // i.e. not via fromSocket). Auto-redial additionally needs reconnectEnabled.
  private readonly reconnectEnabled: boolean;
  private readonly url?: string;
  private readonly createSocket: (url: string) => WebSocketLike;
  private readonly initialMs: number;
  private readonly maxMs: number;
  private readonly stableMs: number;
  private readonly dialTimeoutMs: number;
  private attempt = 0;
  private paused = false;
  private closed = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private dialTimer: ReturnType<typeof setTimeout> | undefined;
  private stableTimer: ReturnType<typeof setTimeout> | undefined;
  private current: ConnectionState = { phase: "connecting" };
  private readonly listeners = new Set<(state: ConnectionState) => void>();

  /** Constructs a transport by dialing a new browser-native WebSocket to `url`. */
  constructor(url: string, options?: WsTransportOptions);
  /** Internal: construct from an existing WebSocketLike (used by fromSocket). */
  constructor(socket: WebSocketLike, fromExisting: true, options?: WsTransportOptions);
  constructor(
    urlOrSocket: string | WebSocketLike,
    fromExisting?: true | WsTransportOptions,
    options?: WsTransportOptions,
  ) {
    const opts: WsTransportOptions | undefined =
      fromExisting === true ? options : (fromExisting as WsTransportOptions | undefined);

    this.maxReceiveQueue = opts?.maxReceiveQueue;
    this.initialWindow = opts?.initialWindow;
    this.maxSendBuffer = opts?.maxSendBuffer;
    this.maxFrameBytes = opts?.maxFrameBytes;
    this.interceptors = opts?.interceptors;
    this.createSocket =
      opts?.createSocket ??
      ((u: string) => new WebSocket(u, SUBPROTOCOL) as unknown as WebSocketLike);
    this.initialMs =
      opts?.backoff?.initialMs && opts.backoff.initialMs > 0
        ? opts.backoff.initialMs
        : DEFAULT_RECONNECT_INITIAL_MS;
    this.maxMs =
      opts?.backoff?.maxMs && opts.backoff.maxMs > 0 ? opts.backoff.maxMs : DEFAULT_RECONNECT_MAX_MS;
    this.stableMs = Math.max(0, opts?.backoff?.stableMs ?? 0);
    this.dialTimeoutMs = Math.max(0, opts?.dialTimeoutMs ?? 0);

    if (fromExisting === true) {
      // fromSocket: wrap the given socket and never dial (it doesn't own it).
      this.reconnectEnabled = false;
      this.attach(urlOrSocket as WebSocketLike);
      return;
    }

    // Dialing constructor: the transport owns the socket and may reconnect.
    this.url = urlOrSocket as string;
    this.reconnectEnabled = opts?.reconnect === true;
    if (opts?.paused === true) {
      this.paused = true;
      this.current = { phase: "paused" };
      return;
    }
    this.dial();
  }

  /** fromSocket wraps an already-created WebSocketLike (browser socket or a test fake). */
  static fromSocket(ws: WebSocketLike, options?: WsTransportOptions): WsTransport {
    return new WsTransport(ws, true, options);
  }

  /** state is the current connection state. */
  get state(): ConnectionState {
    return this.current;
  }

  /**
   * subscribeState calls `listener` with the current state now and on every
   * change; returns the unsubscribe function. A throwing listener is isolated:
   * its error is rethrown asynchronously and other listeners still run.
   */
  subscribeState(listener: (state: ConnectionState) => void): () => void {
    this.listeners.add(listener);
    this.notify(listener, this.current);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * pause suspends dialing (app backgrounded, offline, signed out): a scheduled
   * redial is cancelled and a dial still in its handshake is aborted (its
   * buffered calls fail with CODE_UNAVAILABLE). An open socket is left alone;
   * when it later drops, the transport parks in "paused" instead of redialing.
   * No-op for fromSocket transports.
   */
  pause(): void {
    if (this.closed || this.url === undefined || this.paused) {
      return;
    }
    this.paused = true;
    this.clearTimer("retryTimer");
    if (this.current.phase === "waiting") {
      this.setState({ phase: "paused" });
    } else if (this.current.phase === "connecting") {
      this.clearTimer("dialTimer");
      this.mux?.close(new WsStatusError(CODE_UNAVAILABLE, "dial paused"));
      this.setState({ phase: "paused" });
    }
  }

  /**
   * resume re-enables dialing after pause() (or the `paused` option) with a
   * fresh backoff; with no live socket it dials at once.
   */
  resume(): void {
    if (this.closed || !this.paused) {
      return;
    }
    this.paused = false;
    this.attempt = 0;
    if (this.current.phase === "paused") {
      this.dial();
    }
  }

  /**
   * retryNow dials immediately instead of waiting out the backoff (user taps
   * "retry", network came back). Acts only in "waiting" and "disconnected";
   * a no-op while paused, dialing, connected, closed, or for fromSocket.
   */
  retryNow(): void {
    if (this.closed || this.paused || this.url === undefined) {
      return;
    }
    if (this.current.phase === "waiting" || this.current.phase === "disconnected") {
      this.dial();
    }
  }

  /** dial opens a fresh socket+mux and arms the dial timeout. */
  private dial(): void {
    this.clearTimer("retryTimer");
    this.attach(this.createSocket(this.url as string));
    if (this.current.phase === "connecting" && this.dialTimeoutMs > 0) {
      const mux = this.mux;
      this.dialTimer = setTimeout(() => {
        this.dialTimer = undefined;
        const err = new WsStatusError(CODE_UNAVAILABLE, "dial timeout");
        mux?.close(err);
        this.afterDrop(err, true);
      }, this.dialTimeoutMs);
    }
  }

  /** attach installs a mux over `ws` and follows its lifecycle. */
  private attach(ws: WebSocketLike): void {
    const mux = new Mux(
      ws,
      this.maxReceiveQueue,
      this.initialWindow,
      this.maxSendBuffer,
      this.maxFrameBytes,
    );
    this.mux = mux;
    mux.setLifecycle({
      open: () => {
        if (mux === this.mux) {
          this.onOpen();
        }
      },
      drop: (err, redial) => {
        if (mux === this.mux) {
          this.afterDrop(err, redial);
        }
      },
    });
    if (mux.isOpen) {
      this.onOpen();
    } else {
      this.setState({ phase: "connecting" });
    }
  }

  /** onOpen marks the connection live and arms the stable-connection backoff reset. */
  private onOpen(): void {
    this.clearTimer("dialTimer");
    if (this.stableMs === 0) {
      this.attempt = 0;
    } else {
      this.stableTimer = setTimeout(() => {
        this.stableTimer = undefined;
        this.attempt = 0;
      }, this.stableMs);
    }
    this.setState({ phase: "connected" });
  }

  /**
   * afterDrop decides what follows a dead connection: a jittered redial, a
   * park in "paused", or "disconnected" when no redial applies. New openStream
   * calls in the backoff gap fail fast on the dead mux; once the replacement is
   * being dialed they buffer until its socket opens.
   */
  private afterDrop(err: WsStatusError, redial: boolean): void {
    this.clearTimer("dialTimer");
    this.clearTimer("stableTimer");
    if (!redial || !this.reconnectEnabled) {
      this.setState({ phase: "disconnected", error: err });
      return;
    }
    if (this.paused) {
      this.setState({ phase: "paused" });
      return;
    }
    const delay = this.nextBackoff(this.attempt);
    this.attempt += 1;
    this.retryTimer = setTimeout(() => this.dial(), delay);
    this.setState({ phase: "waiting", retryAt: Date.now() + delay });
  }

  /** nextBackoff returns initial*factor^attempt capped at maxMs, with full jitter. */
  private nextBackoff(attempt: number): number {
    let d = this.initialMs;
    for (let i = 0; i < attempt && d < this.maxMs; i++) {
      d *= RECONNECT_FACTOR;
    }
    if (d > this.maxMs) {
      d = this.maxMs;
    }
    // Full jitter: a random delay in [0, d].
    return Math.floor(Math.random() * (d + 1));
  }

  private clearTimer(name: "retryTimer" | "dialTimer" | "stableTimer"): void {
    const timer = this[name];
    if (timer !== undefined) {
      clearTimeout(timer);
      this[name] = undefined;
    }
  }

  private setState(state: ConnectionState): void {
    this.current = state;
    for (const listener of [...this.listeners]) {
      this.notify(listener, state);
    }
  }

  private notify(listener: (state: ConnectionState) => void, state: ConnectionState): void {
    try {
      listener(state);
    } catch (err) {
      queueMicrotask(() => {
        throw err;
      });
    }
  }

  /** openStream begins a new RPC: sends OPEN(method, init.headers) and returns the stream. */
  openStream(method: string, init?: StreamInit): ClientStream {
    if (this.mux === undefined) {
      // Paused before the first dial: there is no connection to carry the call.
      return failedStream(
        1,
        this.closed
          ? new WsStatusError(CODE_CANCELLED, "transport closed")
          : new WsStatusError(CODE_UNAVAILABLE, "not connected"),
        this.initialWindow,
        this.maxSendBuffer,
      );
    }
    return this.mux.openStream(method, init);
  }

  /**
   * unary dispatches a typed unary RPC through the interceptor chain. The
   * terminal serializes the request, opens a stream, reads the single response,
   * and observes the terminal status (a trailing error END is surfaced, not
   * swallowed). Generated clients call this; callers rarely call it directly.
   */
  async unary<I extends Message, O extends Message>(
    method: MethodInfo<I, O>,
    message: I,
    options?: CallShape,
  ): Promise<UnaryResponse<I, O>> {
    const req: UnaryRequest<I, O> = {
      stream: false,
      method,
      header: { ...options?.headers },
      message,
      signal: options?.signal,
      timeoutMs: options?.timeoutMs,
    };
    const chain = applyInterceptors((r) => this.unaryTerminal(r as UnaryRequest), this.interceptors);
    return (await chain(req)) as UnaryResponse<I, O>;
  }

  /**
   * stream dispatches a typed streaming RPC (server-, client-, or bidi-) through
   * the interceptor chain. The request is an AsyncIterable of typed messages;
   * the response exposes an AsyncIterable of typed messages plus header/trailer.
   */
  async stream<I extends Message, O extends Message>(
    method: MethodInfo<I, O>,
    input: AsyncIterable<I>,
    options?: CallShape,
  ): Promise<StreamResponse<I, O>> {
    const req: StreamRequest<I, O> = {
      stream: true,
      method,
      header: { ...options?.headers },
      message: input,
      signal: options?.signal,
      timeoutMs: options?.timeoutMs,
    };
    const chain = applyInterceptors((r) => this.streamTerminal(r as StreamRequest), this.interceptors);
    return (await chain(req)) as StreamResponse<I, O>;
  }

  /** unaryTerminal performs the actual unary I/O (innermost of the chain). */
  private async unaryTerminal(req: UnaryRequest): Promise<UnaryResponse> {
    const m = req.method;
    const stream = this.openStream(route(m), {
      headers: req.header,
      signal: req.signal,
      timeoutMs: req.timeoutMs,
    });
    stream.send(toBinary(m.input, req.message));
    stream.closeSend();
    const bytes = await stream.recv();
    if (bytes === null) {
      throw new Error(`${m.typeName}.${m.name}: server closed stream without a response`);
    }
    const message = fromBinary(m.output, bytes);
    // Observe the terminal status: a trailing error END after the message must
    // surface (this recv() rejects) rather than being swallowed.
    const tail = await stream.recv();
    if (tail !== null) {
      throw new Error(`${m.typeName}.${m.name}: server sent more than one response message`);
    }
    const header = await stream.responseLeadingHeaders();
    const trailer = await stream.responseHeaders();
    return { stream: false, method: m, header, message, trailer };
  }

  /** streamTerminal performs the actual streaming I/O (innermost of the chain). */
  private async streamTerminal(req: StreamRequest): Promise<StreamResponse> {
    const m = req.method;
    const stream = this.openStream(route(m), {
      headers: req.header,
      signal: req.signal,
      timeoutMs: req.timeoutMs,
    });
    // Pump request messages concurrently through an explicit iterator so the
    // terminal path can finalize the caller's source with return() instead of
    // waiting for it: the source is not under transport control and may be
    // infinite/idle (e.g. a long-lived bidi sync channel), so the pump must
    // never gate stream teardown. On source failure, cancel so the read side
    // ends promptly; the captured error is re-raised after iteration.
    const source = req.message[Symbol.asyncIterator]();
    let pumpError: unknown;
    void (async () => {
      try {
        for (;;) {
          const r = await source.next();
          if (r.done) {
            break;
          }
          stream.send(toBinary(m.input, r.value));
        }
        stream.closeSend();
      } catch (err) {
        pumpError = err;
        stream.cancel();
      }
    })();

    // Metadata fills in as it arrives rather than blocking the response object:
    // leading header on the first frame, trailer once the stream ends.
    const header: Record<string, string> = {};
    void stream.responseLeadingHeaders().then((h) => Object.assign(header, h));
    const trailer: Record<string, string> = {};

    async function* responseGen(): AsyncIterable<Message> {
      let readFailed = false;
      try {
        for await (const frameBytes of stream) {
          yield fromBinary(m.output, frameBytes);
        }
      } catch (err) {
        readFailed = true;
        throw err;
      } finally {
        // Best-effort source finalization, never awaited: with a next() still
        // pending, return() is queued by the generator protocol and can only
        // run once that await settles — which may be never for an idle source.
        try {
          void Promise.resolve(source.return?.()).catch(() => {});
        } catch {
          // a hostile iterator throwing from return() must not mask teardown
        }
        // Bounded: responseHeaders() settles on every terminal path
        // (endOk / endError / cancel / abort).
        Object.assign(trailer, await stream.responseHeaders());
        // The read-side error is the root cause and takes priority; a source
        // error surfaces only when the read side ended cleanly.
        if (!readFailed && pumpError !== undefined) {
          throw pumpError;
        }
      }
    }

    return { stream: true, method: m, header, message: responseGen(), trailer };
  }

  /** close tears down all in-flight streams, stops reconnection, and closes the socket. */
  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.clearTimer("retryTimer");
    this.clearTimer("dialTimer");
    this.clearTimer("stableTimer");
    this.mux?.close();
    this.setState({ phase: "closed" });
  }
}
