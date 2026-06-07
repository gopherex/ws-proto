import { Mux } from "./mux.js";
import type { WebSocketLike, StreamInit } from "./mux.js";
import type { ClientStream } from "./stream.js";

export type { WebSocketLike, StreamInit };

/**
 * SUBPROTOCOL is the WebSocket subprotocol token offered during the RFC 6455
 * handshake (`Sec-WebSocket-Protocol: wsrpc.v1`). The server selects this token
 * to confirm it speaks the same framing protocol, and intermediary proxies can
 * use it for traffic identification and routing.
 */
export const SUBPROTOCOL = "wsrpc.v1";

/** Reconnect backoff defaults (full jitter applied on top). */
const DEFAULT_RECONNECT_INITIAL_MS = 100;
const DEFAULT_RECONNECT_MAX_MS = 30_000;
const RECONNECT_FACTOR = 2;

/**
 * WsTransportOptions tunes a transport instance.
 *
 * maxReceiveQueue bounds the per-stream inbound MSG backlog. A consumer too slow
 * to keep its queue under this bound has its stream reset
 * (CODE_RESOURCE_EXHAUSTED) instead of buffering without limit. Defaults to 256.
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
  reconnect?: boolean;
  backoff?: { initialMs?: number; maxMs?: number };
  createSocket?: (url: string) => WebSocketLike;
}

/**
 * WsTransport dials (or wraps) one WebSocket and multiplexes RPC streams over
 * it. It is the untyped analog of connect-web's transport; Plan 4's generator
 * wraps openStream() with typed clients.
 */
export class WsTransport {
  private mux: Mux;
  private readonly maxReceiveQueue?: number;
  private readonly initialWindow?: number;

  // Reconnect machinery (active only when reconnect is enabled and the transport
  // owns dialing, i.e. not via fromSocket).
  private readonly reconnectEnabled: boolean;
  private readonly url?: string;
  private readonly createSocket: (url: string) => WebSocketLike;
  private readonly initialMs: number;
  private readonly maxMs: number;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private closed = false;

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
    this.createSocket =
      opts?.createSocket ??
      ((u: string) => new WebSocket(u, SUBPROTOCOL) as unknown as WebSocketLike);
    this.initialMs =
      opts?.backoff?.initialMs && opts.backoff.initialMs > 0
        ? opts.backoff.initialMs
        : DEFAULT_RECONNECT_INITIAL_MS;
    this.maxMs =
      opts?.backoff?.maxMs && opts.backoff.maxMs > 0 ? opts.backoff.maxMs : DEFAULT_RECONNECT_MAX_MS;

    if (fromExisting === true) {
      // fromSocket: wrap the given socket and never reconnect (it doesn't own it).
      this.reconnectEnabled = false;
      this.mux = new Mux(urlOrSocket as WebSocketLike, this.maxReceiveQueue, this.initialWindow);
      return;
    }

    // Dialing constructor: the transport owns the socket and may reconnect.
    this.url = urlOrSocket as string;
    this.reconnectEnabled = opts?.reconnect === true;
    this.mux = this.connect();
  }

  /** fromSocket wraps an already-created WebSocketLike (browser socket or a test fake). */
  static fromSocket(ws: WebSocketLike, options?: WsTransportOptions): WsTransport {
    return new WsTransport(ws, true, options);
  }

  /** connect opens a fresh socket+mux and, when reconnect is on, wires the redial hook. */
  private connect(): Mux {
    const ws = this.createSocket(this.url as string);
    const mux = new Mux(ws, this.maxReceiveQueue, this.initialWindow);
    if (this.reconnectEnabled) {
      // A successful new socket resets the backoff once it actually opens.
      ws.onopen = ((prev) => () => {
        this.attempt = 0;
        prev?.({});
      })(ws.onopen);
      mux.setOnDisconnect(() => this.scheduleReconnect());
    }
    return mux;
  }

  /**
   * scheduleReconnect installs a fresh socket+mux after a jittered backoff. New
   * openStream calls made in the gap go to the new mux and buffer (sendBuffer)
   * until its socket opens, then flush. Called only with reconnect enabled.
   */
  private scheduleReconnect(): void {
    if (this.closed) {
      return;
    }
    const delay = this.nextBackoff(this.attempt);
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (this.closed) {
        return;
      }
      // Build the replacement immediately; it buffers sends until its socket
      // opens. If this socket also fails, its onDisconnect re-schedules.
      this.mux = this.connect();
    }, delay);
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

  /** openStream begins a new RPC: sends OPEN(method, init.headers) and returns the stream. */
  openStream(method: string, init?: StreamInit): ClientStream {
    return this.mux.openStream(method, init);
  }

  /** close tears down all in-flight streams, stops reconnection, and closes the socket. */
  close(): void {
    this.closed = true;
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.mux.close();
  }
}
