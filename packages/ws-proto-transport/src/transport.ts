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

/**
 * WsTransportOptions tunes a transport instance.
 *
 * maxReceiveQueue bounds the per-stream inbound MSG backlog. A consumer too slow
 * to keep its queue under this bound has its stream reset
 * (CODE_RESOURCE_EXHAUSTED) instead of buffering without limit. Defaults to 256.
 */
export interface WsTransportOptions {
  maxReceiveQueue?: number;
}

/**
 * WsTransport dials (or wraps) one WebSocket and multiplexes RPC streams over
 * it. It is the untyped analog of connect-web's transport; Plan 4's generator
 * wraps openStream() with typed clients.
 */
export class WsTransport {
  private readonly mux: Mux;

  /** Constructs a transport by dialing a new browser-native WebSocket to `url`. */
  constructor(url: string, options?: WsTransportOptions);
  /** Internal: construct from an existing WebSocketLike (used by fromSocket). */
  constructor(socket: WebSocketLike, fromExisting: true, options?: WsTransportOptions);
  constructor(
    urlOrSocket: string | WebSocketLike,
    fromExisting?: true | WsTransportOptions,
    options?: WsTransportOptions,
  ) {
    if (fromExisting === true) {
      this.mux = new Mux(urlOrSocket as WebSocketLike, options?.maxReceiveQueue);
      return;
    }
    const opts = fromExisting as WsTransportOptions | undefined;
    // Offer the wsrpc.v1 subprotocol so the server can confirm it during the
    // RFC 6455 handshake and proxies can identify wsrpc traffic.
    const ws = new WebSocket(urlOrSocket as string, SUBPROTOCOL) as unknown as WebSocketLike;
    this.mux = new Mux(ws, opts?.maxReceiveQueue);
  }

  /** fromSocket wraps an already-created WebSocketLike (browser socket or a test fake). */
  static fromSocket(ws: WebSocketLike, options?: WsTransportOptions): WsTransport {
    return new WsTransport(ws, true, options);
  }

  /** openStream begins a new RPC: sends OPEN(method, init.headers) and returns the stream. */
  openStream(method: string, init?: StreamInit): ClientStream {
    return this.mux.openStream(method, init);
  }

  /** close tears down all in-flight streams and closes the socket. */
  close(): void {
    this.mux.close();
  }
}
