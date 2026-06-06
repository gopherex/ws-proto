import { Mux } from "./mux.js";
import type { WebSocketLike } from "./mux.js";
import type { ClientStream } from "./stream.js";

export type { WebSocketLike };

/**
 * WsTransport dials (or wraps) one WebSocket and multiplexes RPC streams over
 * it. It is the untyped analog of connect-web's transport; Plan 4's generator
 * wraps openStream() with typed clients.
 */
export class WsTransport {
  private readonly mux: Mux;

  /** Constructs a transport by dialing a new browser-native WebSocket to `url`. */
  constructor(url: string);
  /** Internal: construct from an existing WebSocketLike (used by fromSocket). */
  constructor(socket: WebSocketLike, fromExisting: true);
  constructor(urlOrSocket: string | WebSocketLike, fromExisting?: true) {
    if (fromExisting === true) {
      this.mux = new Mux(urlOrSocket as WebSocketLike);
      return;
    }
    const ws = new WebSocket(urlOrSocket as string) as unknown as WebSocketLike;
    this.mux = new Mux(ws);
  }

  /** fromSocket wraps an already-created WebSocketLike (browser socket or a test fake). */
  static fromSocket(ws: WebSocketLike): WsTransport {
    return new WsTransport(ws, true);
  }

  /** openStream begins a new RPC: sends OPEN(method, headers) and returns the stream. */
  openStream(method: string, headers: Record<string, string> = {}): ClientStream {
    return this.mux.openStream(method, headers);
  }

  /** close tears down all in-flight streams and closes the socket. */
  close(): void {
    this.mux.close();
  }
}
