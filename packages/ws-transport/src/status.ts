import type { Status } from "./frame.js";

/**
 * WsStatusError is thrown by ClientStream.recv()/iteration when a stream ends
 * with a non-OK status (END with code != 0) or is reset (RST). It mirrors the
 * gRPC-style status: numeric code, human message, and opaque detail blobs.
 */
export class WsStatusError extends Error {
  readonly code: number;
  readonly details: Uint8Array[];

  constructor(code: number, message: string, details: Uint8Array[] = []) {
    super(message);
    this.name = "WsStatusError";
    this.code = code;
    this.details = details;
    // Restore prototype chain for instanceof across transpilation targets.
    Object.setPrototypeOf(this, WsStatusError.prototype);
  }
}

/** gRPC-style "OK" status code. */
export const CODE_OK = 0;
/** gRPC-style "CANCELLED" status code (used when a stream is RST). */
export const CODE_CANCELLED = 1;

/**
 * statusErrorFromProto converts a decoded END Status into a WsStatusError, or
 * returns null when the status is OK (code 0 / absent). Used by the mux to
 * decide whether a clean END (null) or an error end (throw) occurred.
 */
export function statusErrorFromProto(status: Status | undefined): WsStatusError | null {
  if (status === undefined || status.code === CODE_OK) {
    return null;
  }
  return new WsStatusError(status.code, status.message, status.details ?? []);
}
