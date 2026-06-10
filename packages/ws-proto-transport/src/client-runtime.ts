import type { Message } from "@bufbuild/protobuf";
import type { StreamResponse, UnaryResponse } from "./interceptor.js";

/**
 * Runtime helpers the generated client delegates to, so the generated code stays
 * thin and the callback-timing logic lives in one tested place.
 */

/** CallCallbacks are the optional leading-header / trailer hooks of a call. */
export interface CallCallbacks {
  onHeader?: (header: Record<string, string>) => void;
  onTrailer?: (trailer: Record<string, string>) => void;
}

/**
 * applyUnaryCallbacks fires the header/trailer hooks for a unary response (both
 * are already resolved) and returns the message.
 */
export function applyUnaryCallbacks<O extends Message>(
  r: UnaryResponse<Message, O>,
  cb?: CallCallbacks,
): O {
  cb?.onHeader?.(r.header);
  cb?.onTrailer?.(r.trailer);
  return r.message;
}

/**
 * iterateStream yields every response message, firing onHeader exactly once
 * before the first message (or before completing if the stream is empty) and
 * onTrailer after the stream ends. Used by server-streaming and bidi clients.
 */
export async function* iterateStream<O extends Message>(
  r: StreamResponse<Message, O>,
  cb?: CallCallbacks,
): AsyncIterable<O> {
  let fired = false;
  for await (const m of r.message) {
    if (!fired) {
      cb?.onHeader?.(r.header);
      fired = true;
    }
    yield m;
  }
  if (!fired) {
    cb?.onHeader?.(r.header);
  }
  cb?.onTrailer?.(r.trailer);
}

/**
 * firstResponse drains a streaming response expecting a single message (the
 * client-streaming case), firing the same callbacks as iterateStream. Throws if
 * the server ended the stream without any message.
 */
export async function firstResponse<O extends Message>(
  r: StreamResponse<Message, O>,
  label: string,
  cb?: CallCallbacks,
): Promise<O> {
  let out: O | undefined;
  let fired = false;
  for await (const m of r.message) {
    if (!fired) {
      cb?.onHeader?.(r.header);
      fired = true;
    }
    out = m;
  }
  if (!fired) {
    cb?.onHeader?.(r.header);
  }
  cb?.onTrailer?.(r.trailer);
  if (out === undefined) {
    throw new Error(`${label}: server closed stream without a response`);
  }
  return out;
}
