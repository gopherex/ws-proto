import type { Message } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";

/**
 * Interceptors are the typed, message-level extension point of the client
 * (the analog of connect-web interceptors). They wrap a typed RPC call and can
 * read/modify request metadata, observe or transform the request and response
 * MESSAGES, short-circuit a call, and read response trailers — across all four
 * method kinds. They are installed transport-wide via WsTransportOptions.
 *
 * This module defines only the types and the composer; WsTransport.unary/stream
 * build the chain and supply the terminal function that does the actual I/O.
 */

/** MethodKind mirrors the protobuf method kinds. */
export type MethodKind =
  | "unary"
  | "server_streaming"
  | "client_streaming"
  | "bidi_streaming";

/**
 * MethodInfo describes one RPC method to the transport and interceptors. The
 * generated client supplies it (service type name, method name, kind, and the
 * input/output protobuf schemas used for (de)serialization).
 */
export interface MethodInfo<I extends Message = Message, O extends Message = Message> {
  readonly typeName: string; // fully-qualified service name, e.g. "echo.v1.EchoService"
  readonly name: string; // method name, e.g. "Unary"
  readonly kind: MethodKind;
  readonly input: GenMessage<I>;
  readonly output: GenMessage<O>;
}

/** route returns the wire path "/pkg.Service/Method" for a method. */
export function route(method: MethodInfo): string {
  return `/${method.typeName}/${method.name}`;
}

/** A unary RPC invocation as seen by interceptors (one request message). */
export interface UnaryRequest<I extends Message = Message, O extends Message = Message> {
  readonly stream: false;
  readonly method: MethodInfo<I, O>;
  /** Request metadata; interceptors may mutate it before calling next. */
  header: Record<string, string>;
  readonly message: I;
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
}

/** A streaming RPC invocation as seen by interceptors (a stream of requests). */
export interface StreamRequest<I extends Message = Message, O extends Message = Message> {
  readonly stream: true;
  readonly method: MethodInfo<I, O>;
  header: Record<string, string>;
  readonly message: AsyncIterable<I>;
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
}

/** The completed unary response (leading header, single message, trailer). */
export interface UnaryResponse<I extends Message = Message, O extends Message = Message> {
  readonly stream: false;
  readonly method: MethodInfo<I, O>;
  header: Record<string, string>;
  message: O;
  trailer: Record<string, string>;
}

/** The streaming response (leading header, a stream of messages, trailer). */
export interface StreamResponse<I extends Message = Message, O extends Message = Message> {
  readonly stream: true;
  readonly method: MethodInfo<I, O>;
  header: Record<string, string>;
  message: AsyncIterable<O>;
  trailer: Record<string, string>;
}

export type AnyRequest = UnaryRequest | StreamRequest;
export type AnyResponse = UnaryResponse | StreamResponse;

/** AnyFn performs one RPC call; interceptors wrap it. */
export type AnyFn = (req: AnyRequest) => Promise<AnyResponse>;

/** Interceptor wraps an AnyFn to add behavior around a call. */
export type Interceptor = (next: AnyFn) => AnyFn;

/**
 * applyInterceptors composes interceptors around a terminal AnyFn. The FIRST
 * interceptor in the array is the OUTERMOST (runs first on the way in, last on
 * the way out) — matching connect-web ordering. With none, the terminal is
 * returned unchanged.
 */
export function applyInterceptors(next: AnyFn, interceptors?: readonly Interceptor[]): AnyFn {
  if (!interceptors || interceptors.length === 0) {
    return next;
  }
  return interceptors.reduceRight<AnyFn>((n, i) => i(n), next);
}
