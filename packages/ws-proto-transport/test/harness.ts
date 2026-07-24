/**
 * Test harness for lifecycle/teardown invariants.
 *
 * The defect class under test is "a promise hangs forever", so hangs must be a
 * DIAGNOSIS, not a runner timeout: expectSettles() turns a hang into an explicit
 * HangError, expectNeverSettles() pins down behavior that is deliberately
 * pending. Invariants are formulated once and parametrized over RPC shapes and
 * socket states rather than hand-written per scenario.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import { FakeSocket, Kind, WsTransport } from "../src/index.js";
import type { MethodInfo } from "../src/index.js";
import { StatusSchema } from "../src/gen/transport_pb.js";
import type { Status } from "../src/gen/transport_pb.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
export const tick = (): Promise<void> => new Promise<void>((r) => setTimeout(r, 0));
/** wait sleeps real ms (kept tiny via small test backoffs). */
export const wait = (ms: number): Promise<void> => new Promise<void>((r) => setTimeout(r, ms));

/** HangError marks a promise that failed to settle in time — the bug signature. */
export class HangError extends Error {
  constructor(label: string, ms: number) {
    super(`${label} did not settle within ${ms}ms — hang detected`);
    this.name = "HangError";
  }
}

/**
 * expectSettles awaits `p` but throws HangError if it stays pending for `ms`.
 * The promise's own outcome (value or rejection) is passed through unchanged,
 * so `await expectSettles(p)` behaves like `await p` plus a hang detector.
 */
export function expectSettles<T>(p: Promise<T>, ms = 500, label = "promise"): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new HangError(label, ms)), ms);
    p.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}

/**
 * expectRejects is expectSettles specialized to a rejection: it resolves with
 * the rejection reason, throws HangError on a hang, and throws if the promise
 * fulfills instead of rejecting.
 */
export async function expectRejects(p: Promise<unknown>, ms = 500, label = "promise"): Promise<unknown> {
  try {
    const v = await expectSettles(p, ms, label);
    throw new Error(`${label} fulfilled (${String(v)}) but a rejection was expected`);
  } catch (e) {
    if (e instanceof HangError) {
      throw e;
    }
    if (e instanceof Error && e.message.includes("but a rejection was expected")) {
      throw e;
    }
    return e;
  }
}

/**
 * expectNeverSettles asserts `p` is still pending after `ms`. Used to PIN
 * deliberate behavior (e.g. a promise that must wait for reconnect), so a later
 * change that makes it settle is caught. Attaches handlers so the deliberately
 * pending/rejected promise can never surface as an unhandled rejection.
 */
export async function expectNeverSettles(p: Promise<unknown>, ms = 50, label = "promise"): Promise<void> {
  let settled: "fulfilled" | "rejected" | undefined;
  p.then(
    () => (settled = "fulfilled"),
    () => (settled = "rejected"),
  );
  await wait(ms);
  if (settled !== undefined) {
    throw new Error(`${label} ${settled} but was expected to stay pending for ${ms}ms`);
  }
}

/** swallow marks a promise as observed so an expected rejection is never "unhandled". */
export function swallow(p: Promise<unknown>): void {
  void p.catch(() => {});
}

// ---- typed-method fixtures (Status doubles as the test I/O message) ----

export function makeMethod(kind: MethodInfo["kind"], name: string): MethodInfo<Status, Status> {
  return { typeName: "test.Svc", name, kind, input: StatusSchema, output: StatusSchema };
}

export const unaryMethod = makeMethod("unary", "Unary");
export const serverStreamMethod = makeMethod("server_streaming", "Feed");
export const clientStreamMethod = makeMethod("client_streaming", "Upload");
export const bidiMethod = makeMethod("bidi_streaming", "Sync");

/** msg builds a typed Status test message. */
export const msg = (code: number): Status => create(StatusSchema, { code });
/** msgBytes builds the wire bytes of a Status test message. */
export const msgBytes = (code: number): Uint8Array => toBinary(StatusSchema, msg(code));

/**
 * foreverSource yields `first` then blocks forever — the canonical long-lived
 * bidi request source (idle between user actions). `events` records whether the
 * generator's finally ran, so tests can assert transport-driven finalization.
 */
export function foreverSource(
  first: Status,
  events: { finallyRan?: boolean; unblock?: () => void } = {},
): AsyncGenerator<Status> {
  return (async function* () {
    try {
      yield first;
      // Idle forever unless the test unblocks it (to observe queued return()).
      await new Promise<void>((r) => {
        events.unblock = r;
      });
    } finally {
      events.finallyRan = true;
    }
  })();
}

// ---- socket-state matrix ----

/**
 * SocketState enumerates the transport states each invariant must hold in:
 *   preopen — socket never reached OPEN;
 *   open    — socket opened normally;
 *   dropped — socket opened then hard-dropped (error, no close frame);
 *   closed  — transport deliberately closed.
 */
export type SocketState = "preopen" | "open" | "dropped" | "closed";

export interface Rig {
  sock: FakeSocket;
  transport: WsTransport;
}

/** rig builds a fromSocket transport over a FakeSocket. */
export function rig(opts: { autoOpen?: boolean; protocol?: string } = {}, topts = {}): Rig {
  const sock = new FakeSocket(opts);
  const transport = WsTransport.fromSocket(sock, topts);
  return { sock, transport };
}

export { FakeSocket, Kind };
