# Plan 3 — TypeScript Runtime `@gopherex/ws-transport` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the browser/node WebSocket transport runtime npm package `@gopherex/ws-transport` (the analog of connect-web's transport) at `packages/ws-transport/`. It dials one WebSocket, multiplexes many concurrent RPC streams over it using the shared `Frame` wire contract, and exposes an untyped (payload = `Uint8Array`) public API that the Plan 4 generator (`protoc-gen-ws-es`) will wrap with typed clients.

**Architecture:** One WebSocket carries binary messages; each message is a marshaled `Frame`. A `Mux` owns the socket and a `Map<number, StreamState>` registry. The socket's `onmessage` handler decodes a `Frame` and routes it by `stream_id`: `MSG` pushes the payload onto that stream's async queue; `END`/`RST` resolve/reject the queue and delete the entry; `OPEN` is ignored (server never opens streams). The send path encodes a `Frame` to bytes and calls `ws.send`; frames produced before the socket reaches `OPEN` state are buffered and flushed on `onopen`. Each stream bridges push (`onmessage`) to pull (`recv()` / async iterator) via an unbounded `AsyncQueue` (backpressure is intentionally simple in v1 and documented). `WsStatusError` carries gRPC-style code/message/details and is thrown on non-OK `END` or `RST`.

**Tech Stack:** TypeScript (ESM, `"type": "module"`), `@bufbuild/protobuf` v2 (`create`, `toBinary`, `fromBinary`), `@bufbuild/protoc-gen-es` v2 (devDep, generates `src/gen/transport_pb.ts` from `transport/transport.proto`), `vitest` (dev) for tests. Node 20+. No git branches/worktrees — commit on the current branch. Commit messages carry **no** `Co-Authored-By` trailer.

**Constraints from user:** Single current git branch only (never create branches/worktrees). Commit messages must NOT contain a `Co-Authored-By` trailer. The public API names below are **fixed** — Plan 4 depends on them exactly.

### Fixed wire contract (identical to Plan 1's `transport/transport.proto` — do NOT redesign)

```proto
syntax = "proto3";
package wsproto.transport.v1;

message Frame {
  uint32 stream_id = 1;
  Kind kind = 2;
  map<string, string> headers = 3;
  bytes payload = 4;
  Status status = 5;
  string method = 6;
}

enum Kind {
  KIND_UNSPECIFIED = 0;
  KIND_OPEN = 1;
  KIND_MSG = 2;
  KIND_HALF_CLOSE = 3;
  KIND_END = 4;
  KIND_RST = 5;
}

message Status {
  int32 code = 1;
  string message = 2;
  repeated bytes details = 3;
}
```

### Fixed public API (Plan 4 depends on these EXACT names)

```ts
export interface WebSocketLike { /* send, close, onmessage, onopen, onclose, onerror, binaryType */ }
export class WsStatusError extends Error { readonly code: number; readonly details: Uint8Array[]; constructor(code: number, message: string, details?: Uint8Array[]); }
export class WsTransport {
  constructor(url: string);
  static fromSocket(ws: WebSocketLike): WsTransport;
  openStream(method: string, headers?: Record<string, string>): ClientStream;
  close(): void;
}
export interface ClientStream {
  send(payload: Uint8Array): void;     // MSG
  closeSend(): void;                   // HALF_CLOSE
  recv(): Promise<Uint8Array | null>;  // next MSG payload; null on clean END(ok); throws WsStatusError on non-OK END / RST
  responseHeaders(): Promise<Record<string, string>>;
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array>;
}
```

---

## File Structure

| Path | Responsibility |
|---|---|
| `packages/ws-transport/package.json` | npm package `@gopherex/ws-transport`, ESM, exports, deps/devDeps, scripts |
| `packages/ws-transport/tsconfig.json` | TS compiler config (ESM, strict, `src/` → `dist/`) |
| `packages/ws-transport/buf.gen.yaml` | protoc-gen-es v2 generation config (drives `generate:proto` script) |
| `packages/ws-transport/src/gen/transport_pb.ts` | **Generated, committed**: `FrameSchema`, `KindSchema`, `StatusSchema`, `Kind` enum |
| `packages/ws-transport/src/frame.ts` | Frame codec: `encodeFrame`/`decodeFrame`, `Kind` re-export, `FrameInit` helper |
| `packages/ws-transport/src/status.ts` | `WsStatusError` + status helpers (proto Status → error) |
| `packages/ws-transport/src/queue.ts` | `AsyncQueue<T>` — push/pull bridge with end/error |
| `packages/ws-transport/src/stream.ts` | `ClientStream` interface + `StreamImpl` (per-RPC state) |
| `packages/ws-transport/src/mux.ts` | `Mux`: socket ownership, send buffering, `onmessage` routing, stream registry |
| `packages/ws-transport/src/transport.ts` | `WsTransport`, `WebSocketLike` |
| `packages/ws-transport/src/index.ts` | Public barrel: re-exports the fixed API |
| `packages/ws-transport/src/fake-socket.ts` | `FakeSocket implements WebSocketLike` test helper (exported for downstream tests too) |
| `packages/ws-transport/test/queue.test.ts` | Unit tests for `AsyncQueue` |
| `packages/ws-transport/test/frame.test.ts` | Codec round-trip tests |
| `packages/ws-transport/test/transport.test.ts` | Frame-level integration tests (all four kinds + non-OK END + RST) |

---

## Task 1: Bootstrap the npm package

**Files:**
- Create: `packages/ws-transport/package.json`, `packages/ws-transport/tsconfig.json`

- [ ] **Step 1: Create `packages/ws-transport/package.json`**

```json
{
  "name": "@gopherex/ws-transport",
  "version": "0.1.0",
  "description": "WebSocket multiplexing transport for protobuf RPC services (analog of connect-web transport).",
  "type": "module",
  "license": "MIT",
  "files": [
    "dist",
    "src"
  ],
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    }
  },
  "scripts": {
    "generate:proto": "buf generate",
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "test:watch": "vitest",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@bufbuild/protobuf": "^2.2.0"
  },
  "devDependencies": {
    "@bufbuild/buf": "^1.45.0",
    "@bufbuild/protoc-gen-es": "^2.2.0",
    "typescript": "^5.6.0",
    "vitest": "^2.1.0"
  }
}
```

> Note: `@bufbuild/buf` provides the `buf` CLI as a devDependency so generation needs no global install. If your toolchain already ships `buf`/`protoc`, you may drop it and run `buf generate` from the environment instead.

- [ ] **Step 2: Create `packages/ws-transport/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "lib": ["ES2022", "DOM"],
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "dist",
    "rootDir": "src",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "verbatimModuleSyntax": true
  },
  "include": ["src"],
  "exclude": ["dist", "node_modules", "test"]
}
```

> `"lib": ["DOM"]` gives us the ambient `WebSocket`, `MessageEvent`, `CloseEvent` types used by the browser-native dial path. `test/` is excluded from the build (vitest compiles tests itself).

- [ ] **Step 3: Install dependencies**

Run:
```bash
cd packages/ws-transport && npm install
```
Expected: creates `node_modules/` and `package-lock.json`. (If the repo uses a workspace root `package.json`, run `npm install` from the repo root instead and confirm the package is picked up as a workspace.)

- [ ] **Step 4: Commit**

```bash
git add packages/ws-transport/package.json packages/ws-transport/tsconfig.json packages/ws-transport/package-lock.json
git commit -m "chore(ts): bootstrap @gopherex/ws-transport package"
```

---

## Task 2: Generate `transport_pb.ts` from the wire contract

**Files:**
- Create: `packages/ws-transport/buf.gen.yaml`
- Generated (committed): `packages/ws-transport/src/gen/transport_pb.ts`

This step assumes `transport/transport.proto` already exists at the repo root (delivered by Plan 1). The runtime marshals `Frame` against the protoc-gen-es v2 output of that exact file.

- [ ] **Step 1: Create `packages/ws-transport/buf.gen.yaml`**

```yaml
version: v2
inputs:
  - directory: ../../transport
plugins:
  - local: protoc-gen-es
    out: src/gen
    opt:
      - target=ts
      - import_extension=js
```

> `import_extension=js` makes generated relative imports use `.js` suffixes, required by NodeNext ESM resolution. `target=ts` emits `.ts` source (committed, not `.d.ts`). The plugin binary `protoc-gen-es` is resolved from `node_modules/.bin` when `buf generate` runs via the npm script.

- [ ] **Step 2: Generate**

Run:
```bash
cd packages/ws-transport && npm run generate:proto
```
Expected: creates `packages/ws-transport/src/gen/transport_pb.ts` exporting (at minimum) `FrameSchema`, `KindSchema`, `StatusSchema`, and a `Kind` enum const with members `KIND_UNSPECIFIED`, `KIND_OPEN`, `KIND_MSG`, `KIND_HALF_CLOSE`, `KIND_END`, `KIND_RST`.

If `buf` cannot find the proto, verify the relative path `../../transport/transport.proto` resolves from `packages/ws-transport/`. If `protoc-gen-es` is not found, confirm `npm install` placed it under `node_modules/.bin/`.

- [ ] **Step 3: Verify the generated symbols exist**

Run:
```bash
cd packages/ws-transport && node --input-type=module -e "import('./src/gen/transport_pb.ts').catch(()=>{}); console.log('file present:', require('fs').existsSync('src/gen/transport_pb.ts'))"
```
Expected: prints `file present: true`. (Do not import the `.ts` directly at runtime — this check only asserts the file was written. Type-level verification happens in Task 4 when `frame.ts` imports the schemas and `npx tsc --noEmit` runs.)

- [ ] **Step 4: Commit (generated output is committed)**

```bash
git add packages/ws-transport/buf.gen.yaml packages/ws-transport/src/gen/transport_pb.ts
git commit -m "feat(ts): generate transport_pb.ts via protoc-gen-es v2"
```

---

## Task 3: AsyncQueue (push/pull bridge)

**Files:**
- Create: `packages/ws-transport/src/queue.ts`
- Test: `packages/ws-transport/test/queue.test.ts`

The `AsyncQueue` is the core primitive bridging `onmessage` pushes to `recv()`/iterator pulls. v1 is unbounded (no backpressure); this is documented in the source. TDD first.

- [ ] **Step 1: Write the failing test `packages/ws-transport/test/queue.test.ts`**

```ts
import { describe, it, expect } from "vitest";
import { AsyncQueue } from "../src/queue.js";

describe("AsyncQueue", () => {
  it("delivers a pushed value to a waiting pull", async () => {
    const q = new AsyncQueue<number>();
    const pull = q.pull();
    q.push(42);
    expect(await pull).toEqual({ done: false, value: 42 });
  });

  it("buffers pushes that precede pulls (FIFO)", async () => {
    const q = new AsyncQueue<number>();
    q.push(1);
    q.push(2);
    expect(await q.pull()).toEqual({ done: false, value: 1 });
    expect(await q.pull()).toEqual({ done: false, value: 2 });
  });

  it("end() resolves the current and subsequent pulls as done", async () => {
    const q = new AsyncQueue<number>();
    const pull = q.pull();
    q.end();
    expect(await pull).toEqual({ done: true, value: undefined });
    expect(await q.pull()).toEqual({ done: true, value: undefined });
  });

  it("end() drains buffered values before reporting done", async () => {
    const q = new AsyncQueue<number>();
    q.push(7);
    q.end();
    expect(await q.pull()).toEqual({ done: false, value: 7 });
    expect(await q.pull()).toEqual({ done: true, value: undefined });
  });

  it("fail() rejects a waiting pull", async () => {
    const q = new AsyncQueue<number>();
    const pull = q.pull();
    q.fail(new Error("boom"));
    await expect(pull).rejects.toThrow("boom");
  });

  it("fail() rejects subsequent pulls too", async () => {
    const q = new AsyncQueue<number>();
    q.fail(new Error("boom"));
    await expect(q.pull()).rejects.toThrow("boom");
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run:
```bash
cd packages/ws-transport && npx vitest run test/queue.test.ts
```
Expected: FAIL (cannot resolve `../src/queue.js`).

- [ ] **Step 3: Implement `packages/ws-transport/src/queue.ts`**

```ts
/**
 * AsyncQueue bridges synchronous pushes (from the WebSocket onmessage handler)
 * to asynchronous pulls (recv() / async iteration).
 *
 * Semantics:
 *   - push(v): enqueue a value. If a pull is waiting, it is resolved immediately.
 *   - end():   signal a clean end-of-stream. Buffered values are still drained
 *              by subsequent pulls; once drained, pulls resolve `{ done: true }`.
 *   - fail(e): signal an error end-of-stream. Buffered values are discarded and
 *              all current/future pulls reject with `e`.
 *
 * Backpressure: v1 is intentionally UNBOUNDED. A slow consumer cannot slow a
 * fast producer; memory grows with the backlog. Credit-based per-stream flow
 * control is a documented future extension (see transport design spec).
 */
export interface PullResult<T> {
  done: boolean;
  value: T | undefined;
}

type Waiter<T> = {
  resolve: (r: PullResult<T>) => void;
  reject: (e: unknown) => void;
};

export class AsyncQueue<T> {
  private readonly values: T[] = [];
  private readonly waiters: Waiter<T>[] = [];
  private ended = false;
  private error: unknown = undefined;

  push(value: T): void {
    if (this.ended || this.error !== undefined) {
      return; // ignore late pushes after end/fail
    }
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ done: false, value });
      return;
    }
    this.values.push(value);
  }

  end(): void {
    if (this.ended || this.error !== undefined) {
      return;
    }
    this.ended = true;
    // No buffered values left -> resolve all pending pulls as done.
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      waiter.resolve({ done: true, value: undefined });
    }
  }

  fail(error: unknown): void {
    if (this.error !== undefined) {
      return;
    }
    this.error = error ?? new Error("AsyncQueue failed");
    this.values.length = 0;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      waiter.reject(this.error);
    }
  }

  pull(): Promise<PullResult<T>> {
    if (this.values.length > 0) {
      const value = this.values.shift()!;
      return Promise.resolve({ done: false, value });
    }
    if (this.error !== undefined) {
      return Promise.reject(this.error);
    }
    if (this.ended) {
      return Promise.resolve({ done: true, value: undefined });
    }
    return new Promise<PullResult<T>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }
}
```

- [ ] **Step 4: Run to verify it passes**

Run:
```bash
cd packages/ws-transport && npx vitest run test/queue.test.ts
```
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add packages/ws-transport/src/queue.ts packages/ws-transport/test/queue.test.ts
git commit -m "feat(ts): add AsyncQueue push/pull bridge"
```

---

## Task 4: Frame codec + Kind re-export

**Files:**
- Create: `packages/ws-transport/src/frame.ts`
- Test: `packages/ws-transport/test/frame.test.ts`

- [ ] **Step 1: Write the failing test `packages/ws-transport/test/frame.test.ts`**

```ts
import { describe, it, expect } from "vitest";
import { encodeFrame, decodeFrame, Kind } from "../src/frame.js";

describe("frame codec", () => {
  it("round-trips an OPEN frame with method and headers", () => {
    const bytes = encodeFrame({
      streamId: 3,
      kind: Kind.KIND_OPEN,
      method: "/pkg.Svc/Do",
      headers: { k: "v" },
    });
    const f = decodeFrame(bytes);
    expect(f.streamId).toBe(3);
    expect(f.kind).toBe(Kind.KIND_OPEN);
    expect(f.method).toBe("/pkg.Svc/Do");
    expect(f.headers["k"]).toBe("v");
  });

  it("round-trips a MSG frame with payload bytes", () => {
    const payload = new Uint8Array([1, 2, 3, 4]);
    const f = decodeFrame(encodeFrame({ streamId: 5, kind: Kind.KIND_MSG, payload }));
    expect(f.kind).toBe(Kind.KIND_MSG);
    expect(Array.from(f.payload)).toEqual([1, 2, 3, 4]);
  });

  it("round-trips an END frame with a non-OK status and details", () => {
    const f = decodeFrame(
      encodeFrame({
        streamId: 7,
        kind: Kind.KIND_END,
        status: { code: 7, message: "nope", details: [new Uint8Array([9])] },
        headers: { trailer: "t" },
      }),
    );
    expect(f.kind).toBe(Kind.KIND_END);
    expect(f.status?.code).toBe(7);
    expect(f.status?.message).toBe("nope");
    expect(f.status?.details?.length).toBe(1);
    expect(f.headers["trailer"]).toBe("t");
  });

  it("defaults optional fields when omitted", () => {
    const f = decodeFrame(encodeFrame({ streamId: 1, kind: Kind.KIND_HALF_CLOSE }));
    expect(f.kind).toBe(Kind.KIND_HALF_CLOSE);
    expect(f.method).toBe("");
    expect(f.payload.length).toBe(0);
    expect(Object.keys(f.headers).length).toBe(0);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run:
```bash
cd packages/ws-transport && npx vitest run test/frame.test.ts
```
Expected: FAIL (cannot resolve `../src/frame.js`).

- [ ] **Step 3: Implement `packages/ws-transport/src/frame.ts`**

```ts
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import { FrameSchema, Kind } from "./gen/transport_pb.js";
import type { Frame, Status } from "./gen/transport_pb.js";

export { Kind };
export type { Frame, Status };

/**
 * FrameInit is the partial shape callers pass to encodeFrame. All fields are
 * optional except the two that every frame carries (streamId, kind). Unset
 * fields take protobuf defaults (0 / "" / empty).
 */
export interface FrameInit {
  streamId: number;
  kind: Kind;
  method?: string;
  headers?: Record<string, string>;
  payload?: Uint8Array;
  status?: {
    code: number;
    message?: string;
    details?: Uint8Array[];
  };
}

/** encodeFrame marshals a FrameInit into the binary wire form of a Frame. */
export function encodeFrame(init: FrameInit): Uint8Array {
  const status: Status | undefined =
    init.status === undefined
      ? undefined
      : ({
          $typeName: "wsproto.transport.v1.Status",
          code: init.status.code,
          message: init.status.message ?? "",
          details: init.status.details ?? [],
        } as Status);

  const frame = create(FrameSchema, {
    streamId: init.streamId,
    kind: init.kind,
    method: init.method ?? "",
    headers: init.headers ?? {},
    payload: init.payload ?? new Uint8Array(0),
    status,
  });
  return toBinary(FrameSchema, frame);
}

/** decodeFrame unmarshals the binary wire form of a Frame. */
export function decodeFrame(bytes: Uint8Array): Frame {
  return fromBinary(FrameSchema, bytes);
}
```

> Note: the exact `create(FrameSchema, {...})` input shape is what `@bufbuild/protobuf` v2 accepts (a plain init object). If protoc-gen-es names the Status `$typeName` differently, copy the literal from the generated `src/gen/transport_pb.ts` (`StatusSchema`'s `typeName`). The `as Status` cast bridges the partial init to the generated message type; `create` accepts the partial directly, so if the cast complains, pass the status fields through `create(StatusSchema, {...})` instead.

- [ ] **Step 4: Typecheck against the generated schema**

Run:
```bash
cd packages/ws-transport && npx tsc -p tsconfig.json --noEmit
```
Expected: no errors. This is the first real proof that `frame.ts` lines up with the generated `transport_pb.ts` symbols. If the `Status` literal/cast fails to typecheck, switch to building the nested status via `create(StatusSchema, { code, message, details })` and import `StatusSchema` from `./gen/transport_pb.js`.

- [ ] **Step 5: Run to verify it passes**

Run:
```bash
cd packages/ws-transport && npx vitest run test/frame.test.ts
```
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add packages/ws-transport/src/frame.ts packages/ws-transport/test/frame.test.ts
git commit -m "feat(ts): add Frame codec over generated schema"
```

---

## Task 5: WsStatusError + status helper

**Files:**
- Create: `packages/ws-transport/src/status.ts`

`WsStatusError` is part of the fixed public API. It has no dedicated unit test file (it is fully exercised by the integration tests in Task 8), but its construction and field shape are asserted there. Implement it now so `stream.ts`/`mux.ts` can use it.

- [ ] **Step 1: Implement `packages/ws-transport/src/status.ts`**

```ts
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
```

- [ ] **Step 2: Typecheck**

Run:
```bash
cd packages/ws-transport && npx tsc -p tsconfig.json --noEmit
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add packages/ws-transport/src/status.ts
git commit -m "feat(ts): add WsStatusError and status helper"
```

---

## Task 6: ClientStream interface + StreamImpl

**Files:**
- Create: `packages/ws-transport/src/stream.ts`

`StreamImpl` holds per-RPC state: an `AsyncQueue<Uint8Array>` of inbound MSG payloads, a promise for response headers, and a send callback wired to the mux. It cannot function without the mux, so it is exercised by the Task 8 integration tests (build/typecheck verification only here).

- [ ] **Step 1: Implement `packages/ws-transport/src/stream.ts`**

```ts
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
```

- [ ] **Step 2: Typecheck**

Run:
```bash
cd packages/ws-transport && npx tsc -p tsconfig.json --noEmit
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add packages/ws-transport/src/stream.ts
git commit -m "feat(ts): add ClientStream interface and StreamImpl"
```

---

## Task 7: Mux + WsTransport + WebSocketLike + barrel

**Files:**
- Create: `packages/ws-transport/src/mux.ts`, `packages/ws-transport/src/transport.ts`, `packages/ws-transport/src/index.ts`

This is the heart of the runtime: socket ownership, pre-open send buffering, the `onmessage` routing logic, and the stream registry. It is verified by build here and by the Task 8 integration tests.

- [ ] **Step 1: Implement `packages/ws-transport/src/mux.ts`**

```ts
import { encodeFrame, decodeFrame, Kind } from "./frame.js";
import { StreamImpl } from "./stream.js";
import { WsStatusError, statusErrorFromProto, CODE_CANCELLED } from "./status.js";

/**
 * WebSocketLike is the minimal browser-WebSocket surface the mux drives. The
 * real browser `WebSocket` satisfies it, as does the test FakeSocket.
 */
export interface WebSocketLike {
  binaryType: string;
  send(data: ArrayBufferView | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onopen: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

/** WebSocket.readyState OPEN constant; defined locally to avoid DOM coupling. */
const WS_OPEN = 1;

/**
 * Mux owns one WebSocketLike and multiplexes many streams over it. Client only:
 * it allocates monotonic ODD stream ids (1, 3, 5, ...) and never reacts to OPEN.
 */
export class Mux {
  private readonly ws: WebSocketLike;
  private readonly streams = new Map<number, StreamImpl>();
  private nextId = 1;

  /** Frames produced before the socket reaches OPEN are buffered here. */
  private readonly sendBuffer: Uint8Array[] = [];
  private open = false;
  private closed = false;

  constructor(ws: WebSocketLike) {
    this.ws = ws;
    this.ws.binaryType = "arraybuffer";

    // If the socket is already open (e.g. fromSocket on a live socket), flush.
    if ((this.ws as { readyState?: number }).readyState === WS_OPEN) {
      this.open = true;
    }

    this.ws.onopen = () => {
      this.open = true;
      this.flush();
    };
    this.ws.onmessage = (ev) => this.handleMessage(ev.data);
    this.ws.onclose = () => this.failAll(new WsStatusError(CODE_CANCELLED, "websocket closed"));
    this.ws.onerror = () => this.failAll(new WsStatusError(CODE_CANCELLED, "websocket error"));
  }

  /** open allocates an odd stream id, registers the stream, and sends OPEN. */
  openStream(method: string, headers: Record<string, string>): StreamImpl {
    const id = this.nextId;
    this.nextId += 2;

    const stream = new StreamImpl(id, {
      sendMsg: (sid, payload) =>
        this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_MSG, payload })),
      halfClose: (sid) => this.writeFrame(encodeFrame({ streamId: sid, kind: Kind.KIND_HALF_CLOSE })),
    });
    this.streams.set(id, stream);

    this.writeFrame(encodeFrame({ streamId: id, kind: Kind.KIND_OPEN, method, headers }));
    return stream;
  }

  /** writeFrame sends now if the socket is open, otherwise buffers until onopen. */
  private writeFrame(bytes: Uint8Array): void {
    if (this.closed) {
      return;
    }
    if (this.open) {
      this.ws.send(bytes);
      return;
    }
    this.sendBuffer.push(bytes);
  }

  private flush(): void {
    if (this.closed) {
      return;
    }
    for (const bytes of this.sendBuffer) {
      this.ws.send(bytes);
    }
    this.sendBuffer.length = 0;
  }

  /** handleMessage decodes one inbound binary WS message and routes by stream id. */
  private handleMessage(data: unknown): void {
    const bytes = toUint8Array(data);
    if (bytes === null) {
      return; // ignore non-binary frames
    }
    const frame = decodeFrame(bytes);

    switch (frame.kind) {
      case Kind.KIND_OPEN:
        // Server never opens streams in this protocol; ignore.
        return;

      case Kind.KIND_MSG: {
        const s = this.streams.get(frame.streamId);
        if (s) {
          s.pushMsg(frame.payload);
        }
        return;
      }

      case Kind.KIND_END: {
        const s = this.streams.get(frame.streamId);
        this.streams.delete(frame.streamId);
        if (!s) {
          return;
        }
        const err = statusErrorFromProto(frame.status);
        if (err === null) {
          s.endOk(frame.headers);
        } else {
          s.endError(err, frame.headers);
        }
        return;
      }

      case Kind.KIND_RST: {
        const s = this.streams.get(frame.streamId);
        this.streams.delete(frame.streamId);
        if (!s) {
          return;
        }
        const err =
          statusErrorFromProto(frame.status) ??
          new WsStatusError(CODE_CANCELLED, frame.status?.message || "stream reset");
        s.endError(err, frame.headers);
        return;
      }

      default:
        // KIND_UNSPECIFIED / KIND_HALF_CLOSE inbound: not expected from server; ignore.
        return;
    }
  }

  private failAll(err: WsStatusError): void {
    if (this.closed) {
      // already torn down; just drain
    }
    for (const [id, s] of this.streams) {
      s.endError(err);
      this.streams.delete(id);
    }
  }

  /** close tears down all streams and the socket. */
  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.failAll(new WsStatusError(CODE_CANCELLED, "transport closed"));
    try {
      this.ws.close(1000, "");
    } catch {
      // closing an already-closed socket is fine
    }
  }
}

/** toUint8Array normalizes the WS `data` payload to a Uint8Array, or null if not binary. */
function toUint8Array(data: unknown): Uint8Array | null {
  if (data instanceof Uint8Array) {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }
  return null; // string / Blob — not used by this protocol
}
```

- [ ] **Step 2: Implement `packages/ws-transport/src/transport.ts`**

```ts
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
```

> Note: `new WebSocket(url)` relies on the ambient browser/Node `WebSocket` global (Node 22+ ships it natively; in older Node or non-browser environments, callers should use `WsTransport.fromSocket(...)` with their own WebSocket implementation, e.g. the `ws` package). The dial constructor is intentionally not exercised by unit tests — `fromSocket` + `FakeSocket` covers all routing logic without a network.

- [ ] **Step 3: Implement `packages/ws-transport/src/index.ts`**

```ts
export { WsTransport } from "./transport.js";
export type { WebSocketLike } from "./transport.js";
export type { ClientStream } from "./stream.js";
export { WsStatusError } from "./status.js";
export { Kind } from "./frame.js";
export type { Frame, Status } from "./frame.js";
export { encodeFrame, decodeFrame } from "./frame.js";
export type { FrameInit } from "./frame.js";
export { FakeSocket } from "./fake-socket.js";
```

> `FakeSocket` is exported from the package barrel so downstream packages (the Plan 4 generator's tests) can reuse the same server-side test driver. It is implemented in Task 8 Step 1; this export line will fail to typecheck until then — that is expected and resolves within Task 8.

- [ ] **Step 4: Build (will fail on the missing FakeSocket import only)**

Run:
```bash
cd packages/ws-transport && npx tsc -p tsconfig.json --noEmit
```
Expected: the ONLY error is the unresolved `./fake-socket.js` import in `index.ts`. All of `mux.ts`/`transport.ts` must typecheck. Proceed to Task 8 to add `FakeSocket`; do NOT commit yet.

---

## Task 8: FakeSocket test driver + Frame-level integration tests

**Files:**
- Create: `packages/ws-transport/src/fake-socket.ts`, `packages/ws-transport/test/transport.test.ts`

The `FakeSocket` implements `WebSocketLike` and lets a test play the server side: it captures Frames the client sent (so the test can assert OPEN/MSG/HALF_CLOSE were emitted) and injects inbound Frames (MSG/END/RST) back into the client. Tests encode/decode Frames with the same `@bufbuild/protobuf` codec the runtime uses.

- [ ] **Step 1: Implement `packages/ws-transport/src/fake-socket.ts`**

```ts
import type { WebSocketLike } from "./mux.js";
import { decodeFrame, encodeFrame, Kind } from "./frame.js";
import type { Frame, FrameInit } from "./frame.js";

/**
 * FakeSocket is a WebSocketLike for tests. It does no networking; instead it:
 *   - captures every binary message the client sends (decoded to a Frame), and
 *   - lets the test inject inbound Frames as if from a server.
 *
 * By default it auto-opens on the next microtask so client writes flush; pass
 * { autoOpen: false } and call open() manually to exercise pre-open buffering.
 */
export class FakeSocket implements WebSocketLike {
  binaryType = "arraybuffer";

  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;

  /** Raw bytes the client has sent (in order). */
  readonly sentBytes: Uint8Array[] = [];
  /** Decoded Frames the client has sent (in order). */
  readonly sent: Frame[] = [];

  closedCode: number | undefined;
  closedReason: string | undefined;
  private isClosed = false;

  constructor(opts: { autoOpen?: boolean } = {}) {
    if (opts.autoOpen !== false) {
      // Open after the current synchronous setup so onopen is registered.
      queueMicrotask(() => this.open());
    }
  }

  send(data: ArrayBufferView | ArrayBuffer): void {
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    // Copy so later buffer reuse cannot mutate captured frames.
    const copy = bytes.slice();
    this.sentBytes.push(copy);
    this.sent.push(decodeFrame(copy));
  }

  close(code?: number, reason?: string): void {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    this.closedCode = code;
    this.closedReason = reason;
    this.onclose?.({ code, reason });
  }

  // ---- test-side controls ----

  /** open transitions the socket to OPEN, triggering the mux to flush buffered sends. */
  open(): void {
    this.onopen?.({});
  }

  /** inject delivers a server-built Frame to the client mux. */
  inject(init: FrameInit): void {
    const bytes = encodeFrame(init);
    this.onmessage?.({ data: bytes });
  }

  /** error simulates a socket error (mux fails all streams). */
  error(): void {
    this.onerror?.({});
  }

  /** lastSent returns the most recently sent Frame, or undefined. */
  lastSent(): Frame | undefined {
    return this.sent[this.sent.length - 1];
  }
}

// Re-export Kind so test files can import it from one place.
export { Kind };
```

- [ ] **Step 2: Write the integration test `packages/ws-transport/test/transport.test.ts`**

```ts
import { describe, it, expect } from "vitest";
import { WsTransport, WsStatusError, FakeSocket, Kind } from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("WsTransport over FakeSocket", () => {
  it("sends OPEN with method and headers on openStream", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    t.openStream("/pkg.Svc/Unary", { auth: "token" });
    await tick();

    const open = sock.sent.find((f) => f.kind === Kind.KIND_OPEN);
    expect(open).toBeDefined();
    expect(open!.streamId).toBe(1);
    expect(open!.method).toBe("/pkg.Svc/Unary");
    expect(open!.headers["auth"]).toBe("token");
  });

  it("allocates monotonic odd stream ids", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    t.openStream("/s/A");
    t.openStream("/s/B");
    t.openStream("/s/C");
    await tick();
    const ids = sock.sent.filter((f) => f.kind === Kind.KIND_OPEN).map((f) => f.streamId);
    expect(ids).toEqual([1, 3, 5]);
  });

  it("buffers frames sent before the socket opens, then flushes on open", async () => {
    const sock = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(sock);

    const s = t.openStream("/s/Buf");
    s.send(new Uint8Array([1]));
    s.closeSend();

    expect(sock.sent.length).toBe(0); // nothing sent while closed

    sock.open();
    await tick();

    const kinds = sock.sent.map((f) => f.kind);
    expect(kinds).toEqual([Kind.KIND_OPEN, Kind.KIND_MSG, Kind.KIND_HALF_CLOSE]);
  });

  // ---- the four stream kinds at the Frame level ----

  it("unary: MSG + HALF_CLOSE out, one MSG + clean END in", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Unary");
    s.send(new Uint8Array([0xaa]));
    s.closeSend();
    await tick();

    const out = sock.sent.map((f) => f.kind);
    expect(out).toEqual([Kind.KIND_OPEN, Kind.KIND_MSG, Kind.KIND_HALF_CLOSE]);
    expect(sock.sent[1]!.payload[0]).toBe(0xaa);

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([0xbb]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });

    const r1 = await s.recv();
    expect(r1).not.toBeNull();
    expect(r1![0]).toBe(0xbb);
    expect(await s.recv()).toBeNull(); // clean END
  });

  it("server-stream: HALF_CLOSE out, N MSG + clean END in (async iteration)", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/SS");
    s.closeSend();
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([0]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([1]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([2]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });

    const got: number[] = [];
    for await (const msg of s) {
      got.push(msg[0]!);
    }
    expect(got).toEqual([0, 1, 2]);
  });

  it("client-stream: N MSG + HALF_CLOSE out, one MSG + clean END in", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/CS");
    s.send(new Uint8Array([1]));
    s.send(new Uint8Array([2]));
    s.send(new Uint8Array([3]));
    s.closeSend();
    await tick();

    const msgCount = sock.sent.filter((f) => f.kind === Kind.KIND_MSG).length;
    expect(msgCount).toBe(3);
    expect(sock.lastSent()!.kind).toBe(Kind.KIND_HALF_CLOSE);

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([6]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });

    expect((await s.recv())![0]).toBe(6);
    expect(await s.recv()).toBeNull();
  });

  it("bidi: interleaved MSG out and MSG in, then clean END", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/BD");

    s.send(new Uint8Array([1]));
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([10]) });
    expect((await s.recv())![0]).toBe(10);

    s.send(new Uint8Array([2]));
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([20]) });
    expect((await s.recv())![0]).toBe(20);

    s.closeSend();
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    expect(await s.recv()).toBeNull();

    const outKinds = sock.sent.map((f) => f.kind);
    expect(outKinds).toContain(Kind.KIND_OPEN);
    expect(outKinds.filter((k) => k === Kind.KIND_MSG).length).toBe(2);
    expect(outKinds).toContain(Kind.KIND_HALF_CLOSE);
  });

  // ---- error paths ----

  it("non-OK END rejects recv with a WsStatusError carrying code/message/details", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Err");
    s.closeSend();
    await tick();

    sock.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 7, message: "denied", details: [new Uint8Array([0xde])] },
    });

    await expect(s.recv()).rejects.toBeInstanceOf(WsStatusError);
    // Re-pull to inspect the error object's fields.
    const sock2 = new FakeSocket();
    const t2 = WsTransport.fromSocket(sock2);
    const s2 = t2.openStream("/s/Err2");
    s2.closeSend();
    await tick();
    sock2.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 7, message: "denied", details: [new Uint8Array([0xde])] },
    });
    try {
      await s2.recv();
      throw new Error("expected throw");
    } catch (e) {
      const err = e as WsStatusError;
      expect(err).toBeInstanceOf(WsStatusError);
      expect(err.code).toBe(7);
      expect(err.message).toBe("denied");
      expect(Array.from(err.details[0]!)).toEqual([0xde]);
    }
  });

  it("RST rejects recv with a WsStatusError", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Rst");
    s.closeSend();
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_RST, status: { code: 1, message: "cancelled" } });

    await expect(s.recv()).rejects.toBeInstanceOf(WsStatusError);
  });

  it("RST during async iteration throws out of the for-await loop", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/RstIter");
    s.closeSend();
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([1]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_RST, status: { code: 1, message: "boom" } });

    const got: number[] = [];
    await expect(
      (async () => {
        for await (const msg of s) {
          got.push(msg[0]!);
        }
      })(),
    ).rejects.toBeInstanceOf(WsStatusError);
    expect(got).toEqual([1]); // the one MSG before RST was delivered
  });

  it("responseHeaders resolves with trailers from a clean END", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Trailers");
    s.closeSend();
    await tick();

    sock.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 0 },
      headers: { "x-trailer": "ok" },
    });

    const headers = await s.responseHeaders();
    expect(headers["x-trailer"]).toBe("ok");
  });

  it("close() fails in-flight streams with a WsStatusError", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Closed");
    await tick();

    t.close();
    await expect(s.recv()).rejects.toBeInstanceOf(WsStatusError);
    expect(sock.closedCode).toBe(1000);
  });
});
```

- [ ] **Step 3: Run to verify the suite passes**

Run:
```bash
cd packages/ws-transport && npx vitest run
```
Expected: PASS — all tests across `queue.test.ts`, `frame.test.ts`, and `transport.test.ts`.

If `transport.test.ts` fails on timing (an `inject` arriving before the stream registered), insert an extra `await tick()` after `openStream` and before `inject`. The mux registers the stream synchronously inside `openStream`, so injection should be safe immediately, but the extra tick is a harmless stabilizer.

- [ ] **Step 4: Full typecheck**

Run:
```bash
cd packages/ws-transport && npx tsc -p tsconfig.json --noEmit
```
Expected: no errors (the `index.ts` `FakeSocket` import now resolves).

- [ ] **Step 5: Commit**

```bash
git add packages/ws-transport/src/mux.ts packages/ws-transport/src/transport.ts packages/ws-transport/src/index.ts packages/ws-transport/src/fake-socket.ts packages/ws-transport/test/transport.test.ts
git commit -m "feat(ts): add Mux, WsTransport, FakeSocket and Frame-level integration tests"
```

---

## Task 9: Build + finalize

**Files:**
- No new files; verifies the package builds and the full suite is green.

- [ ] **Step 1: Type-emit build**

Run:
```bash
cd packages/ws-transport && npm run build
```
Expected: emits `dist/` with `.js` + `.d.ts` files, no errors. (If `dist/` should be git-ignored, add `packages/ws-transport/dist/` to the repo `.gitignore` and do not commit it; only `src/` and the generated `src/gen/transport_pb.ts` are committed.)

- [ ] **Step 2: Full verification**

Run:
```bash
cd packages/ws-transport && npm run typecheck && npm test
```
Expected: typecheck clean; vitest reports all tests passing across the three test files.

- [ ] **Step 3: Commit any build-config fixes**

```bash
git add -A packages/ws-transport
git commit -m "chore(ts): finalize @gopherex/ws-transport build and tests"
```

---

## Self-Review (completed during planning)

- **Spec coverage:**
  - One WS connection, binary `Frame` messages, marshaled via `@bufbuild/protobuf` v2 against generated `transport_pb.ts` — Task 2 (generation) + Task 4 (codec) ✓.
  - Client-initiated, monotonic ODD stream ids (1, 3, 5, …) — `Mux.openStream` (Task 7) + the "monotonic odd stream ids" test (Task 8) ✓.
  - Per-RPC flow OPEN → MSG\* → HALF_CLOSE → MSG\* → END, plus RST abort — Mux routing (Task 7) and all four stream-kind tests + RST tests (Task 8) ✓.
  - Untyped payload (`Uint8Array`) — `ClientStream.send/recv` and `StreamImpl` (Task 6) ✓.
  - Connection mux holding `Map<number, StreamImpl>`; `onmessage` decodes Frame and routes by `stream_id` (MSG → queue push; END/RST → resolve/reject + delete; OPEN ignored); send path encodes Frame→bytes → `ws.send`; pre-open send buffering; per-stream `AsyncQueue` bridging push→pull — all in `mux.ts` + `queue.ts` (Tasks 3, 7) ✓.
  - Unbounded queue / simple backpressure, documented — `queue.ts` doc comment (Task 3) ✓.
  - `FakeSocket implements WebSocketLike` that plays the server side (captures sent Frames, injects inbound Frames); tests cover all four kinds + non-OK END → `WsStatusError` + RST → error; Frames encoded/decoded via `@bufbuild/protobuf` + generated schema — Task 8 ✓.
  - Generation step via buf + `@bufbuild/protoc-gen-es` devDependency importing `FrameSchema`, `KindSchema` — Task 2 (`buf.gen.yaml`, `generate:proto` script) ✓.
  - Package setup: `package.json` (name `@gopherex/ws-transport`, `"type": "module"`, exports, dep `@bufbuild/protobuf`@^2, devDeps `vitest`, `@bufbuild/protoc-gen-es`@^2, `typescript`), `tsconfig.json`, `src/` files (`frame.ts`, `mux.ts`, `transport.ts`, `stream.ts`, `status.ts`, `index.ts`), `src/gen/transport_pb.ts` (committed), `buf.gen.yaml`, `*.test.ts` — Tasks 1, 2, 4–8 ✓.

- **Placeholder scan:** every code step shows complete real TypeScript — `AsyncQueue` (full push/pull/end/fail), the mux `handleMessage` routing switch (full), `StreamImpl` (full async-iterator + headers promise), `FakeSocket` (full send-capture + inject). No `TODO`, no `...`, no stubbed bodies. The one intentional Task-7→Task-8 forward reference (`FakeSocket` import in `index.ts`) is called out inline with the expected interim typecheck error.

- **API-name consistency with the fixed contract:**
  - `WebSocketLike` (interface) — defined in `mux.ts`, re-exported via `transport.ts` and `index.ts` ✓.
  - `WsStatusError extends Error { readonly code: number; readonly details: Uint8Array[]; constructor(code, message, details?) }` — `status.ts`, exported ✓.
  - `WsTransport { constructor(url: string); static fromSocket(ws): WsTransport; openStream(method, headers?): ClientStream; close(): void }` — `transport.ts` ✓ (the second overload signature is non-public, used only by `fromSocket`).
  - `ClientStream { send(Uint8Array): void; closeSend(): void; recv(): Promise<Uint8Array | null>; responseHeaders(): Promise<Record<string,string>>; [Symbol.asyncIterator](): AsyncIterator<Uint8Array> }` — `stream.ts`, matches exactly ✓.
  - `recv()` returns `null` on clean END(ok) and throws `WsStatusError` on non-OK END / RST — verified by the unary/server-stream (null) and error/RST (throw) tests ✓.
  - Enum member names match the FIXED wire contract: `KIND_UNSPECIFIED=0, KIND_OPEN=1, KIND_MSG=2, KIND_HALF_CLOSE=3, KIND_END=4, KIND_RST=5` (consistent with Plan 1's `transport.proto`) — all `Kind.*` references use these names ✓.

- **Known risks flagged inline:** native `WebSocket` global availability for the dial constructor (Task 7 Step 2 note — use `fromSocket` elsewhere); `import_extension=js` / `target=ts` plugin options required for NodeNext ESM (Task 2 Step 1 note); possible `Status` init cast vs `create(StatusSchema, …)` fallback (Task 4 Step 3 note); optional injection-timing stabilizer tick (Task 8 Step 3 note); `dist/` commit-vs-ignore decision (Task 9 Step 1 note).
