# Plan 4 — `protoc-gen-ws-es` + Cross-Language Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `protoc-gen-ws-es` — a TypeScript `protoc`/`buf` plugin (analog of `protoc-gen-connect-es`) that emits `*_ws_pb.ts` typed service clients interoperating with `protoc-gen-es` v2 output (`*_pb.ts`) — and prove it end-to-end with a node-vs-Go integration test where the generated TS client drives the Plan 2 golden Go `wsrpc` server over a real WebSocket, exercising all four RPC stream kinds.

**Architecture:** The plugin is built on `@bufbuild/protoplugin` (`createEcmaScriptPlugin` + `generateTs(schema, "ts")`). It iterates `schema.files → file.services → service.methods`, and for each service emits (1) a data-driven service descriptor const (method `localName` → `{ kind, input schema, output schema }`) and (2) a typed client class taking a `WsTransport` in its constructor. Each method delegates to `transport.openStream("/pkg.Svc/Method", headers)` and marshals via `toBinary`/`fromBinary` against the schemas imported from the sibling `*_pb.ts`. The integration test launches the golden Go server (`go run ./example/server`) on a fixed port, waits for readiness, constructs the generated client over `new WsTransport(url)`, and asserts results for unary / server-stream / client-stream / bidi.

**Tech Stack:** TypeScript 5, Node 20+, `@bufbuild/protoplugin`@^2, `@bufbuild/protobuf`@^2, `vitest` (dev), `ws` (dev — node WebSocket polyfill for `@gopherex/ws-transport`), `tsx`/`tsc` build. The Go server side reuses Plan 1's `wsrpc/` runtime + Plan 2's generated golden handler. Generation orchestrated via `buf.gen.yaml`. No git branches/worktrees — commit on the current branch. Commit messages carry NO `Co-Authored-By` trailer.

**Constraints from user:** Single current git branch only (never create branches/worktrees). Commit messages must NOT contain a `Co-Authored-By` trailer. WebSocket origin scoped via `OriginPatterns` on the Go side — never disable TLS verification.

**Upstream contracts this plan targets (do NOT redesign):**

- TS runtime `@gopherex/ws-transport` public API (Plan 3):
  ```ts
  class WsTransport {
    constructor(url: string);
    static fromSocket(ws: WebSocket): WsTransport;
    openStream(method: string, headers?: Record<string, string>): ClientStream;
    close(): void;
  }
  interface ClientStream {
    send(payload: Uint8Array): void;
    closeSend(): void;
    recv(): Promise<Uint8Array | null>;
    responseHeaders(): Promise<Record<string, string>>;
    [Symbol.asyncIterator](): AsyncIterator<Uint8Array>;
  }
  class WsStatusError extends Error { code: number; details: Uint8Array[]; }
  ```
- `protoc-gen-es` v2 conventions: import message types + schemas from sibling `*_pb.ts` (`import { GetUserRequestSchema } from "./svc_pb.js"`); use `create`, `toBinary`, `fromBinary` from `@bufbuild/protobuf`. Wire method name is `/pkg.Service/Method` (derived from `service.typeName` + `method.name`).
- Plan 2 produces `example/golden.proto` → a Go golden example with `wsrpc` Handler + `example/server` main. Plan 4 reuses that server binary; it does NOT regenerate Go.

---

## File Structure

| Path | Responsibility |
|---|---|
| `package.json` (repo root) | npm workspaces root listing `packages/*` |
| `packages/protoc-gen-ws-es/package.json` | name `@gopherex/protoc-gen-ws-es`, `bin: protoc-gen-ws-es`, deps |
| `packages/protoc-gen-ws-es/tsconfig.json` | TS compile config (ESM, NodeNext) |
| `packages/protoc-gen-ws-es/src/index.ts` | Plugin entry: `createEcmaScriptPlugin` + `runNodeJs` |
| `packages/protoc-gen-ws-es/src/ws-es.ts` | `generateTs` emission logic (descriptor + client) |
| `packages/protoc-gen-ws-es/bin/protoc-gen-ws-es.js` | shebang launcher pointing at built `dist/index.js` |
| `packages/protoc-gen-ws-es/vitest.config.ts` | vitest config for plugin unit tests |
| `packages/protoc-gen-ws-es/test/golden.proto` | tiny fixture proto (all four method kinds) |
| `packages/protoc-gen-ws-es/test/buf.gen.test.yaml` | buf config to generate fixture into `test/gen/` |
| `packages/protoc-gen-ws-es/test/gen/*` | generated `_pb.ts` + `_ws_pb.ts` fixture output (gitignored) |
| `packages/protoc-gen-ws-es/test/generate.test.ts` | unit test: emitted client structure + `tsc` compiles |
| `packages/protoc-gen-ws-es/test/integration.test.ts` | cross-lang test: TS client ↔ Go `wsrpc` server |
| `packages/protoc-gen-ws-es/test/tsc-check.tsconfig.json` | strict tsconfig used to type-check generated output |
| `buf.gen.yaml` (repo root) | golden `example/` multi-plugin generation entry |

---

## Task 1: Workspace + package scaffolding

**Files:**
- Create/Edit: `package.json` (repo root), `packages/protoc-gen-ws-es/package.json`, `packages/protoc-gen-ws-es/tsconfig.json`, `packages/protoc-gen-ws-es/bin/protoc-gen-ws-es.js`, `.gitignore` (append)

- [ ] **Step 1: Create the repo-root npm workspace**

`package.json` (repo root) — if one already exists from Plan 3, merge the `workspaces` array instead of overwriting:
```json
{
  "name": "ws-proto-workspace",
  "private": true,
  "type": "module",
  "workspaces": [
    "packages/ws-transport",
    "packages/protoc-gen-ws-es"
  ]
}
```

- [ ] **Step 2: Create `packages/protoc-gen-ws-es/package.json`**

```json
{
  "name": "@gopherex/protoc-gen-ws-es",
  "version": "0.1.0",
  "type": "module",
  "description": "protoc/buf plugin emitting WebSocket transport clients for protobuf-es v2",
  "bin": {
    "protoc-gen-ws-es": "./bin/protoc-gen-ws-es.js"
  },
  "files": ["dist", "bin"],
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "default": "./dist/index.js"
    }
  },
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "clean": "rm -rf dist test/gen",
    "test": "vitest run"
  },
  "dependencies": {
    "@bufbuild/protobuf": "^2.2.0",
    "@bufbuild/protoplugin": "^2.2.0"
  },
  "devDependencies": {
    "@bufbuild/buf": "^1.47.0",
    "@bufbuild/protoc-gen-es": "^2.2.0",
    "@gopherex/ws-transport": "*",
    "typescript": "^5.6.0",
    "vitest": "^2.1.0",
    "ws": "^8.18.0",
    "@types/ws": "^8.5.0"
  },
  "engines": {
    "node": ">=20"
  }
}
```

> `@gopherex/ws-transport` is the Plan 3 runtime; it resolves through the workspace. `ws` is a node WebSocket polyfill used only at test time so the generated client can run outside a browser.

- [ ] **Step 3: Create `packages/protoc-gen-ws-es/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "declaration": true,
    "outDir": "dist",
    "rootDir": "src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true
  },
  "include": ["src/**/*.ts"]
}
```

- [ ] **Step 4: Create the bin launcher `packages/protoc-gen-ws-es/bin/protoc-gen-ws-es.js`**

```js
#!/usr/bin/env node
import "../dist/index.js";
```

Make it executable:
```bash
chmod +x /home/yaroher/devel/github/gopherex/ws-proto/packages/protoc-gen-ws-es/bin/protoc-gen-ws-es.js
```

- [ ] **Step 5: Append generated/build artifacts to `.gitignore`**

Append these lines to the repo-root `.gitignore`:
```
node_modules/
packages/*/dist/
packages/protoc-gen-ws-es/test/gen/
```

- [ ] **Step 6: Install workspace deps**

```bash
cd /home/yaroher/devel/github/gopherex/ws-proto
npm install
```
Expected: `node_modules/` populated; `@bufbuild/protoplugin`, `@bufbuild/protobuf`, `@bufbuild/buf` resolvable.

- [ ] **Step 7: Commit**

```bash
git add package.json package-lock.json packages/protoc-gen-ws-es/package.json packages/protoc-gen-ws-es/tsconfig.json packages/protoc-gen-ws-es/bin/protoc-gen-ws-es.js .gitignore
git commit -m "chore: scaffold protoc-gen-ws-es package and workspace"
```

---

## Task 2: Plugin entry (`src/index.ts`)

**Files:**
- Create: `packages/protoc-gen-ws-es/src/index.ts`

The entry wires `createEcmaScriptPlugin` to the (not-yet-written) `generateTs` logic and runs the plugin over stdin/stdout via `runNodeJs`. It is exercised by Task 4's generation test, so build verification only here.

- [ ] **Step 1: Implement `packages/protoc-gen-ws-es/src/index.ts`**

```ts
import { createEcmaScriptPlugin, runNodeJs } from "@bufbuild/protoplugin";
import { generateTs } from "./ws-es.js";

export const protocGenWsEs = createEcmaScriptPlugin({
  name: "protoc-gen-ws-es",
  version: "v0.1.0",
  // We only emit TypeScript; protoc-gen-es itself handles js/dts of messages.
  generateTs,
});

runNodeJs(protocGenWsEs);
```

- [ ] **Step 2: Verify it does not yet build (missing `ws-es.ts`)**

Run:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto/packages/protoc-gen-ws-es
npx tsc -p tsconfig.json --noEmit
```
Expected: FAIL — `Cannot find module './ws-es.js'`. This is expected; implemented in Task 3.

---

## Task 3: Emission logic (`src/ws-es.ts`)

**Files:**
- Create: `packages/protoc-gen-ws-es/src/ws-es.ts`

This is the heart of the plugin: real `@bufbuild/protoplugin` code emitting the descriptor const and the typed client class for every service, covering all four `methodKind` values.

- [ ] **Step 1: Implement `packages/protoc-gen-ws-es/src/ws-es.ts` (FULL REAL CODE)**

```ts
import type { Schema, GeneratedFile } from "@bufbuild/protoplugin";
import type { DescService, DescMethod } from "@bufbuild/protobuf";

// generateTs is invoked once per plugin run with the full schema. For each
// proto file that declares services we emit a sibling "<name>_ws_pb.ts" next to
// protoc-gen-es's "<name>_pb.ts".
export function generateTs(schema: Schema): void {
  for (const file of schema.files) {
    if (file.services.length === 0) {
      continue; // nothing to generate for service-less files
    }
    const f = schema.generateFile(file.name + "_ws_pb.ts");
    f.preamble(file);

    // Runtime imports: WsTransport + ClientStream from @gopherex/ws-transport.
    const WsTransport = f.import("WsTransport", "@gopherex/ws-transport");
    const ClientStream = f.import("ClientStream", "@gopherex/ws-transport");
    // protobuf-es v2 codec functions.
    const create = f.import("create", "@bufbuild/protobuf");
    const toBinary = f.import("toBinary", "@bufbuild/protobuf");
    const fromBinary = f.import("fromBinary", "@bufbuild/protobuf");

    for (const service of file.services) {
      printDescriptor(f, service);
      printClient(f, service, {
        WsTransport,
        ClientStream,
        create,
        toBinary,
        fromBinary,
      });
    }
  }
}

interface RuntimeImports {
  WsTransport: ReturnType<GeneratedFile["import"]>;
  ClientStream: ReturnType<GeneratedFile["import"]>;
  create: ReturnType<GeneratedFile["import"]>;
  toBinary: ReturnType<GeneratedFile["import"]>;
  fromBinary: ReturnType<GeneratedFile["import"]>;
}

// Fully-qualified wire path "/pkg.Service/Method".
function wirePath(service: DescService, method: DescMethod): string {
  return `/${service.typeName}/${method.name}`;
}

// Map protobuf-es methodKind to a stable string we emit into the descriptor.
function kindLiteral(method: DescMethod): string {
  switch (method.methodKind) {
    case "unary":
      return "unary";
    case "server_streaming":
      return "server_streaming";
    case "client_streaming":
      return "client_streaming";
    case "bidi_streaming":
      return "bidi_streaming";
  }
}

// Emit `export const FooServiceDescriptor = { ... } as const;`
// A data-driven map: localName -> { kind, path, input schema, output schema }.
function printDescriptor(f: GeneratedFile, service: DescService): void {
  const descName = service.name + "Descriptor";
  f.print(f.jsDoc(service));
  f.print(f.export("const", descName), " = {");
  f.print("  typeName: ", f.string(service.typeName), ",");
  f.print("  methods: {");
  for (const method of service.methods) {
    const inputSchema = f.importSchema(method.input);
    const outputSchema = f.importSchema(method.output);
    f.print("    ", method.localName, ": {");
    f.print("      kind: ", f.string(kindLiteral(method)), ",");
    f.print("      path: ", f.string(wirePath(service, method)), ",");
    f.print("      input: ", inputSchema, ",");
    f.print("      output: ", outputSchema, ",");
    f.print("    },");
  }
  f.print("  },");
  f.print("} as const;");
  f.print();
}

function printClient(
  f: GeneratedFile,
  service: DescService,
  rt: RuntimeImports,
): void {
  const className = service.name + "Client";
  f.print(f.jsDoc(service));
  f.print(f.export("class", className), " {");
  f.print("  constructor(private readonly transport: ", rt.WsTransport, ") {}");
  f.print();

  for (const method of service.methods) {
    switch (method.methodKind) {
      case "unary":
        printUnary(f, service, method, rt);
        break;
      case "server_streaming":
        printServerStreaming(f, service, method, rt);
        break;
      case "client_streaming":
        printClientStreaming(f, service, method, rt);
        break;
      case "bidi_streaming":
        printBidiStreaming(f, service, method, rt);
        break;
    }
    f.print();
  }
  f.print("}");
  f.print();
}

// unary: async getUser(req, headers?): Promise<Res>
function printUnary(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
  rt: RuntimeImports,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async ", method.localName, "(req: ", inT, ", headers?: Record<string, string>): Promise<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  f.print("    stream.send(", rt.toBinary, "(", inSchema, ", req));");
  f.print("    stream.closeSend();");
  f.print("    const bytes = await stream.recv();");
  f.print("    if (bytes === null) {");
  f.print("      throw new Error(", f.string(`${service.typeName}.${method.name}: server closed stream without a response`), ");");
  f.print("    }");
  f.print("    return ", rt.fromBinary, "(", outSchema, ", bytes);");
  f.print("  }");
}

// server_streaming: watch(req, headers?): AsyncIterable<Res>
function printServerStreaming(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
  rt: RuntimeImports,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async *", method.localName, "(req: ", inT, ", headers?: Record<string, string>): AsyncIterable<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  f.print("    stream.send(", rt.toBinary, "(", inSchema, ", req));");
  f.print("    stream.closeSend();");
  f.print("    for await (const bytes of stream) {");
  f.print("      yield ", rt.fromBinary, "(", outSchema, ", bytes);");
  f.print("    }");
  f.print("  }");
}

// client_streaming: upload(reqs, headers?): Promise<Res>
function printClientStreaming(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
  rt: RuntimeImports,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async ", method.localName, "(reqs: AsyncIterable<", inT, ">, headers?: Record<string, string>): Promise<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  f.print("    for await (const req of reqs) {");
  f.print("      stream.send(", rt.toBinary, "(", inSchema, ", req));");
  f.print("    }");
  f.print("    stream.closeSend();");
  f.print("    const bytes = await stream.recv();");
  f.print("    if (bytes === null) {");
  f.print("      throw new Error(", f.string(`${service.typeName}.${method.name}: server closed stream without a response`), ");");
  f.print("    }");
  f.print("    return ", rt.fromBinary, "(", outSchema, ", bytes);");
  f.print("  }");
}

// bidi_streaming: chat(reqs, headers?): AsyncIterable<Res>
function printBidiStreaming(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
  rt: RuntimeImports,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async *", method.localName, "(reqs: AsyncIterable<", inT, ">, headers?: Record<string, string>): AsyncIterable<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  // Pump request messages concurrently so the read loop below can interleave.
  f.print("    const pump = (async () => {");
  f.print("      for await (const req of reqs) {");
  f.print("        stream.send(", rt.toBinary, "(", inSchema, ", req));");
  f.print("      }");
  f.print("      stream.closeSend();");
  f.print("    })();");
  f.print("    try {");
  f.print("      for await (const bytes of stream) {");
  f.print("        yield ", rt.fromBinary, "(", outSchema, ", bytes);");
  f.print("      }");
  f.print("    } finally {");
  f.print("      await pump;");
  f.print("    }");
  f.print("  }");
}
```

> Notes for the implementer:
> - `f.importShape(desc)` imports the TS message *type* (e.g. `GetUserRequest`); `f.importSchema(desc)` imports the schema object (e.g. `GetUserRequestSchema`) — both resolve to the protoc-gen-es `*_pb.ts` sibling via protoplugin's import tracking, which emits the `import { ... } from "./golden_pb.js"` line automatically.
> - `f.string(x)` emits a properly quoted/escaped string literal. `f.export("class", name)` / `f.export("const", name)` emit `export class Name` / `export const name`.
> - `create` is imported for API symmetry / future use; if `tsc` in Task 4 flags it as unused, drop the `const create = ...` import line and the `create` field from `RuntimeImports`. protoplugin only emits imports that are actually referenced, so an unreferenced `f.import` is harmless at emit time but keep the source tidy.
> - `ClientStream` is imported for the descriptor/type surface; if unreferenced after Task 4, remove it the same way.

- [ ] **Step 2: Build the plugin**

Run:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto/packages/protoc-gen-ws-es
npm run build
```
Expected: `dist/index.js`, `dist/ws-es.js` produced, no `tsc` errors.

- [ ] **Step 3: Commit**

```bash
git add packages/protoc-gen-ws-es/src/index.ts packages/protoc-gen-ws-es/src/ws-es.ts
git commit -m "feat: protoc-gen-ws-es plugin emitting ws_pb.ts clients for all four kinds"
```

---

## Task 4: Generation unit test (emit + tsc compiles)

**Files:**
- Create: `packages/protoc-gen-ws-es/test/golden.proto`, `packages/protoc-gen-ws-es/test/buf.gen.test.yaml`, `packages/protoc-gen-ws-es/test/tsc-check.tsconfig.json`, `packages/protoc-gen-ws-es/vitest.config.ts`, `packages/protoc-gen-ws-es/test/generate.test.ts`

- [ ] **Step 1: Create the fixture proto `packages/protoc-gen-ws-es/test/golden.proto`**

Match Plan 2's golden service shape so the same wire paths are exercised by the integration test.
```proto
syntax = "proto3";

package example.v1;

message GetUserRequest { string id = 1; }
message GetUserResponse { string id = 1; string name = 2; }

message WatchRequest { string prefix = 1; }
message Event { string id = 1; string kind = 2; }

message Chunk { bytes data = 1; }
message UploadSummary { uint64 total_bytes = 1; uint32 chunks = 2; }

message ChatMessage { string from = 1; string text = 2; }

service UserService {
  // unary
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  // server streaming
  rpc Watch(WatchRequest) returns (stream Event);
  // client streaming
  rpc Upload(stream Chunk) returns (UploadSummary);
  // bidi
  rpc Chat(stream ChatMessage) returns (stream ChatMessage);
}
```

- [ ] **Step 2: Create `packages/protoc-gen-ws-es/test/buf.gen.test.yaml`**

This runs `protoc-gen-es` (messages) THEN `protoc-gen-ws-es` (clients) into `test/gen/`.
```yaml
version: v2
inputs:
  - directory: test
plugins:
  - local: ./node_modules/.bin/protoc-gen-es
    out: test/gen
    opt: target=ts
  - local: ./bin/protoc-gen-ws-es.js
    out: test/gen
    opt: target=ts
```

- [ ] **Step 3: Create the strict type-check config `packages/protoc-gen-ws-es/test/tsc-check.tsconfig.json`**

Used by the test to confirm the generated `*_ws_pb.ts` actually compiles against the real runtime + protobuf-es types.
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "types": []
  },
  "include": ["gen/**/*.ts"]
}
```

- [ ] **Step 4: Create `packages/protoc-gen-ws-es/vitest.config.ts`**

```ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
```

- [ ] **Step 5: Write the failing generation test `packages/protoc-gen-ws-es/test/generate.test.ts`**

```ts
import { describe, it, expect, beforeAll } from "vitest";
import { execFileSync } from "node:child_process";
import { readFileSync, existsSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const pkgRoot = resolve(here, "..");
const genFile = resolve(here, "gen/golden_ws_pb.ts");

function run(cmd: string, args: string[]): void {
  execFileSync(cmd, args, { cwd: pkgRoot, stdio: "inherit" });
}

describe("protoc-gen-ws-es generation", () => {
  beforeAll(() => {
    rmSync(resolve(here, "gen"), { recursive: true, force: true });
    run("npm", ["run", "build"]);
    // buf must see the built plugin bin as executable.
    run("npx", ["buf", "generate", "--template", "test/buf.gen.test.yaml"]);
  });

  it("emits the sibling _ws_pb.ts", () => {
    expect(existsSync(genFile)).toBe(true);
    expect(existsSync(resolve(here, "gen/golden_pb.ts"))).toBe(true);
  });

  it("emits a client class and descriptor", () => {
    const src = readFileSync(genFile, "utf8");
    expect(src).toContain("export class UserServiceClient");
    expect(src).toContain("export const UserServiceDescriptor");
    expect(src).toContain('"/example.v1.UserService/GetUser"');
  });

  it("emits the correct signature for each method kind", () => {
    const src = readFileSync(genFile, "utf8");
    // unary -> Promise
    expect(src).toMatch(/async getUser\(req: GetUserRequest[^)]*\): Promise<GetUserResponse>/);
    // server streaming -> async generator
    expect(src).toMatch(/async \*watch\(req: WatchRequest[^)]*\): AsyncIterable<Event>/);
    // client streaming -> Promise from AsyncIterable input
    expect(src).toMatch(/async upload\(reqs: AsyncIterable<Chunk>[^)]*\): Promise<UploadSummary>/);
    // bidi -> async generator from AsyncIterable input
    expect(src).toMatch(/async \*chat\(reqs: AsyncIterable<ChatMessage>[^)]*\): AsyncIterable<ChatMessage>/);
  });

  it("imports schemas from the protoc-gen-es sibling", () => {
    const src = readFileSync(genFile, "utf8");
    expect(src).toMatch(/from "\.\/golden_pb\.js"/);
    expect(src).toContain("GetUserRequestSchema");
    expect(src).toContain("GetUserResponseSchema");
  });

  it("type-checks against the real runtime (tsc)", () => {
    // Throws (non-zero exit) if the generated code does not compile.
    run("npx", ["tsc", "-p", "test/tsc-check.tsconfig.json"]);
  });
});
```

- [ ] **Step 6: Run to verify it fails**

Run:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto/packages/protoc-gen-ws-es
npx vitest run test/generate.test.ts
```
Expected first run: may FAIL if (a) `@gopherex/ws-transport` (Plan 3) is not yet built — build it first via `npm -w @gopherex/ws-transport run build`; or (b) the `tsc` step flags an unused `create`/`ClientStream` import. Resolve per the Task 3 Step 1 notes (drop unused imports), then re-run.

- [ ] **Step 7: Make it pass**

Iterate until all five assertions pass. Likely fixes:
- Build the Plan 3 runtime so `@gopherex/ws-transport` resolves: `npm -w @gopherex/ws-transport run build`.
- If `tsc` reports `create`/`ClientStream` unused, remove those `f.import` lines from `ws-es.ts` and rebuild the plugin.
- Confirm `protoc-gen-es` emitted `golden_pb.ts` (proves plugin ordering in `buf.gen.test.yaml`).

Run again, expected PASS:
```bash
npx vitest run test/generate.test.ts
```

- [ ] **Step 8: Commit**

```bash
git add packages/protoc-gen-ws-es/test/golden.proto packages/protoc-gen-ws-es/test/buf.gen.test.yaml packages/protoc-gen-ws-es/test/tsc-check.tsconfig.json packages/protoc-gen-ws-es/vitest.config.ts packages/protoc-gen-ws-es/test/generate.test.ts packages/protoc-gen-ws-es/src/ws-es.ts
git commit -m "test: protoc-gen-ws-es generation unit test + tsc verification"
```

---

## Task 5: Root `buf.gen.yaml` golden entry

**Files:**
- Edit/Create: `buf.gen.yaml` (repo root)

This is the production generation pipeline for the golden `example/` proto (from Plan 2). It runs all four plugins in dependency order: Go message types, Go ws handlers, TS message types, TS ws clients.

- [ ] **Step 1: Write/extend `buf.gen.yaml`**

If Plan 2 already created `buf.gen.yaml` with the two Go plugins, ADD the two TS plugin entries; otherwise create the full file:
```yaml
version: v2
inputs:
  - directory: example
plugins:
  # --- Go ---
  - local: protoc-gen-go
    out: example/gen
    opt: paths=source_relative
  - local: protoc-gen-go-ws
    out: example/gen
    opt: paths=source_relative
  # --- TypeScript ---
  - local: ./node_modules/.bin/protoc-gen-es
    out: packages/protoc-gen-ws-es/test/gen   # adjust to the real TS consumer dir
    opt: target=ts
  - local: ./packages/protoc-gen-ws-es/bin/protoc-gen-ws-es.js
    out: packages/protoc-gen-ws-es/test/gen
    opt: target=ts
```

> The TS `out` here points at the test consumer dir used by the integration test in Task 6. If Plan 3 establishes a canonical `example/gen-ts/` dir, point both TS plugins there instead and update the integration test import path accordingly. The Go entries are owned by Plan 2 — do not change their `out`/`opt` if already present.

- [ ] **Step 2: Sanity-run the TS half of the pipeline**

The Go plugins require Plan 2's binaries on `PATH`; the TS half can be validated independently:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto
npx buf generate --template packages/protoc-gen-ws-es/test/buf.gen.test.yaml
```
Expected: regenerates `packages/protoc-gen-ws-es/test/gen/golden_pb.ts` + `golden_ws_pb.ts`.

- [ ] **Step 3: Commit**

```bash
git add buf.gen.yaml
git commit -m "build: add TS ws-es plugins to buf.gen.yaml golden pipeline"
```

---

## Task 6: Cross-language integration test (node TS client ↔ Go `wsrpc` server)

**Files:**
- Create: `packages/protoc-gen-ws-es/test/integration.test.ts`

This is the headline deliverable: the generated TS client driving the real Go golden server over a real WebSocket, exercising all four method kinds.

**Preconditions (documented at the top of the test):**
- Plan 2's golden Go server exists at `example/server/main.go` and serves the `UserService` `wsrpc` handler. It MUST: (a) listen on `WS_PROTO_TEST_ADDR` (env, e.g. `127.0.0.1:8910`) or a fixed `:8910`; (b) print `LISTENING <addr>` to stdout once ready; (c) scope WebSocket origins via `OriginPatterns: []string{"127.0.0.1:*", "localhost:*"}` in its `websocket.Accept` call (do NOT disable TLS verification — this is CSRF/origin scoping only).
- The golden server's `UserService` implements: `GetUser` (echo id→name), `Watch` (emits N events for a prefix), `Upload` (sums chunk byte counts), `Chat` (echo each message back).

> If Plan 2's `example/server/main.go` does not yet emit `LISTENING <addr>` and read `WS_PROTO_TEST_ADDR`, add those two lines as part of executing this task (it is a tiny, test-only readiness contract; record the change in the Plan 2 example, not as new product behavior).

- [ ] **Step 1: Provide a node WebSocket global**

`@gopherex/ws-transport` uses the browser-native `WebSocket`. Node 20 has a global `WebSocket`, but to be robust across versions the test installs the `ws` polyfill if absent. This goes inside the test file (Step 2). No separate file needed.

- [ ] **Step 2: Write the integration test `packages/protoc-gen-ws-es/test/integration.test.ts`**

```ts
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

// Node WebSocket polyfill for the browser-oriented @gopherex/ws-transport.
import { WebSocket as WsImpl } from "ws";
if (typeof (globalThis as any).WebSocket === "undefined") {
  (globalThis as any).WebSocket = WsImpl;
}

import { WsTransport } from "@gopherex/ws-transport";
import {
  UserServiceClient,
} from "./gen/golden_ws_pb.js";
import {
  create,
} from "@bufbuild/protobuf";
import {
  ChunkSchema,
  ChatMessageSchema,
} from "./gen/golden_pb.js";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..", ".."); // packages/protoc-gen-ws-es -> repo root
const ADDR = "127.0.0.1:8910";
const WS_URL = `ws://${ADDR}/`;

let server: ChildProcessWithoutNullStreams;

function waitForListening(proc: ChildProcessWithoutNullStreams): Promise<void> {
  return new Promise((res, rej) => {
    const timer = setTimeout(() => rej(new Error("server did not become ready in 20s")), 20000);
    proc.stdout.on("data", (buf: Buffer) => {
      const line = buf.toString();
      process.stdout.write(`[go-server] ${line}`);
      if (line.includes("LISTENING")) {
        clearTimeout(timer);
        res();
      }
    });
    proc.stderr.on("data", (buf: Buffer) => process.stderr.write(`[go-server:err] ${buf}`));
    proc.on("exit", (code) => {
      clearTimeout(timer);
      rej(new Error(`server exited early with code ${code}`));
    });
  });
}

beforeAll(async () => {
  server = spawn("go", ["run", "./example/server"], {
    cwd: repoRoot,
    env: { ...process.env, WS_PROTO_TEST_ADDR: ADDR },
  }) as ChildProcessWithoutNullStreams;
  await waitForListening(server);
}, 60000);

afterAll(() => {
  if (server && !server.killed) {
    server.kill("SIGKILL");
  }
});

describe("TS client <-> Go wsrpc server", () => {
  it("unary: GetUser", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new UserServiceClient(transport);
      const res = await client.getUser({ id: "u1" } as any);
      expect(res.id).toBe("u1");
      expect(res.name.length).toBeGreaterThan(0);
    } finally {
      transport.close();
    }
  });

  it("server streaming: Watch", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new UserServiceClient(transport);
      const got: string[] = [];
      for await (const ev of client.watch({ prefix: "p" } as any)) {
        got.push(ev.id);
      }
      expect(got.length).toBeGreaterThan(0);
      expect(got.every((id) => id.startsWith("p"))).toBe(true);
    } finally {
      transport.close();
    }
  });

  it("client streaming: Upload", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new UserServiceClient(transport);
      async function* chunks() {
        yield create(ChunkSchema, { data: new Uint8Array([1, 2, 3]) });
        yield create(ChunkSchema, { data: new Uint8Array([4, 5]) });
      }
      const summary = await client.upload(chunks());
      expect(Number(summary.totalBytes)).toBe(5);
      expect(summary.chunks).toBe(2);
    } finally {
      transport.close();
    }
  });

  it("bidi: Chat", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new UserServiceClient(transport);
      async function* outbound() {
        yield create(ChatMessageSchema, { from: "a", text: "hi" });
        yield create(ChatMessageSchema, { from: "a", text: "bye" });
      }
      const received: string[] = [];
      for await (const msg of client.chat(outbound())) {
        received.push(msg.text);
      }
      expect(received).toEqual(["hi", "bye"]);
    } finally {
      transport.close();
    }
  });
});
```

> Implementation notes:
> - The `as any` casts on plain-object requests are only to keep the test concise; the generated signatures accept the precise `MessageInitShape`-style object that protobuf-es `create` accepts. If `tsc`/vitest's transform rejects the bare object, replace with `create(GetUserRequestSchema, { id: "u1" })` and import `GetUserRequestSchema` from `./gen/golden_pb.js`.
> - `summary.totalBytes` is a `bigint` (proto `uint64`), hence `Number(...)`.
> - The Go server is started ONCE for the suite (`beforeAll`) and torn down in `afterAll`. Each test opens its own `WsTransport` to keep stream-id spaces independent and isolate failures.

- [ ] **Step 3: Ensure generated output exists, then run to verify it fails (or passes)**

Run:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto
npm -w @gopherex/ws-transport run build          # Plan 3 runtime
npm -w @gopherex/protoc-gen-ws-es run build       # this plugin
npx buf generate --template packages/protoc-gen-ws-es/test/buf.gen.test.yaml
go build ./example/...                            # confirm Plan 2 server compiles
npx vitest run packages/protoc-gen-ws-es/test/integration.test.ts
```
Expected first run: FAIL if the Go server lacks the `LISTENING`/`WS_PROTO_TEST_ADDR` readiness contract, or if any handler behavior differs. Apply the minimal Plan 2 server adjustments (readiness print + env addr + `OriginPatterns`) and re-run.

- [ ] **Step 4: Make it pass**

Iterate until all four tests pass. Debugging guide:
- Connection refused → server not listening; verify `LISTENING` printed and `ADDR` matches.
- WebSocket handshake 403 → add `OriginPatterns: []string{"127.0.0.1:*", "localhost:*"}` to the Go `websocket.Accept` options (do NOT disable TLS verification).
- `recv()` returns null on unary → server sent `END` without a `MSG`; check the Go handler returns a response before returning nil.
- bidi hangs → ensure the generated `chat` pump calls `closeSend()` after the request iterable drains, and the Go handler returns (sends `END`) after observing `HALF_CLOSE`.

Run again, expected PASS:
```bash
npx vitest run packages/protoc-gen-ws-es/test/integration.test.ts
```

- [ ] **Step 5: Commit**

```bash
git add packages/protoc-gen-ws-es/test/integration.test.ts
git commit -m "test: cross-language integration test (TS client vs Go wsrpc server)"
```

---

## Task 7: Finalize

- [ ] **Step 1: Full TS verification**

Run:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto
npm -w @gopherex/protoc-gen-ws-es run build
npx vitest run --root packages/protoc-gen-ws-es
```
Expected: generation unit test + integration test all PASS.

- [ ] **Step 2: Full repo verification (Go + TS)**

Run:
```bash
make test           # Go: gofmt/vet/test (Plan 1 target)
npm test --workspaces --if-present
```
Expected: Go clean, all TS workspaces' tests pass.

- [ ] **Step 3: Commit any cleanups**

```bash
git add -A
git commit -m "chore: finalize protoc-gen-ws-es and cross-language integration"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** `protoc-gen-ws-es` TS plugin on `@bufbuild/protoplugin` (Tasks 2–3) ✓; emits `*_ws_pb.ts` per-service descriptor const + typed client (Task 3) ✓; all four method kinds with the exact required signatures — unary `Promise<Res>`, server-stream `AsyncIterable<Res>`, client-stream `Promise<Res>` from `AsyncIterable<Req>`, bidi `AsyncIterable<Res>` from `AsyncIterable<Req>` (Task 3) ✓; interops with protoc-gen-es v2 (imports schemas/shapes from sibling `*_pb.ts`, uses `toBinary`/`fromBinary`/`create`) ✓; lives at `packages/protoc-gen-ws-es/` with `package.json` bin + tsconfig + `src/index.ts` + `src/ws-es.ts` + build script (Task 1–3) ✓; `buf.gen.yaml` runs `protoc-gen-es` then `protoc-gen-ws-es` (Tasks 4–5) ✓; unit test asserts emitted signatures AND `tsc`-compiles the output (Task 4) ✓; cross-lang integration test launches `go run ./example/server`, waits for readiness, drives all four kinds over a real `ws://`, tears down (Task 6) ✓; origin scoped via `OriginPatterns`, TLS verification never disabled (Task 6) ✓.
- **Placeholder scan:** No `TODO`, no `...`, no stubbed function bodies. Every code block (`index.ts`, full `ws-es.ts` with real `f.print`/`f.import`/`f.importShape`/`f.importSchema` lines for unary + all three streaming kinds, both test files, all configs) is complete and runnable. Inline "Notes" call out the only two discretionary trims (unused `create`/`ClientStream` imports; `as any` test casts) with the exact concrete replacement.
- **API-name consistency with the fixed runtime contract:** Generated client calls only `transport.openStream(method, headers)`, `stream.send(Uint8Array)`, `stream.closeSend()`, `await stream.recv()` (null check), and `for await (const bytes of stream)` — exactly the Plan 3 `WsTransport`/`ClientStream` surface. `new UserServiceClient(transport)` matches the required constructor. Wire path `/${service.typeName}/${method.name}` = `/example.v1.UserService/GetUser`, matching Plan 1's `/pkg.Service/Method` routing and Plan 2's Go handler registration. `methodKind` string literals (`unary`/`server_streaming`/`client_streaming`/`bidi_streaming`) match protobuf-es v2 exactly. protobuf-es symbols (`create`, `toBinary`, `fromBinary`, `*Schema`) match v2 conventions. No `WsStatusError` is constructed by the generator — it is thrown by the runtime and propagates through `recv()`/iteration, consistent with the Plan 3 contract.
- **Cross-plan boundaries respected:** Plan 4 consumes (not redefines) the Plan 3 `@gopherex/ws-transport` API and the Plan 2 golden Go server; the only Plan 2 touch is the documented test-only readiness contract (`LISTENING` print + `WS_PROTO_TEST_ADDR` + `OriginPatterns`), explicitly flagged in Task 6.
```
