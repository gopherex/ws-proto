import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, execFileSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { tmpdir } from "node:os";
import { mkdtempSync, rmSync } from "node:fs";

// Node WebSocket polyfill for the browser-oriented @gopherex/ws-transport.
import { WebSocket as WsImpl } from "ws";
if (typeof (globalThis as any).WebSocket === "undefined") {
  (globalThis as any).WebSocket = WsImpl;
}

import { WsTransport } from "@gopherex/ws-transport";
import { create } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";

const here = dirname(fileURLToPath(import.meta.url));
const pkgRoot = resolve(here, ".."); // packages/protoc-gen-ws-es
const repoRoot = resolve(here, "..", "..", ".."); // packages/protoc-gen-ws-es -> repo root

// Generated symbols are loaded dynamically in beforeAll, after we (re)generate
// them, so this file can be collected even when test/gen does not exist yet.
let EchoServiceClient: typeof import("./gen/echo_ws_pb.js").EchoServiceClient;
let UnaryRequestSchema: GenMessage<any>;
let ServerStreamRequestSchema: GenMessage<any>;
let ClientStreamRequestSchema: GenMessage<any>;
let BidiRequestSchema: GenMessage<any>;
const ADDR = "127.0.0.1:8910";
const WS_URL = `ws://${ADDR}/`;

let server: ChildProcessWithoutNullStreams;
let buildDir = "";

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
  // Ensure the generated client/messages exist (the unit-test file may have
  // cleaned test/gen; generation is idempotent), then load them dynamically.
  execFileSync("npx", ["buf", "generate", "--template", "test/buf.gen.test.yaml"], {
    cwd: pkgRoot,
    stdio: "inherit",
  });
  ({ EchoServiceClient } = await import("./gen/echo_ws_pb.js"));
  ({
    UnaryRequestSchema,
    ServerStreamRequestSchema,
    ClientStreamRequestSchema,
    BidiRequestSchema,
  } = await import("./gen/echo_pb.js"));

  // Build the Go server to a real binary first so killing the spawned process
  // actually terminates the listener (`go run` forks a separate child that can
  // outlive the wrapper and leak the bound port between runs).
  buildDir = mkdtempSync(resolve(tmpdir(), "ws-proto-server-"));
  const bin = resolve(buildDir, "server");
  execFileSync("go", ["build", "-o", bin, "./example/server"], {
    cwd: repoRoot,
    stdio: "inherit",
  });

  server = spawn(bin, [], {
    cwd: repoRoot,
    env: { ...process.env, WS_PROTO_TEST_ADDR: ADDR },
  }) as ChildProcessWithoutNullStreams;
  await waitForListening(server);
}, 60000);

afterAll(() => {
  if (server && !server.killed) {
    server.kill("SIGKILL");
  }
  if (buildDir) {
    rmSync(buildDir, { recursive: true, force: true });
  }
});

describe("TS client <-> Go wsrpc server", () => {
  it("unary: Unary", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new EchoServiceClient(transport);
      const res = await client.unary(create(UnaryRequestSchema, { name: "bob" }));
      expect(res.greeting).toBe("hello bob");
    } finally {
      transport.close();
    }
  });

  it("server streaming: ServerStream", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new EchoServiceClient(transport);
      const got: number[] = [];
      for await (const ev of client.serverStream(create(ServerStreamRequestSchema, { count: 3 }))) {
        got.push(ev.index);
      }
      expect(got).toEqual([0, 1, 2]);
    } finally {
      transport.close();
    }
  });

  it("client streaming: ClientStream", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new EchoServiceClient(transport);
      async function* values() {
        yield create(ClientStreamRequestSchema, { value: 1 });
        yield create(ClientStreamRequestSchema, { value: 2 });
        yield create(ClientStreamRequestSchema, { value: 3 });
        yield create(ClientStreamRequestSchema, { value: 4 });
      }
      const res = await client.clientStream(values());
      expect(res.sum).toBe(10);
    } finally {
      transport.close();
    }
  });

  it("bidi: Bidi", async () => {
    const transport = new WsTransport(WS_URL);
    try {
      const client = new EchoServiceClient(transport);
      async function* outbound() {
        yield create(BidiRequestSchema, { text: "hi" });
        yield create(BidiRequestSchema, { text: "bye" });
      }
      const received: string[] = [];
      for await (const msg of client.bidi(outbound())) {
        received.push(msg.echo);
      }
      expect(received).toEqual(["echo:hi", "echo:bye"]);
    } finally {
      transport.close();
    }
  });
});
