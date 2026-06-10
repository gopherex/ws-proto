import { describe, it, expect, beforeAll } from "vitest";
import { WsTransport, FakeSocket, WsStatusError, Kind } from "@gopherex/ws-proto-transport";
import type { Interceptor } from "@gopherex/ws-proto-transport";
import { create } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";

// Drives the generated EchoServiceClient over an in-memory FakeSocket so we can
// exercise the error / null-END / request-source-failure edges the live Go
// integration test does not cover. Generated symbols are imported dynamically
// (test/gen is produced by generate.test.ts's beforeAll, which runs first since
// fileParallelism is disabled).
let EchoServiceClient: typeof import("./gen/echo_ws_pb.js").EchoServiceClient;
let UnaryRequestSchema: GenMessage<any>;
let ServerStreamRequestSchema: GenMessage<any>;
let ClientStreamRequestSchema: GenMessage<any>;

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

beforeAll(async () => {
  const ws = await import("./gen/echo_ws_pb.js");
  EchoServiceClient = ws.EchoServiceClient;
  const pb = await import("./gen/echo_pb.js");
  UnaryRequestSchema = pb.UnaryRequestSchema as GenMessage<any>;
  ServerStreamRequestSchema = pb.ServerStreamRequestSchema as GenMessage<any>;
  ClientStreamRequestSchema = pb.ClientStreamRequestSchema as GenMessage<any>;
});

describe("generated client error edges", () => {
  it("unary rejects when the server ends the stream without a response", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }));
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    await expect(p).rejects.toThrow(/without a response/);
  });

  it("unary rejects with WsStatusError on a non-OK END", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }));
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 7, message: "denied" } });
    await expect(p).rejects.toBeInstanceOf(WsStatusError);
  });

  it("runs a transport interceptor through the generated client (auth + message transform)", async () => {
    const seen: string[] = [];
    const auth: Interceptor = (next) => async (req) => {
      req.header["authorization"] = "Bearer tok";
      seen.push(`${req.method.typeName}/${req.method.name}`);
      const res = await next(req);
      return res;
    };
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock, { interceptors: [auth] }));
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }));
    await tick();

    // The interceptor saw the typed method and injected the auth header onto OPEN.
    expect(seen).toEqual(["echo.v1.EchoService/Unary"]);
    const open = sock.sent.find((f) => f.kind === Kind.KIND_OPEN && f.streamId === 1);
    expect(open?.headers["authorization"]).toBe("Bearer tok");

    const msg = sock.sent.find((f) => f.kind === Kind.KIND_MSG && f.streamId === 1)!;
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msg.payload });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    await p;
  });

  it("unary rejects when the server ends with an error AFTER sending a message", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }));
    await tick();
    // A message followed by a non-OK END: the trailing error must NOT be
    // swallowed by returning the message — the call rejects with the status.
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array() });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 7, message: "denied" } });
    await expect(p).rejects.toBeInstanceOf(WsStatusError);
  });

  it("server-streaming surfaces a WsStatusError out of the for-await loop", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const stream = client.serverStream(create(ServerStreamRequestSchema, { count: 0 }));
    const consume = (async () => {
      for await (const _ of stream) {
        // drain
      }
    })();
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_RST, status: { code: 2, message: "boom" } });
    await expect(consume).rejects.toBeInstanceOf(WsStatusError);
  });

  it("client-streaming cancels (RST) and rejects when the request source throws", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    async function* boom(): AsyncIterable<ReturnType<typeof create>> {
      yield create(ClientStreamRequestSchema, { value: 1 });
      throw new Error("request source failed");
    }
    const p = client.clientStream(boom() as never);
    await expect(p).rejects.toThrow(/request source failed/);
    await tick();
    const rst = sock.sent.find((fr) => fr.kind === Kind.KIND_RST && fr.streamId === 1);
    expect(rst).toBeDefined();
  });
});

describe("generated client call options", () => {
  it("unary invokes onTrailer with the response trailers", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    let trailers: Record<string, string> | undefined;
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }), {
      onTrailer: (t) => {
        trailers = t;
      },
    });
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array() });
    sock.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 0 },
      headers: { "x-trailer": "done" },
    });
    await p;
    expect(trailers).toBeDefined();
    expect(trailers!["x-trailer"]).toBe("done");
  });

  it("unary invokes onHeader with the leading response headers", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    let header: Record<string, string> | undefined;
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }), {
      onHeader: (h) => {
        header = h;
      },
    });
    await tick();
    sock.inject({
      streamId: 1,
      kind: Kind.KIND_HEADER,
      headers: { "x-lead": "hi" },
    });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array() });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    await p;
    await tick();
    expect(header).toBeDefined();
    expect(header!["x-lead"]).toBe("hi");
  });

  it("unary sends request metadata via options.headers", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }), {
      headers: { authorization: "bearer t" },
    });
    await tick();
    const open = sock.sent.find((fr) => fr.kind === Kind.KIND_OPEN && fr.streamId === 1);
    expect(open).toBeDefined();
    expect(open!.headers["authorization"]).toBe("bearer t");
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array() });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    await p;
  });

  it("unary rejects with a deadline error when timeoutMs elapses and the server never responds", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }), { timeoutMs: 20 });
    await tick();
    const open = sock.sent.find((fr) => fr.kind === Kind.KIND_OPEN && fr.streamId === 1);
    expect(open).toBeDefined();
    expect(open!.headers["ws-timeout-ms"]).toBe("20");
    // Server never responds; the local deadline timer aborts the stream.
    await expect(p).rejects.toBeInstanceOf(WsStatusError);
  });

  it("unary aborts when options.signal fires (rejects, sends RST)", async () => {
    const sock = new FakeSocket();
    const client = new EchoServiceClient(WsTransport.fromSocket(sock));
    const controller = new AbortController();
    const p = client.unary(create(UnaryRequestSchema, { name: "x" }), {
      signal: controller.signal,
    });
    await tick();
    controller.abort();
    await expect(p).rejects.toBeInstanceOf(WsStatusError);
    const rst = sock.sent.find((fr) => fr.kind === Kind.KIND_RST && fr.streamId === 1);
    expect(rst).toBeDefined();
  });
});
