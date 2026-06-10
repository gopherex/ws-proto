import { describe, it, expect } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { WsTransport, FakeSocket, Kind } from "../src/index.js";
import type { Interceptor, MethodInfo } from "../src/index.js";
import { StatusSchema } from "../src/gen/transport_pb.js";
import type { Status } from "../src/gen/transport_pb.js";

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

// Use the transport's own Status message as a stand-in typed I/O message.
const echoMethod: MethodInfo<Status, Status> = {
  typeName: "test.Svc",
  name: "Echo",
  kind: "unary",
  input: StatusSchema,
  output: StatusSchema,
};

const serverStreamMethod: MethodInfo<Status, Status> = {
  typeName: "test.Svc",
  name: "Feed",
  kind: "server_streaming",
  input: StatusSchema,
  output: StatusSchema,
};

describe("WsTransport.unary", () => {
  it("round-trips a typed message and exposes leading header + trailer", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const p = t.unary(echoMethod, create(StatusSchema, { code: 7, message: "hi" }));
    await tick();

    const open = sock.sent.find((f) => f.kind === Kind.KIND_OPEN);
    expect(open?.method).toBe("/test.Svc/Echo");
    const msg = sock.sent.find((f) => f.kind === Kind.KIND_MSG);
    expect(msg).toBeDefined();

    sock.inject({ streamId: 1, kind: Kind.KIND_HEADER, headers: { "x-lead": "L" } });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msg!.payload });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 }, headers: { "x-tr": "T" } });

    const resp = await p;
    expect(resp.message.code).toBe(7);
    expect(resp.message.message).toBe("hi");
    expect(resp.header["x-lead"]).toBe("L");
    expect(resp.trailer["x-tr"]).toBe("T");
  });

  it("lets an interceptor inject a request header", async () => {
    const auth: Interceptor = (next) => async (req) => {
      req.header["authorization"] = "Bearer t";
      return next(req);
    };
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { interceptors: [auth] });
    const p = t.unary(echoMethod, create(StatusSchema, { code: 1 }));
    await tick();
    const open = sock.sent.find((f) => f.kind === Kind.KIND_OPEN);
    expect(open?.headers["authorization"]).toBe("Bearer t");

    const msg = sock.sent.find((f) => f.kind === Kind.KIND_MSG)!;
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msg.payload });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    await p;
  });

  it("lets an interceptor transform the request message (typed)", async () => {
    const bump: Interceptor = (next) => async (req) => {
      if (!req.stream) {
        // Replace the typed message before it is serialized.
        (req as { message: unknown }).message = create(StatusSchema, { code: 99 });
      }
      return next(req);
    };
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { interceptors: [bump] });
    const p = t.unary(echoMethod, create(StatusSchema, { code: 1 }));
    await tick();
    const msg = sock.sent.find((f) => f.kind === Kind.KIND_MSG)!;
    // The bytes on the wire must reflect the transformed message (code 99).
    expect(Array.from(msg.payload)).toEqual(Array.from(toBinary(StatusSchema, create(StatusSchema, { code: 99 }))));
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msg.payload });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    const resp = await p;
    expect(resp.message.code).toBe(99);
  });

  it("lets an interceptor short-circuit without touching the socket", async () => {
    const shortCircuit: Interceptor = (next) => async (req) => ({
      stream: false as const,
      method: req.method,
      header: {},
      message: create(StatusSchema, { code: 42 }),
      trailer: {},
    });
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { interceptors: [shortCircuit] });
    const resp = await t.unary(echoMethod, create(StatusSchema, { code: 1 }));
    expect(resp.message.code).toBe(42);
    await tick();
    expect(sock.sent.find((f) => f.kind === Kind.KIND_OPEN)).toBeUndefined();
  });
});

describe("WsTransport.stream", () => {
  it("server-streaming: one request, many responses, with header/trailer", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    async function* one() {
      yield create(StatusSchema, { code: 0, message: "req" });
    }
    const resp = await t.stream(serverStreamMethod, one());
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: toBinary(StatusSchema, create(StatusSchema, { code: 1 })) });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: toBinary(StatusSchema, create(StatusSchema, { code: 2 })) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 }, headers: { "x-tr": "T" } });

    const codes: number[] = [];
    for await (const m of resp.message) {
      codes.push(m.code);
    }
    expect(codes).toEqual([1, 2]);
    expect(resp.trailer["x-tr"]).toBe("T");
  });
});
