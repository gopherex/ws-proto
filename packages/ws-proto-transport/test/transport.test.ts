import { describe, it, expect, afterEach } from "vitest";
import {
  WsTransport,
  WsStatusError,
  FakeSocket,
  Kind,
  SUBPROTOCOL,
  CODE_DEADLINE_EXCEEDED,
} from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("WsTransport over FakeSocket", () => {
  it("sends OPEN with method and headers on openStream", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    t.openStream("/pkg.Svc/Unary", { headers: { auth: "token" } });
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

  it("responseHeaders resolves with trailers even on a non-OK END", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/ErrTrailers");
    s.closeSend();
    await tick();

    sock.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 7, message: "denied" },
      headers: { "x-trailer": "meta" },
    });

    const headers = await s.responseHeaders();
    expect(headers["x-trailer"]).toBe("meta");
    await expect(s.recv()).rejects.toBeInstanceOf(WsStatusError);
  });

  it("early break out of async iteration sends RST and detaches the stream", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Break");
    s.closeSend();
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([1]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([2]) });

    const got: number[] = [];
    for await (const msg of s) {
      got.push(msg[0]!);
      break; // early exit -> iterator return() -> cancel() -> RST
    }
    await tick();

    expect(got).toEqual([1]);
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === 1);
    expect(rst).toBeDefined();
  });

  it("timeoutMs sends ws-timeout-ms on OPEN and aborts the stream on expiry", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Timeout", { timeoutMs: 30 });
    await tick();

    const open = sock.sent.find((f) => f.kind === Kind.KIND_OPEN && f.streamId === 1);
    expect(open).toBeDefined();
    expect(open!.headers["ws-timeout-ms"]).toBe("30");

    // recv rejects with a DEADLINE_EXCEEDED WsStatusError after the timeout.
    let err: unknown;
    try {
      await s.recv();
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_DEADLINE_EXCEEDED);

    // The abort also detached the stream via RST.
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === 1);
    expect(rst).toBeDefined();
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

describe("WsTransport subprotocol negotiation", () => {
  // Save the original globalThis.WebSocket so we can restore it.
  const originalWebSocket = (globalThis as Record<string, unknown>).WebSocket;

  afterEach(() => {
    (globalThis as Record<string, unknown>).WebSocket = originalWebSocket;
  });

  it("dials with the wsrpc.v1 subprotocol token", () => {
    let capturedUrl: string | undefined;
    let capturedProtocols: string | string[] | undefined;

    // Minimal stub that records constructor args and implements WebSocketLike.
    class StubWebSocket {
      binaryType = "arraybuffer";
      onmessage = null;
      onopen = null;
      onclose = null;
      onerror = null;
      readonly protocol = "";
      constructor(url: string, protocols?: string | string[]) {
        capturedUrl = url;
        capturedProtocols = protocols;
      }
      send() {}
      close() {}
    }

    (globalThis as Record<string, unknown>).WebSocket = StubWebSocket;

    new WsTransport("ws://test-host/rpc");

    expect(capturedUrl).toBe("ws://test-host/rpc");
    expect(capturedProtocols).toBe(SUBPROTOCOL);
    expect(capturedProtocols).toBe("wsrpc.v1");
  });

  it("SUBPROTOCOL constant equals 'wsrpc.v1'", () => {
    expect(SUBPROTOCOL).toBe("wsrpc.v1");
  });
});
