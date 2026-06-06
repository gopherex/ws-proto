import { describe, it, expect } from "vitest";
import { WsTransport, WsStatusError, FakeSocket, Kind } from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

/** CODE_CANCELLED is the gRPC-style status code an abort surfaces as. */
const CODE_CANCELLED = 1;

describe("AbortSignal support on openStream", () => {
  it("abort before the call: recv() rejects with CANCELLED and an RST is sent", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    const controller = new AbortController();
    controller.abort();

    const s = t.openStream("/s/A", { signal: controller.signal });
    await tick();

    await expect(s.recv()).rejects.toBeInstanceOf(WsStatusError);
    try {
      await s.recv();
      throw new Error("expected throw");
    } catch (e) {
      expect((e as WsStatusError).code).toBe(CODE_CANCELLED);
    }

    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === 1);
    expect(rst).toBeDefined();
  });

  it("abort during the call: a pending recv() rejects and an RST is sent", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    const controller = new AbortController();
    const s = t.openStream("/s/B", { signal: controller.signal });
    s.closeSend();
    await tick();

    const pending = s.recv(); // pending: no inbound yet
    controller.abort();

    await expect(pending).rejects.toBeInstanceOf(WsStatusError);

    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === 1);
    expect(rst).toBeDefined();
  });

  it("abort reason (Error) is carried as the WsStatusError message", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    const controller = new AbortController();
    const s = t.openStream("/s/Reason", { signal: controller.signal });
    s.closeSend();
    await tick();

    const pending = s.recv();
    controller.abort(new Error("user cancelled"));

    try {
      await pending;
      throw new Error("expected throw");
    } catch (e) {
      const err = e as WsStatusError;
      expect(err).toBeInstanceOf(WsStatusError);
      expect(err.code).toBe(CODE_CANCELLED);
      expect(err.message).toBe("user cancelled");
    }
  });

  it("listener removed on normal completion: a later abort has no effect", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    const controller = new AbortController();
    const s = t.openStream("/s/Done", { signal: controller.signal });
    s.closeSend();
    await tick();

    // Clean END finishes the stream and detaches the abort listener.
    sock.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 0 },
      headers: { "x-trailer": "ok" },
    });

    expect(await s.recv()).toBeNull(); // clean END

    const rstBefore = sock.sent.filter((f) => f.kind === Kind.KIND_RST).length;
    // A later abort must not throw and must not produce a new RST.
    expect(() => controller.abort()).not.toThrow();
    await tick();
    const rstAfter = sock.sent.filter((f) => f.kind === Kind.KIND_RST).length;
    expect(rstAfter).toBe(rstBefore);

    // Stream stays stable: trailers still resolve, recv() still null.
    expect((await s.responseHeaders())["x-trailer"]).toBe("ok");
    expect(await s.recv()).toBeNull();
  });

  it("headers via init still land on the OPEN frame", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);

    t.openStream("/s/Hdr", { headers: { k: "v" } });
    await tick();

    const open = sock.sent.find((f) => f.kind === Kind.KIND_OPEN);
    expect(open).toBeDefined();
    expect(open!.headers["k"]).toBe("v");
  });
});
