import { describe, it, expect } from "vitest";
import { WsTransport, FakeSocket, CODE_RESOURCE_EXHAUSTED } from "../src/index.js";

const MiB = 1024 * 1024;

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("bounded send buffer", () => {
  it("aborts a stream that buffers more unsent data than maxSendBuffer", async () => {
    const sock = new FakeSocket();
    // Tiny 8-byte window so sends stall immediately, and a small 64-byte send
    // buffer so a flood overruns it. The peer never returns credit.
    const t = WsTransport.fromSocket(sock, { initialWindow: 8, maxSendBuffer: 64 });
    const s = t.openStream("/s/Flood");
    await tick();

    // Flood send() far beyond maxSendBuffer with no credit coming back. A bounded
    // outbound buffer must abort the stream rather than grow without limit.
    for (let i = 0; i < 100; i++) {
      s.send(new Uint8Array(8));
    }

    let err: unknown;
    void s.recv().catch((e) => {
      err = e;
    });
    await tick();
    expect(err).toMatchObject({ code: CODE_RESOURCE_EXHAUSTED });
  });

  it("does not abort a stream whose sends stay within the window", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { initialWindow: 1024 });
    const s = t.openStream("/s/Ok");
    await tick();

    s.send(new Uint8Array(8));
    s.send(new Uint8Array(8));

    let err: unknown;
    void s.recv().catch((e) => {
      err = e;
    });
    await tick();
    expect(err).toBeUndefined();
    s.cancel();
  });

  it("fails the connection when too much is buffered before the socket opens", async () => {
    const sock = new FakeSocket({ autoOpen: false }); // socket never opens
    // Large per-stream window so the per-stream send bound does not fire; the
    // frames instead pile up in the mux's pre-open buffer, which is itself
    // bounded and must fail the connection rather than grow without limit.
    const t = WsTransport.fromSocket(sock, { initialWindow: 16 * MiB });
    const s = t.openStream("/s/M");

    const big = new Uint8Array(MiB);
    for (let i = 0; i < 8; i++) {
      s.send(big); // 8 MiB total, never flushed (socket closed)
    }

    let err: unknown;
    void s.recv().catch((e) => {
      err = e;
    });
    await tick();
    expect(err).toBeDefined();
  });
});
