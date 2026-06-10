import { describe, it, expect } from "vitest";
import { WsTransport, FakeSocket, Kind } from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("byte-based receive bound", () => {
  it("does not reset a window-obeying peer that sends many small messages", async () => {
    const sock = new FakeSocket();
    // Default 256 KiB window. 300 one-byte messages = 300 bytes, far under it —
    // a window-obeying peer would send all of these without waiting for credit.
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Many");
    await tick();

    const n = 300;
    for (let i = 0; i < n; i++) {
      sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array([i & 0xff]) });
    }
    sock.inject({ streamId: s.id, kind: Kind.KIND_END, status: { code: 0 } });

    let count = 0;
    for (;;) {
      const m = await s.recv();
      if (m === null) {
        break;
      }
      count++;
    }
    expect(count).toBe(n);

    // The stream must NOT have been reset: more frames than a 256-FRAME cap, but
    // the byte-based bound (aligned with the window) is nowhere near exceeded.
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === s.id);
    expect(rst).toBeUndefined();
  });
});
