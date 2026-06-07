import { describe, it, expect } from "vitest";
import {
  WsTransport,
  WsStatusError,
  FakeSocket,
  Kind,
  CODE_RESOURCE_EXHAUSTED,
} from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("bounded receive queue", () => {
  it("resets a slow stream and RSTs the peer when its queue overflows", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { maxReceiveQueue: 4 });

    const s = t.openStream("/s/Slow");
    await tick();

    // Consumer never reads. Fill exactly the bound, then push one more to overflow.
    for (let i = 0; i < 4; i++) {
      sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array([i]) });
    }
    sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array([99]) });

    // recv() must reject with RESOURCE_EXHAUSTED (after draining buffered MSGs).
    let err: unknown;
    try {
      // Drain the buffered values first; the failure surfaces once they are gone.
      for (let i = 0; i < 5; i++) {
        await s.recv();
      }
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_RESOURCE_EXHAUSTED);

    // An RST for this stream must have been sent to the peer.
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === s.id);
    expect(rst).toBeDefined();
  });

  it("does not affect other streams when one overflows (no head-of-line blocking)", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { maxReceiveQueue: 4 });

    const a = t.openStream("/s/A"); // slow: never read
    const b = t.openStream("/s/B"); // fast: reads promptly
    await tick();

    // Overflow A.
    for (let i = 0; i < 5; i++) {
      sock.inject({ streamId: a.id, kind: Kind.KIND_MSG, payload: new Uint8Array([i]) });
    }

    // B receives its message immediately despite A overflowing.
    sock.inject({ streamId: b.id, kind: Kind.KIND_MSG, payload: new Uint8Array([7]) });
    const got = await b.recv();
    expect(got).not.toBeNull();
    expect(Array.from(got!)).toEqual([7]);
  });

  it("defaults to a finite bound (not unbounded)", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Default");
    await tick();

    // Push beyond the default 256 without reading; the stream is reset.
    for (let i = 0; i < 300; i++) {
      sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array([i & 0xff]) });
    }
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === s.id);
    expect(rst).toBeDefined();
  });
});
