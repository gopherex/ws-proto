import { describe, it, expect } from "vitest";
import { WsTransport, WsStatusError, CODE_UNAVAILABLE, FakeSocket, Kind } from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));
/** wait sleeps real ms (kept tiny via small test backoff). */
const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe("WsTransport reconnect (opt-in)", () => {
  it("on socket drop, in-flight streams fail with CODE_UNAVAILABLE", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 5, maxMs: 20 },
      createSocket: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
    });

    const s = t.openStream("/s/InFlight");
    await tick();
    expect(sockets.length).toBe(1);

    // Drop the socket. recv() must reject with UNAVAILABLE ("connection lost").
    sockets[0]!.close(1006, "abnormal");

    let err: unknown;
    try {
      await s.recv();
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);

    t.close();
  });

  it("creates a fresh socket after the backoff, and a new stream works on it", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 5, maxMs: 10 },
      createSocket: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
    });

    // First socket up; do an RPC round-trip.
    const s1 = t.openStream("/s/One");
    s1.closeSend();
    await tick();
    expect(sockets.length).toBe(1);
    sockets[0]!.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([0xa1]) });
    sockets[0]!.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    expect((await s1.recv())![0]).toBe(0xa1);

    // Drop the connection: schedules a reconnect after ~5ms backoff.
    sockets[0]!.close(1006, "drop");

    // Wait past the backoff for the new socket to be created.
    await wait(40);
    await tick();
    expect(sockets.length).toBe(2);

    // A NEW stream opened after reconnect runs on the fresh socket.
    const s2 = t.openStream("/s/Two");
    s2.closeSend();
    await tick();
    const open2 = sockets[1]!.sent.find((f) => f.kind === Kind.KIND_OPEN);
    expect(open2).toBeDefined();
    expect(open2!.method).toBe("/s/Two");

    sockets[1]!.inject({ streamId: open2!.streamId, kind: Kind.KIND_MSG, payload: new Uint8Array([0xb2]) });
    sockets[1]!.inject({ streamId: open2!.streamId, kind: Kind.KIND_END, status: { code: 0 } });
    expect((await s2.recv())![0]).toBe(0xb2);

    t.close();
  });

  it("openStream during the reconnect gap buffers frames and flushes on the new socket", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 8, maxMs: 16 },
      createSocket: () => {
        // autoOpen:false so we control exactly when the new socket opens, proving
        // the OPEN/MSG frames were buffered until then.
        const s = new FakeSocket({ autoOpen: false });
        sockets.push(s);
        return s;
      },
    });

    // Open the first socket manually.
    sockets[0]!.open();
    await tick();

    // Drop it -> schedules reconnect.
    sockets[0]!.close(1006, "drop");

    // Wait for the replacement socket to be created (still NOT open).
    await wait(40);
    await tick();
    expect(sockets.length).toBe(2);

    // Open a stream during the gap: frames buffer because socket #2 isn't open.
    const s = t.openStream("/s/Gap");
    s.send(new Uint8Array([0xcc]));
    s.closeSend();
    await tick();
    expect(sockets[1]!.sent.length).toBe(0); // buffered, nothing sent yet

    // Now open socket #2: buffered OPEN + MSG + HALF_CLOSE flush in order.
    sockets[1]!.open();
    await tick();
    const kinds = sockets[1]!.sent.map((f) => f.kind);
    expect(kinds).toEqual([Kind.KIND_OPEN, Kind.KIND_MSG, Kind.KIND_HALF_CLOSE]);

    t.close();
  });

  it("without reconnect, a dropped socket fails in-flight and is NOT replaced", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      createSocket: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
    });

    const s = t.openStream("/s/NoReconnect");
    await tick();
    sockets[0]!.close(1006, "drop");

    await expect(s.recv()).rejects.toBeInstanceOf(WsStatusError);

    await wait(40);
    expect(sockets.length).toBe(1); // no new socket created

    t.close();
  });

  it("close() during the backoff gap stops reconnection (no new socket)", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 50, maxMs: 100 },
      createSocket: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
    });

    await tick();
    expect(sockets.length).toBe(1);
    sockets[0]!.close(1006, "drop"); // schedules reconnect ~50ms out
    t.close(); // cancel before the timer fires

    await wait(120);
    expect(sockets.length).toBe(1); // reconnect was cancelled
  });
});
