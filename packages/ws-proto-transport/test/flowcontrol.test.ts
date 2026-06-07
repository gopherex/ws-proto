import { describe, it, expect } from "vitest";
import { WsTransport, FakeSocket, Kind } from "../src/index.js";

/** tick yields to the microtask/timer queue so async plumbing settles. */
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

/** countSent returns how many MSG frames the client has sent on a stream. */
function msgCount(sock: FakeSocket, streamId: number): number {
  return sock.sent.filter((f) => f.kind === Kind.KIND_MSG && f.streamId === streamId).length;
}

describe("flow control: send window (sender side)", () => {
  it("blocks (buffers) sends once the window is spent and resumes on WINDOW_UPDATE", async () => {
    const sock = new FakeSocket();
    // window holds exactly two 4-byte messages.
    const t = WsTransport.fromSocket(sock, { initialWindow: 8 });
    const s = t.openStream("/s/Push");
    await tick();

    const big = () => new Uint8Array(4);
    // Send 5 messages; only the first two fit in the window.
    for (let i = 0; i < 5; i++) {
      s.send(big());
    }
    await tick();
    expect(msgCount(sock, s.id)).toBe(2);

    // Peer returns 8 bytes of credit: exactly two more messages may flush.
    sock.inject({ streamId: s.id, kind: Kind.KIND_WINDOW_UPDATE, window: 8 });
    await tick();
    expect(msgCount(sock, s.id)).toBe(4);

    // Return enough credit for the rest.
    sock.inject({ streamId: s.id, kind: Kind.KIND_WINDOW_UPDATE, window: 8 });
    await tick();
    expect(msgCount(sock, s.id)).toBe(5);
  });

  it("delivers a single message larger than the whole window (no deadlock)", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { initialWindow: 16 });
    const s = t.openStream("/s/Big");
    await tick();

    // One message far larger than the window must still be sent (window goes
    // negative once), then subsequent sends wait for credit.
    s.send(new Uint8Array(64 * 1024));
    await tick();
    expect(msgCount(sock, s.id)).toBe(1);

    // A follow-up small message must wait: the window is deeply negative.
    s.send(new Uint8Array(4));
    await tick();
    expect(msgCount(sock, s.id)).toBe(1);

    // Returning credit beyond the deficit lets the queued message flush.
    sock.inject({ streamId: s.id, kind: Kind.KIND_WINDOW_UPDATE, window: 64 * 1024 + 16 });
    await tick();
    expect(msgCount(sock, s.id)).toBe(2);
  });

  it("backpressure: many large messages all arrive in order as credit is returned", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { initialWindow: 8 });
    const s = t.openStream("/s/Many");
    await tick();

    const count = 20;
    for (let i = 0; i < count; i++) {
      // 4-byte payload tagged with its index in byte 0.
      const p = new Uint8Array(4);
      p[0] = i;
      s.send(p);
    }

    // Drip credit (8 bytes == 2 msgs) repeatedly until all have flushed.
    for (let round = 0; round < count; round++) {
      await tick();
      if (msgCount(sock, s.id) >= count) break;
      sock.inject({ streamId: s.id, kind: Kind.KIND_WINDOW_UPDATE, window: 8 });
    }
    await tick();

    const sent = sock.sent.filter((f) => f.kind === Kind.KIND_MSG && f.streamId === s.id);
    expect(sent.length).toBe(count);
    // Order must be preserved (the pump is FIFO).
    sent.forEach((f, i) => expect(f.payload[0]).toBe(i));
  });
});

describe("flow control: returning credit (receiver side)", () => {
  it("emits a WINDOW_UPDATE once consumed bytes cross initialWindow/2", async () => {
    const sock = new FakeSocket();
    // threshold = initialWindow/2 = 8 bytes.
    const t = WsTransport.fromSocket(sock, { initialWindow: 16 });
    const s = t.openStream("/s/Recv");
    await tick();

    // Inject two 5-byte messages and consume them: 10 >= 8 triggers one update.
    sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array(5) });
    sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array(5) });

    await s.recv(); // 5 consumed, below threshold -> no update yet
    expect(sock.sent.find((f) => f.kind === Kind.KIND_WINDOW_UPDATE)).toBeUndefined();

    await s.recv(); // 10 consumed, crosses threshold -> one coalesced update
    const upd = sock.sent.find((f) => f.kind === Kind.KIND_WINDOW_UPDATE && f.streamId === s.id);
    expect(upd).toBeDefined();
    expect(upd!.window).toBe(10);
  });

  it("does not emit a WINDOW_UPDATE for bytes received but not yet consumed", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { initialWindow: 16 });
    const s = t.openStream("/s/Pending");
    await tick();

    // Many bytes arrive but nobody reads: credit must NOT be returned (that is
    // what makes the window real backpressure — credit returns on consumption).
    for (let i = 0; i < 4; i++) {
      sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload: new Uint8Array(5) });
    }
    await tick();
    expect(sock.sent.find((f) => f.kind === Kind.KIND_WINDOW_UPDATE)).toBeUndefined();
  });
});
