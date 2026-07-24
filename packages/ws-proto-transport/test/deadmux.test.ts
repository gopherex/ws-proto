/**
 * Dead-mux invariants: once a mux is defunct (transport drop, protocol failure,
 * deliberate close), nothing may be silently swallowed. "Not yet open" (frames
 * buffer, then flush) and "gone for good" (operations fail observably) are two
 * different states and must never be conflated:
 *   - a new stream on a defunct mux fails fast with the terminal error;
 *   - the defunct mux holds no buffered frames and no stream registrations;
 *   - cancel() after the drop writes nothing into the void.
 */
import { describe, it, expect } from "vitest";
import {
  WsTransport,
  WsStatusError,
  CODE_UNAVAILABLE,
  CODE_CANCELLED,
  FakeSocket,
  Kind,
} from "../src/index.js";
import { expectRejects, tick, wait } from "./harness.js";

/** muxInternals exposes the private mux state checked by leak assertions. */
function muxInternals(t: WsTransport): {
  sendBuffer: Uint8Array[];
  sendBufferBytes: number;
  streams: Map<number, unknown>;
} {
  return (t as unknown as { mux: never })["mux"];
}

describe("defunct mux: fail fast, no silent buffering", () => {
  it("openStream after a drop fails the stream promptly with UNAVAILABLE", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    await tick(); // socket opens

    sock.error(); // hard drop
    const sentBefore = sock.sent.length;

    const s = t.openStream("/s/AfterDrop");
    const err = await expectRejects(s.recv(), 500, "recv on dead-mux stream");
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    // No frame was written into the dead socket.
    expect(sock.sent.length).toBe(sentBefore);
  });

  it("openStream in the reconnect gap (before the new mux exists) fails fast, then a fresh stream works", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 15, maxMs: 30 },
      createSocket: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
    });
    await tick();
    sockets[0]!.drop(); // schedules reconnect ~15ms out

    // The replacement mux does not exist yet: the call's fate must be decided
    // NOW (fail fast; the caller's retry loop redials), not silently parked.
    const gap = t.openStream("/s/Gap");
    const err = await expectRejects(gap.recv(), 500, "recv on gap stream");
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);

    // After the backoff the new mux carries new streams normally.
    await wait(60);
    expect(sockets.length).toBe(2);
    const s2 = t.openStream("/s/Fresh");
    s2.closeSend();
    await tick();
    const open = sockets[1]!.sent.find((f) => f.kind === Kind.KIND_OPEN);
    expect(open).toBeDefined();
    expect(open!.method).toBe("/s/Fresh");
    t.close();
  });

  it("drop clears the pre-open send buffer and the stream table (no leak)", async () => {
    const sock = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(sock);

    const s = t.openStream("/s/Buffered");
    s.send(new Uint8Array(1024));
    const internals = muxInternals(t);
    expect(internals.sendBufferBytes).toBeGreaterThan(0);
    expect(internals.streams.size).toBe(1);

    sock.error(); // dies before ever opening
    await expectRejects(s.recv(), 500, "recv on dropped stream");

    expect(internals.sendBuffer.length).toBe(0);
    expect(internals.sendBufferBytes).toBe(0);
    expect(internals.streams.size).toBe(0);
  });

  it("close() clears buffers and streams too", async () => {
    const sock = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Closed");
    s.send(new Uint8Array(64));

    t.close();
    const err = await expectRejects(s.recv(), 500, "recv after close");
    expect((err as WsStatusError).code).toBe(CODE_CANCELLED);

    const internals = muxInternals(t);
    expect(internals.sendBuffer.length).toBe(0);
    expect(internals.sendBufferBytes).toBe(0);
    expect(internals.streams.size).toBe(0);
  });

  it("cancel() after a drop writes nothing (§3.3: cancel of a dead stream is a no-op)", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/CancelDead");
    await tick();

    sock.error();
    const sentBefore = sock.sent.length;
    s.cancel(); // stream already failed: must not emit RST into the dead socket
    expect(sock.sent.length).toBe(sentBefore);
    expect(muxInternals(t).sendBuffer.length).toBe(0);
    await expectRejects(s.recv(), 500, "recv after cancel-on-dead");
  });

  it("openStream after subprotocol failure fails fast with the negotiation error", async () => {
    const sock = new FakeSocket({ autoOpen: false, protocol: "" });
    const t = WsTransport.fromSocket(sock);
    sock.open(); // validateProtocol fails, mux is terminally closed

    const s = t.openStream("/s/BadProto");
    const err = await expectRejects(s.recv(), 500, "recv on bad-protocol stream");
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    expect((err as Error).message).toContain("subprotocol");
    expect(muxInternals(t).sendBuffer.length).toBe(0);
  });

  it("double drop (error then close, browser-style) fails streams once and stays consistent", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/DoubleDrop");
    await tick();

    sock.drop(); // onerror + onclose both fire
    const err = await expectRejects(s.recv(), 500, "recv after double drop");
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    expect(muxInternals(t).streams.size).toBe(0);
  });
});
