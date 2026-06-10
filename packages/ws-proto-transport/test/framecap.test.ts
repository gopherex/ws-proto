import { describe, it, expect } from "vitest";
import { WsTransport, FakeSocket, Kind } from "../src/index.js";

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("inbound frame size cap (maxFrameBytes)", () => {
  it("fails the connection when an inbound frame exceeds maxFrameBytes", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock, { maxFrameBytes: 64 });
    const s = t.openStream("/s/M");
    await tick();

    let err: unknown;
    void s.recv().catch((e) => {
      err = e;
    });

    // Deliver a raw binary message larger than the cap (rejected before decode).
    sock.onmessage?.({ data: new Uint8Array(200) });
    await tick();
    expect(err).toBeDefined();
  });

  it("does not cap inbound frames by default", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock); // no maxFrameBytes
    const s = t.openStream("/s/M");
    await tick();

    // A large but legitimate message is delivered and received.
    const payload = new Uint8Array(100_000);
    sock.inject({ streamId: s.id, kind: Kind.KIND_MSG, payload });
    const got = await s.recv();
    expect(got).not.toBeNull();
    expect(got!.length).toBe(100_000);
  });
});
