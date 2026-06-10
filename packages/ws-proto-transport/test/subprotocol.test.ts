import { describe, it, expect } from "vitest";
import { WsTransport, FakeSocket } from "../src/index.js";

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("subprotocol validation", () => {
  it("fails in-flight streams when the server did not negotiate wsrpc.v1", async () => {
    // protocol "" => the handshake selected no subprotocol (or a proxy stripped
    // it). The transport must not silently speak a framing the peer never agreed
    // to: it fails the connection on open.
    const sock = new FakeSocket({ protocol: "" });
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/M");

    let rejected = false;
    void s.recv().catch(() => {
      rejected = true;
    });

    await tick(); // socket opens; validation runs
    await tick();
    expect(rejected).toBe(true);
  });

  it("accepts a connection that negotiated wsrpc.v1", async () => {
    const sock = new FakeSocket({ protocol: "wsrpc.v1" });
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/M");
    await tick();

    let rejected = false;
    void s.recv().catch(() => {
      rejected = true;
    });
    await tick();
    expect(rejected).toBe(false); // still open, waiting for data
    s.cancel();
  });
});
