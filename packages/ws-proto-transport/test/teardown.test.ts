/**
 * Teardown invariant: a terminal event on the read side of a streaming RPC
 * finishes the WHOLE call — the response iterator settles, trailers resolve,
 * and the transport never waits on the caller's request source (which is not
 * under its control and may be infinite/idle). This is the §1 regression suite:
 * before the fix, every test here hung forever.
 */
import { describe, it, expect } from "vitest";
import { WsTransport, WsStatusError, CODE_UNAVAILABLE, FakeSocket, Kind } from "../src/index.js";
import {
  bidiMethod,
  msg,
  msgBytes,
  foreverSource,
  expectSettles,
  expectRejects,
  tick,
} from "./harness.js";

describe("teardown: terminal read event with an infinite request source", () => {
  it("§1 repro: transport drop while the source idles → iterator rejects UNAVAILABLE", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const events: { finallyRan?: boolean; unblock?: () => void } = {};

    const res = await t.stream(bidiMethod, foreverSource(msg(1), events), {});
    const iter = res.message[Symbol.asyncIterator]();
    const pending = iter.next();
    await tick(); // let OPEN/MSG flush

    sock.error(); // hard transport drop, no close frame

    const err = await expectRejects(pending, 500, "response iterator");
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
  });

  it("transport finalizes the request source: its finally runs once the idle await settles", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const events: { finallyRan?: boolean; unblock?: () => void } = {};

    const res = await t.stream(bidiMethod, foreverSource(msg(1), events), {});
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    sock.error();
    await expectRejects(pending, 500, "response iterator");

    // The generator is still parked on its idle await — JS cannot interrupt
    // that — but the transport must have QUEUED return(), so the moment the
    // idle await settles, the source unwinds its finally.
    expect(events.finallyRan).toBeUndefined();
    events.unblock!();
    await tick();
    expect(events.finallyRan).toBe(true);
  });

  it("read-side error takes priority over a later source error", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    let failSource!: (e: Error) => void;
    const source = (async function* () {
      yield msg(1);
      await new Promise<void>((_, rej) => {
        failSource = rej;
      });
    })();

    const res = await t.stream(bidiMethod, source, {});
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    sock.error(); // read side dies first: UNAVAILABLE is the root cause
    failSource(new Error("source boom")); // write side fails afterwards

    const err = await expectRejects(pending, 500, "response iterator");
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    expect((err as Error).message).not.toContain("source boom");
  });

  it("non-OK END with an idle source → status and trailers surface, no hang", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const events: { finallyRan?: boolean; unblock?: () => void } = {};

    const res = await t.stream(bidiMethod, foreverSource(msg(1), events), {});
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    sock.inject({
      streamId: 1,
      kind: Kind.KIND_END,
      status: { code: 7, message: "denied" },
      headers: { "x-tlr": "T" },
    });

    const err = await expectRejects(pending, 500, "response iterator");
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(7);
    expect(res.trailer["x-tlr"]).toBe("T");
  });

  it("clean END with an idle source → iteration completes, trailers resolve, no hang", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const events: { finallyRan?: boolean; unblock?: () => void } = {};

    const res = await t.stream(bidiMethod, foreverSource(msg(1), events), {});
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msgBytes(5) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 }, headers: { "x-tlr": "ok" } });

    const codes = await expectSettles(
      (async () => {
        const got: number[] = [];
        for await (const m of res.message) {
          got.push(m.code);
        }
        return got;
      })(),
      500,
      "response iteration",
    );
    expect(codes).toEqual([5]);
    expect(res.trailer["x-tlr"]).toBe("ok");
  });

  it("caller's early break with an idle source → loop exit settles, RST sent", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const events: { finallyRan?: boolean; unblock?: () => void } = {};

    const res = await t.stream(bidiMethod, foreverSource(msg(1), events), {});
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msgBytes(9) });

    await expectSettles(
      (async () => {
        for await (const m of res.message) {
          expect(m.code).toBe(9);
          break; // early exit must not wait for the infinite source
        }
      })(),
      500,
      "early break out of response iteration",
    );
    await tick();
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === 1);
    expect(rst).toBeDefined();
  });

  it("source error while the stream is healthy → RST sent, iterator rejects with the source error", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const source = (async function* () {
      yield msg(1);
      throw new Error("producer failed");
    })();

    const res = await t.stream(bidiMethod, source, {});
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    const err = await expectRejects(pending, 500, "response iterator");
    expect((err as Error).message).toBe("producer failed");
    const rst = sock.sent.find((f) => f.kind === Kind.KIND_RST && f.streamId === 1);
    expect(rst).toBeDefined();
  });
});
