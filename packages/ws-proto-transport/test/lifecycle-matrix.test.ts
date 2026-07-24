/**
 * Lifecycle invariant matrix (§4): each invariant is stated once and run across
 * RPC shapes (unary / server-stream / client-stream / bidi) and socket states
 * (never opened / open / dropped / closed), instead of hand-written scenarios.
 * Every case runs under the harness hang detector — a hang is a diagnosis, not
 * a runner timeout.
 */
import { describe, it, expect, vi } from "vitest";
import {
  WsTransport,
  WsStatusError,
  CODE_UNAVAILABLE,
  CODE_CANCELLED,
  CODE_DEADLINE_EXCEEDED,
  FakeSocket,
  Kind,
} from "../src/index.js";
import type { MethodInfo } from "../src/index.js";
import {
  unaryMethod,
  serverStreamMethod,
  clientStreamMethod,
  bidiMethod,
  msg,
  msgBytes,
  foreverSource,
  expectSettles,
  expectRejects,
  expectNeverSettles,
  swallow,
  tick,
  wait,
} from "./harness.js";

function muxInternals(t: WsTransport): {
  sendBuffer: Uint8Array[];
  sendBufferBytes: number;
  streams: Map<number, unknown>;
} {
  return (t as unknown as { mux: never })["mux"];
}

/**
 * startCall begins one RPC of the given shape. The `done` promise settles only
 * when the CALLER observes the call's terminal event. (Wrapped in an object —
 * returning the promise directly would make `await startCall()` adopt it.)
 */
async function startCall(
  t: WsTransport,
  method: MethodInfo,
): Promise<{ done: Promise<unknown> }> {
  if (method.kind === "unary") {
    const p = t.unary(unaryMethod, msg(1));
    swallow(p);
    return { done: p };
  }
  const source =
    method.kind === "server_streaming"
      ? (async function* () {
          yield msg(1);
        })()
      : foreverSource(msg(1));
  const res = await t.stream(method, source, {});
  const done = (async () => {
    for await (const m of res.message) {
      void m;
    }
  })();
  swallow(done);
  return { done };
}

const SHAPES: Array<[string, MethodInfo]> = [
  ["unary", unaryMethod],
  ["server_streaming", serverStreamMethod],
  ["client_streaming", clientStreamMethod],
  ["bidi_streaming", bidiMethod],
];

// ---- A. Teardown and error propagation ----

describe("A: a terminal transport event settles every call shape", () => {
  it.each(SHAPES)("%s: drop after open → rejects UNAVAILABLE", async (_name, method) => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    await tick();
    const { done } = await startCall(t, method);
    await tick();

    sock.drop();

    const err = await expectRejects(done, 500, `${_name} call`);
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
  });

  it.each(SHAPES)("%s: drop before the socket ever opens → rejects UNAVAILABLE", async (_name, method) => {
    const sock = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(sock);
    const { done } = await startCall(t, method);
    await tick();

    sock.error();

    const err = await expectRejects(done, 500, `${_name} call (preopen)`);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
  });

  it.each(SHAPES)("%s: transport close() → rejects CANCELLED", async (_name, method) => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    await tick();
    const { done } = await startCall(t, method);
    await tick();

    t.close();

    const err = await expectRejects(done, 500, `${_name} call (closed)`);
    expect((err as WsStatusError).code).toBe(CODE_CANCELLED);
  });

  it("drop between response messages: delivered data is kept, then UNAVAILABLE", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), {});
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msgBytes(41) });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msgBytes(42) });

    const got: number[] = [];
    const err = await expectRejects(
      (async () => {
        for await (const m of res.message) {
          got.push(m.code);
          if (got.length === 2) {
            sock.drop(); // dies exactly between messages
          }
        }
      })(),
      500,
      "iteration across a mid-stream drop",
    );
    expect(got).toEqual([41, 42]); // partial data never silently lost
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
  });

  it("END racing a drop: END that arrived first wins deterministically (clean completion)", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), {});
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: msgBytes(7) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 }, headers: { "x-t": "1" } });
    sock.drop(); // arrives after END in the same tick

    const codes = await expectSettles(
      (async () => {
        const out: number[] = [];
        for await (const m of res.message) {
          out.push(m.code);
        }
        return out;
      })(),
      500,
      "iteration with END-then-drop",
    );
    expect(codes).toEqual([7]);
    expect(res.trailer["x-t"]).toBe("1");
  });

  it("caller's source finally runs on the clean-END path too", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const events: { finallyRan?: boolean; unblock?: () => void } = {};
    const res = await t.stream(bidiMethod, foreverSource(msg(1), events), {});
    await tick();
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });

    await expectSettles(
      (async () => {
        for await (const m of res.message) {
          void m;
        }
      })(),
      500,
      "clean-END iteration",
    );
    events.unblock!();
    await tick();
    expect(events.finallyRan).toBe(true);
  });
});

// ---- B. Cancellation, abort, deadlines ----

describe("B: cancellation and deadlines are terminal exactly once", () => {
  it("abort before the socket opens: call settles CANCELLED, nothing hangs", async () => {
    const sock = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(sock);
    const ctl = new AbortController();
    ctl.abort();
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), { signal: ctl.signal });
    const err = await expectRejects(
      res.message[Symbol.asyncIterator]().next(),
      500,
      "aborted-before-open call",
    );
    expect((err as WsStatusError).code).toBe(CODE_CANCELLED);
  });

  it("abort AFTER a drop: first terminal event wins (stays UNAVAILABLE), abort is a no-op", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const ctl = new AbortController();
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), { signal: ctl.signal });
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    sock.drop();
    ctl.abort();

    const err = await expectRejects(pending, 500, "drop-then-abort call");
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    expect(sock.sent.filter((f) => f.kind === Kind.KIND_RST).length).toBe(0); // no RST into the void
  });

  it("deadline expiry: DEADLINE_EXCEEDED and the stream is deregistered", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), { timeoutMs: 20 });
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    const err = await expectRejects(pending, 500, "deadline call");
    expect((err as WsStatusError).code).toBe(CODE_DEADLINE_EXCEEDED);
    expect(muxInternals(t).streams.size).toBe(0);
  });

  it("deadline expiring after a drop: UNAVAILABLE already won, expiry is a no-op", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), { timeoutMs: 30 });
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    sock.drop();
    const err = await expectRejects(pending, 500, "drop-then-deadline call");
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    await wait(50); // let the (cleared) deadline timer window pass — nothing may fire
    expect(muxInternals(t).streams.size).toBe(0);
  });

  it("double cancel is idempotent: exactly one RST", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/DoubleCancel");
    await tick();

    s.cancel();
    s.cancel();
    await tick();
    expect(sock.sent.filter((f) => f.kind === Kind.KIND_RST).length).toBe(1);
  });
});

// ---- C. Reconnect ----

describe("C: reconnect supervisor", () => {
  it("a series of N drops schedules a reconnect every time", async () => {
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
    for (let i = 1; i <= 3; i++) {
      await expectSettles(
        (async () => {
          while (sockets.length < i) {
            await wait(5);
          }
        })(),
        1000,
        `socket #${i} creation`,
      );
      await tick(); // let it open
      sockets[i - 1]!.drop();
    }
    await expectSettles(
      (async () => {
        while (sockets.length < 4) {
          await wait(5);
        }
      })(),
      1000,
      "socket #4 creation after third drop",
    );
    t.close();
  });

  it("backoff attempt counter grows across failures and resets once a socket opens", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 1, maxMs: 2 },
      createSocket: () => {
        const s = new FakeSocket({ autoOpen: false });
        sockets.push(s);
        return s;
      },
    });
    const attempt = () => (t as unknown as { attempt: number }).attempt;

    sockets[0]!.error(); // fails before opening
    await wait(10);
    sockets[1]!.error();
    await wait(10);
    expect(sockets.length).toBeGreaterThanOrEqual(3);
    expect(attempt()).toBeGreaterThanOrEqual(2); // grew across consecutive failures

    sockets[sockets.length - 1]!.open(); // a socket finally opens
    await tick();
    expect(attempt()).toBe(0); // reset on successful open
    t.close();
  });

  it("subprotocol failure suppresses reconnect (terminal misconfiguration)", async () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport("ws://test/rpc", {
      reconnect: true,
      backoff: { initialMs: 2, maxMs: 4 },
      createSocket: () => {
        const s = new FakeSocket({ protocol: "junk" });
        sockets.push(s);
        return s;
      },
    });
    const s = t.openStream("/s/BadProto");
    await tick(); // socket opens; negotiation fails terminally
    swallow(s.recv());

    await wait(30);
    expect(sockets.length).toBe(1); // no redial loop on a config error
    t.close();
  });
});

// ---- D. Backpressure at the mux boundary ----

describe("D: pre-open mux buffer is bounded and its overflow is observable", () => {
  it("flooding a never-opening socket fails the connection instead of buffering forever", async () => {
    const sock = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/PreopenFlood");

    // One payload above MAX_PREOPEN_BUFFER_BYTES (4 MiB) while the socket never
    // opens: the mux must fail the connection observably, not grow forever.
    s.send(new Uint8Array(5 * 1024 * 1024));

    const err = await expectRejects(s.recv(), 500, "recv after preopen flood");
    expect(err).toBeInstanceOf(WsStatusError);
    expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    expect((err as Error).message).toContain("overflow");

    const internals = muxInternals(t);
    expect(internals.sendBuffer.length).toBe(0); // released, not leaked
    expect(internals.sendBufferBytes).toBe(0);
  });
});

// ---- E. Protocol and framing robustness ----

describe("E: inbound robustness", () => {
  it("frames for an unknown streamId are ignored without disturbing live streams", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Live");
    s.closeSend();
    await tick();

    sock.inject({ streamId: 99, kind: Kind.KIND_MSG, payload: msgBytes(1) });
    sock.inject({ streamId: 99, kind: Kind.KIND_END, status: { code: 13 } });
    sock.inject({ streamId: 99, kind: Kind.KIND_RST, status: { code: 1 } });

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([5]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    expect((await expectSettles(s.recv(), 500, "live recv"))![0]).toBe(5);
    expect(await s.recv()).toBeNull();
  });

  it("an unknown frame Kind is ignored without failing the connection", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/UnknownKind");
    s.closeSend();
    await tick();

    sock.inject({ streamId: 1, kind: 42 as never });

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([9]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    expect((await expectSettles(s.recv(), 500, "recv after unknown kind"))![0]).toBe(9);
  });

  it("undecodable inbound bytes are ignored, the connection survives", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Garbage");
    s.closeSend();
    await tick();

    expect(() => sock.injectRaw(new Uint8Array([0xff, 0xff, 0xff, 0xff]))).not.toThrow();
    expect(() => sock.injectRaw("not binary at all")).not.toThrow();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([3]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    expect((await expectSettles(s.recv(), 500, "recv after garbage"))![0]).toBe(3);
  });

  it("an inbound HALF_CLOSE does not tear down the read side", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/HalfClose");
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_HALF_CLOSE });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([4]) });
    expect((await expectSettles(s.recv(), 500, "recv after half-close"))![0]).toBe(4);
    s.cancel();
  });
});

// ---- F. Resource lifecycle ----

describe("F: resources return to zero on every terminal path", () => {
  type Terminal = [string, (sock: FakeSocket, t: WsTransport) => void];
  const TERMINALS: Terminal[] = [
    ["clean END", (sock) => sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } })],
    ["error END", (sock) => sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 13, message: "x" } })],
    ["RST", (sock) => sock.inject({ streamId: 1, kind: Kind.KIND_RST, status: { code: 1 } })],
    ["transport drop", (sock) => sock.drop()],
    ["transport close", (_sock, t) => t.close()],
  ];

  it.each(TERMINALS)("%s: stream table and buffers are empty afterwards", async (_name, fire) => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/Terminal");
    await tick();
    swallow(s.recv());

    fire(sock, t);
    await tick();

    const internals = muxInternals(t);
    expect(internals.streams.size).toBe(0);
    expect(internals.sendBuffer.length).toBe(0);
    expect(internals.sendBufferBytes).toBe(0);
  });

  it("client cancel: stream table drains and exactly one RST is emitted", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const s = t.openStream("/s/CancelPath");
    await tick();
    s.cancel();
    await tick();
    expect(muxInternals(t).streams.size).toBe(0);
    expect(sock.sent.filter((f) => f.kind === Kind.KIND_RST).length).toBe(1);
  });

  it("deadline timers do not leak past stream completion (fake timers)", async () => {
    vi.useFakeTimers();
    try {
      const sock = new FakeSocket({ autoOpen: false });
      const t = WsTransport.fromSocket(sock);
      sock.open();
      const s = t.openStream("/s/TimerLeak", { timeoutMs: 60_000 });
      s.closeSend();
      expect(vi.getTimerCount()).toBeGreaterThan(0);

      sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
      expect(await s.recv()).toBeNull();
      expect(vi.getTimerCount()).toBe(0); // deadline timer was cleared on close
    } finally {
      vi.useRealTimers();
    }
  });

  it("1000 streams opened and completed: all tables and buffers back to zero", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    await tick();

    for (let i = 0; i < 1000; i++) {
      const s = t.openStream(`/s/Bulk${i}`);
      s.send(new Uint8Array([i % 256]));
      s.closeSend();
      const id = 1 + i * 2;
      sock.inject({ streamId: id, kind: Kind.KIND_MSG, payload: new Uint8Array([1]) });
      sock.inject({ streamId: id, kind: Kind.KIND_END, status: { code: 0 } });
      expect((await s.recv())![0]).toBe(1);
      expect(await s.recv()).toBeNull();
    }

    const internals = muxInternals(t);
    expect(internals.streams.size).toBe(0);
    expect(internals.sendBuffer.length).toBe(0);
    expect(internals.sendBufferBytes).toBe(0);
  });
});

// ---- G. Concurrency ----

describe("G: concurrent streams", () => {
  it("several concurrent calls all reject on a drop — none hangs", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    await tick();
    const calls = await Promise.all([
      startCall(t, bidiMethod),
      startCall(t, serverStreamMethod),
      startCall(t, unaryMethod),
    ]);
    await tick();

    sock.drop();

    for (const [i, call] of calls.entries()) {
      const err = await expectRejects(call.done, 500, `concurrent call #${i}`);
      expect((err as WsStatusError).code).toBe(CODE_UNAVAILABLE);
    }
  });

  it("interleaved frames route to the right stream in order", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const a = t.openStream("/s/A");
    const b = t.openStream("/s/B");
    await tick();

    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([10]) });
    sock.inject({ streamId: 3, kind: Kind.KIND_MSG, payload: new Uint8Array([30]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_MSG, payload: new Uint8Array([11]) });
    sock.inject({ streamId: 3, kind: Kind.KIND_MSG, payload: new Uint8Array([31]) });
    sock.inject({ streamId: 1, kind: Kind.KIND_END, status: { code: 0 } });
    sock.inject({ streamId: 3, kind: Kind.KIND_END, status: { code: 0 } });

    expect((await a.recv())![0]).toBe(10);
    expect((await a.recv())![0]).toBe(11);
    expect(await a.recv()).toBeNull();
    expect((await b.recv())![0]).toBe(30);
    expect((await b.recv())![0]).toBe(31);
    expect(await b.recv()).toBeNull();
  });

  it("a healthy idle bidi call stays pending (pinned): no false terminal events", async () => {
    const sock = new FakeSocket();
    const t = WsTransport.fromSocket(sock);
    const res = await t.stream(bidiMethod, foreverSource(msg(1)), {});
    const pending = res.message[Symbol.asyncIterator]().next();
    await tick();

    await expectNeverSettles(pending, 60, "idle healthy call");
    t.close();
    await expectRejects(pending, 500, "idle call after close");
  });
});
