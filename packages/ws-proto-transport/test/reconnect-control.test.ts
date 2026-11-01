import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  WsTransport,
  WsStatusError,
  CODE_CANCELLED,
  CODE_UNAVAILABLE,
  FakeSocket,
} from "../src/index.js";
import type { ConnectionState, WsTransportOptions } from "../src/index.js";

const URL = "ws://test/rpc";

/**
 * rig builds a transport over manually-opened FakeSockets with a deterministic
 * clock (fake timers) and jitter pinned to its ceiling, so backoff delays are
 * exact: initialMs * 2^attempt capped at maxMs.
 */
function rig(opts: WsTransportOptions = {}) {
  const sockets: FakeSocket[] = [];
  const states: ConnectionState[] = [];
  const t = new WsTransport(URL, {
    backoff: { initialMs: 100, maxMs: 10_000 },
    ...opts,
    createSocket: () => {
      const s = new FakeSocket({ autoOpen: false });
      sockets.push(s);
      return s;
    },
  });
  const unsubscribe = t.subscribeState((s) => states.push(s));
  const last = () => sockets[sockets.length - 1]!;
  return { t, sockets, states, unsubscribe, last };
}

async function failure(p: Promise<unknown>): Promise<WsStatusError> {
  try {
    await p;
  } catch (e) {
    expect(e).toBeInstanceOf(WsStatusError);
    return e as WsStatusError;
  }
  throw new Error("expected rejection");
}

beforeEach(() => {
  vi.useFakeTimers({ now: 1_000_000 });
  vi.spyOn(Math, "random").mockReturnValue(0.999999);
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("connection state", () => {
  it("walks connecting -> connected -> waiting -> connecting -> connected", () => {
    const { t, states, last, sockets } = rig({ reconnect: true });
    expect(t.state).toEqual({ phase: "connecting" });
    last().open();
    last().drop();
    expect(t.state).toEqual({ phase: "waiting", retryAt: 1_000_100 });
    vi.advanceTimersByTime(100);
    expect(sockets.length).toBe(2);
    last().open();
    expect(states.map((s) => s.phase)).toEqual([
      "connecting",
      "connected",
      "waiting",
      "connecting",
      "connected",
    ]);
    t.close();
  });

  it("subscribeState emits the current state immediately and stops after unsubscribe", () => {
    const { t, states, unsubscribe, last } = rig({ reconnect: true });
    expect(states).toEqual([{ phase: "connecting" }]);
    unsubscribe();
    last().open();
    expect(states).toEqual([{ phase: "connecting" }]);
    t.close();
  });

  it("a throwing listener neither breaks the transport nor other listeners", () => {
    const deferred: (() => void)[] = [];
    vi.stubGlobal("queueMicrotask", (fn: () => void) => deferred.push(fn));
    const { t, states, last } = rig({ reconnect: true });
    t.subscribeState(() => {
      throw new Error("listener boom");
    });
    last().open();
    expect(t.state).toEqual({ phase: "connected" });
    expect(states.map((s) => s.phase)).toEqual(["connecting", "connected"]);
    // Both calls (immediate on subscribe, then on open) rethrow asynchronously.
    expect(deferred.length).toBe(2);
    t.close();
    vi.unstubAllGlobals();
    for (const fn of deferred) {
      expect(fn).toThrow("listener boom");
    }
  });

  it("without reconnect a drop ends in disconnected carrying the cause", () => {
    const { t, last } = rig();
    last().open();
    last().drop();
    expect(t.state.phase).toBe("disconnected");
    expect(t.state.phase === "disconnected" && t.state.error.code).toBe(CODE_UNAVAILABLE);
    t.close();
  });

  it("a subprotocol mismatch ends in disconnected without redialing", () => {
    const sockets: FakeSocket[] = [];
    const t = new WsTransport(URL, {
      reconnect: true,
      createSocket: () => {
        const s = new FakeSocket({ autoOpen: false, protocol: "" });
        sockets.push(s);
        return s;
      },
    });
    sockets[0]!.open();
    expect(t.state.phase).toBe("disconnected");
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);
    t.close();
  });

  it("close() ends in closed and never dials again", () => {
    const { t, last, sockets } = rig({ reconnect: true });
    last().open();
    last().drop();
    t.close();
    expect(t.state).toEqual({ phase: "closed" });
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);
  });

  it("fromSocket reports the wrapped socket's lifecycle and ignores dial controls", () => {
    const s = new FakeSocket({ autoOpen: false });
    const t = WsTransport.fromSocket(s);
    expect(t.state.phase).toBe("connecting");
    s.open();
    expect(t.state.phase).toBe("connected");
    t.pause();
    t.retryNow();
    t.resume();
    s.drop();
    expect(t.state.phase).toBe("disconnected");
    t.retryNow();
    expect(t.state.phase).toBe("disconnected");
    t.close();
  });
});

describe("pause / resume", () => {
  it("paused option holds the first dial; calls fail fast until resume()", async () => {
    const { t, sockets } = rig({ reconnect: true, paused: true });
    expect(t.state).toEqual({ phase: "paused" });
    expect(sockets.length).toBe(0);
    const err = await failure(t.openStream("/s/Early").recv());
    expect(err.code).toBe(CODE_UNAVAILABLE);
    t.resume();
    expect(sockets.length).toBe(1);
    expect(t.state).toEqual({ phase: "connecting" });
    t.close();
  });

  it("close() while paused before the first dial fails calls as cancelled", async () => {
    const { t } = rig({ paused: true });
    t.close();
    const err = await failure(t.openStream("/s/Late").recv());
    expect(err.code).toBe(CODE_CANCELLED);
  });

  it("pause() in waiting cancels the pending redial; resume() dials at once", () => {
    const { t, last, sockets } = rig({ reconnect: true });
    last().open();
    last().drop();
    t.pause();
    expect(t.state).toEqual({ phase: "paused" });
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);
    t.resume();
    expect(sockets.length).toBe(2);
    expect(t.state).toEqual({ phase: "connecting" });
    t.close();
  });

  it("pause() in connecting aborts the dial and fails its buffered calls", async () => {
    const { t, sockets } = rig({ reconnect: true });
    const call = t.openStream("/s/Buffered").recv();
    t.pause();
    expect(t.state).toEqual({ phase: "paused" });
    expect(sockets[0]!.closedCode).toBeDefined();
    expect((await failure(call)).code).toBe(CODE_UNAVAILABLE);
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);
    t.close();
  });

  it("pause() keeps a live socket; its later drop parks in paused instead of redialing", () => {
    const { t, last, sockets } = rig({ reconnect: true });
    last().open();
    t.pause();
    expect(t.state).toEqual({ phase: "connected" });
    expect(last().closedCode).toBeUndefined();
    last().drop();
    expect(t.state).toEqual({ phase: "paused" });
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(1);
    t.resume();
    expect(sockets.length).toBe(2);
    t.close();
  });

  it("resume() resets the backoff", () => {
    const { t, last } = rig({ reconnect: true, backoff: { initialMs: 100, maxMs: 10_000, stableMs: 1_000 } });
    for (let i = 0; i < 3; i++) {
      last().open();
      last().drop();
      vi.advanceTimersByTime(10_000);
    }
    t.pause();
    last().open();
    last().drop();
    t.resume();
    last().open();
    last().drop();
    expect(t.state).toEqual({ phase: "waiting", retryAt: Date.now() + 100 });
    t.close();
  });

  it("resume() while not paused is a no-op", () => {
    const { t, last, sockets } = rig({ reconnect: true });
    last().open();
    t.resume();
    expect(sockets.length).toBe(1);
    expect(t.state).toEqual({ phase: "connected" });
    t.close();
  });
});

describe("retryNow", () => {
  it("in waiting dials immediately and cancels the scheduled redial", () => {
    const { t, last, sockets } = rig({ reconnect: true });
    last().open();
    last().drop();
    t.retryNow();
    expect(sockets.length).toBe(2);
    expect(t.state).toEqual({ phase: "connecting" });
    vi.advanceTimersByTime(60_000);
    expect(sockets.length).toBe(2);
    t.close();
  });

  it("from disconnected (no auto-reconnect) dials a fresh socket that carries new calls", () => {
    const { t, last, sockets } = rig();
    last().open();
    last().drop();
    t.retryNow();
    expect(sockets.length).toBe(2);
    last().open();
    expect(t.state).toEqual({ phase: "connected" });
    t.openStream("/s/AfterRetry");
    expect(last().sent.length).toBe(1);
    t.close();
  });

  it.each([
    ["connecting", (r: ReturnType<typeof rig>) => void r],
    ["connected", (r: ReturnType<typeof rig>) => r.last().open()],
    ["paused", (r: ReturnType<typeof rig>) => r.t.pause()],
    ["closed", (r: ReturnType<typeof rig>) => r.t.close()],
  ])("is a no-op in %s", (phase, setup) => {
    const r = rig({ reconnect: true });
    setup(r);
    const before = r.sockets.length;
    r.t.retryNow();
    expect(r.t.state.phase).toBe(phase);
    expect(r.sockets.length).toBe(before);
    r.t.close();
  });
});

describe("backoff reset after a stable connection", () => {
  /** cycle opens the current socket, keeps it up for `upMs`, drops it and returns the scheduled delay. */
  function cycle(r: ReturnType<typeof rig>, upMs: number): number {
    r.last().open();
    vi.advanceTimersByTime(upMs);
    r.last().drop();
    const s = r.t.state;
    if (s.phase !== "waiting") throw new Error(`expected waiting, got ${s.phase}`);
    const delay = s.retryAt - Date.now();
    vi.advanceTimersByTime(delay);
    return delay;
  }

  it("connections that die before stableMs keep growing the backoff", () => {
    const r = rig({ reconnect: true, backoff: { initialMs: 100, maxMs: 10_000, stableMs: 1_000 } });
    expect([cycle(r, 0), cycle(r, 999), cycle(r, 10), cycle(r, 0)]).toEqual([100, 200, 400, 800]);
    r.t.close();
  });

  it("a connection that lived stableMs resets the backoff", () => {
    const r = rig({ reconnect: true, backoff: { initialMs: 100, maxMs: 10_000, stableMs: 1_000 } });
    expect([cycle(r, 0), cycle(r, 0), cycle(r, 1_000), cycle(r, 0)]).toEqual([100, 200, 100, 200]);
    r.t.close();
  });

  it("by default (stableMs unset) every open resets the backoff", () => {
    const r = rig({ reconnect: true });
    expect([cycle(r, 0), cycle(r, 0), cycle(r, 0)]).toEqual([100, 100, 100]);
    r.t.close();
  });

  it("the backoff is capped at maxMs", () => {
    const r = rig({ reconnect: true, backoff: { initialMs: 100, maxMs: 300, stableMs: 1_000 } });
    expect([cycle(r, 0), cycle(r, 0), cycle(r, 0), cycle(r, 0)]).toEqual([100, 200, 300, 300]);
    r.t.close();
  });
});

describe("dial timeout", () => {
  it("aborts a dial that never opens, fails its calls, and schedules a redial", async () => {
    const { t, sockets } = rig({ reconnect: true, dialTimeoutMs: 5_000 });
    const call = t.openStream("/s/Hung").recv();
    vi.advanceTimersByTime(4_999);
    expect(t.state).toEqual({ phase: "connecting" });
    vi.advanceTimersByTime(1);
    expect(sockets[0]!.closedCode).toBeDefined();
    const err = await failure(call);
    expect(err.code).toBe(CODE_UNAVAILABLE);
    expect(err.message).toContain("dial timeout");
    expect(t.state).toEqual({ phase: "waiting", retryAt: Date.now() + 100 });
    t.close();
  });

  it("without reconnect a timed-out dial ends in disconnected", () => {
    const { t } = rig({ dialTimeoutMs: 5_000 });
    vi.advanceTimersByTime(5_000);
    expect(t.state.phase).toBe("disconnected");
    t.close();
  });

  it("an opened socket is never timed out", () => {
    const { t, last } = rig({ reconnect: true, dialTimeoutMs: 5_000 });
    last().open();
    vi.advanceTimersByTime(60_000);
    expect(t.state).toEqual({ phase: "connected" });
    expect(last().closedCode).toBeUndefined();
    t.close();
  });

  it("applies to every redial, not only the first dial", () => {
    const { t, last, sockets } = rig({ reconnect: true, dialTimeoutMs: 5_000 });
    last().open();
    last().drop();
    vi.advanceTimersByTime(100);
    expect(sockets.length).toBe(2);
    vi.advanceTimersByTime(5_000);
    expect(sockets[1]!.closedCode).toBeDefined();
    expect(t.state.phase).toBe("waiting");
    t.close();
  });
});
