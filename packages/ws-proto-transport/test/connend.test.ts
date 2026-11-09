import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { WsTransport, WsStatusError, FakeSocket, Kind } from "../src/index.js";
import type { ConnectionState } from "../src/index.js";

const CODE_UNAUTHENTICATED = 16;
const detail = new Uint8Array([1, 2, 3]);

/** connEnd is the server's connection-level status frame. */
const connEnd = {
  streamId: 0,
  kind: Kind.KIND_CONN_END,
  status: { code: CODE_UNAUTHENTICATED, message: "session ended", details: [detail] },
};

function rig() {
  const sockets: FakeSocket[] = [];
  const states: ConnectionState[] = [];
  const t = new WsTransport("ws://test/rpc", {
    reconnect: true,
    backoff: { initialMs: 100, maxMs: 1_000 },
    createSocket: () => {
      const s = new FakeSocket({ autoOpen: false });
      sockets.push(s);
      return s;
    },
  });
  t.subscribeState((s) => states.push(s));
  const socket = (): FakeSocket => {
    const s = sockets.at(-1);
    if (s === undefined) {
      throw new Error("no socket dialed");
    }
    return s;
  };
  return { t, sockets, states, socket };
}

async function rejection(p: Promise<unknown>): Promise<WsStatusError> {
  try {
    await p;
  } catch (e) {
    if (e instanceof WsStatusError) {
      return e;
    }
    throw e;
  }
  throw new Error("expected rejection");
}

function expectEnded(err: WsStatusError): void {
  expect(err.code).toBe(CODE_UNAUTHENTICATED);
  expect(err.message).toBe("session ended");
  expect(err.details).toEqual([detail]);
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("KIND_CONN_END", () => {
  it("fails live streams with the status, reports it in the state and never redials", async () => {
    const { t, sockets, states, socket } = rig();
    socket().open();
    const live = t.openStream("/s/Live").recv();

    socket().inject(connEnd);
    socket().close(1008, "session ended");

    expectEnded(await rejection(live));
    const last = states.at(-1);
    expect(last?.phase).toBe("disconnected");
    if (last?.phase === "disconnected") {
      expectEnded(last.error);
    }
    vi.advanceTimersByTime(10_000);
    expect(sockets.length).toBe(1);
    t.close();
  });

  it("reaches a client without live streams and fails its later calls", async () => {
    const { t, states, socket } = rig();
    socket().open();

    socket().inject(connEnd);

    const last = states.at(-1);
    expect(last?.phase).toBe("disconnected");
    expectEnded(await rejection(t.openStream("/s/After").recv()));
    t.close();
  });

  it("ignores CONN_END on a stream id other than 0", async () => {
    const { t, states, socket } = rig();
    socket().open();
    const live = t.openStream("/s/Live");

    socket().inject({ ...connEnd, streamId: 1 });

    expect(states.at(-1)?.phase).toBe("connected");
    socket().inject({ streamId: 1, kind: Kind.KIND_END });
    await expect(live.recv()).resolves.toBeNull();
    t.close();
  });
});
