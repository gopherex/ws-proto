import { describe, it, expect } from "vitest";
import { iterateStream, firstResponse } from "../src/index.js";
import type { StreamResponse } from "../src/index.js";

// Build a fake StreamResponse whose header populates only as iteration starts,
// mirroring the real transport (leading header arrives with the first frame).
// Messages are plain numbers here (the helpers are type-erased at runtime).
function fakeStream(items: number[]): { r: StreamResponse; header: Record<string, string> } {
  const header: Record<string, string> = {};
  const trailer: Record<string, string> = { "x-tr": "T" };
  async function* gen(): AsyncIterable<unknown> {
    header["x-lead"] = "L"; // populated as the stream produces
    for (const it of items) yield it;
  }
  const r = { stream: true, method: {}, header, message: gen(), trailer } as unknown as StreamResponse;
  return { r, header };
}

describe("iterateStream", () => {
  it("fires onHeader once before the first message and onTrailer after", async () => {
    const events: string[] = [];
    const { r } = fakeStream([1, 2, 3]);
    const out: number[] = [];
    for await (const m of iterateStream(r, {
      onHeader: (h) => events.push("header:" + h["x-lead"]),
      onTrailer: (t) => events.push("trailer:" + t["x-tr"]),
    })) {
      out.push(m as unknown as number);
      events.push("msg:" + m);
    }
    expect(out).toEqual([1, 2, 3]);
    expect(events).toEqual(["header:L", "msg:1", "msg:2", "msg:3", "trailer:T"]);
  });
});

describe("firstResponse", () => {
  it("returns the single response message", async () => {
    const { r } = fakeStream([42]);
    const v = await firstResponse(r, "test.M");
    expect(v as unknown as number).toBe(42);
  });

  it("throws when the server sends no message", async () => {
    const { r } = fakeStream([]);
    await expect(firstResponse(r, "test.M")).rejects.toThrow(/without a response/);
  });
});
