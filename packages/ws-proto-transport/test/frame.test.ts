import { describe, it, expect } from "vitest";
import { encodeFrame, decodeFrame, Kind } from "../src/frame.js";

describe("frame codec", () => {
  it("round-trips an OPEN frame with method and headers", () => {
    const bytes = encodeFrame({
      streamId: 3,
      kind: Kind.KIND_OPEN,
      method: "/pkg.Svc/Do",
      headers: { k: "v" },
    });
    const f = decodeFrame(bytes);
    expect(f.streamId).toBe(3);
    expect(f.kind).toBe(Kind.KIND_OPEN);
    expect(f.method).toBe("/pkg.Svc/Do");
    expect(f.headers["k"]).toBe("v");
  });

  it("round-trips a MSG frame with payload bytes", () => {
    const payload = new Uint8Array([1, 2, 3, 4]);
    const f = decodeFrame(encodeFrame({ streamId: 5, kind: Kind.KIND_MSG, payload }));
    expect(f.kind).toBe(Kind.KIND_MSG);
    expect(Array.from(f.payload)).toEqual([1, 2, 3, 4]);
  });

  it("round-trips an END frame with a non-OK status and details", () => {
    const f = decodeFrame(
      encodeFrame({
        streamId: 7,
        kind: Kind.KIND_END,
        status: { code: 7, message: "nope", details: [new Uint8Array([9])] },
        headers: { trailer: "t" },
      }),
    );
    expect(f.kind).toBe(Kind.KIND_END);
    expect(f.status?.code).toBe(7);
    expect(f.status?.message).toBe("nope");
    expect(f.status?.details?.length).toBe(1);
    expect(f.headers["trailer"]).toBe("t");
  });

  it("defaults optional fields when omitted", () => {
    const f = decodeFrame(encodeFrame({ streamId: 1, kind: Kind.KIND_HALF_CLOSE }));
    expect(f.kind).toBe(Kind.KIND_HALF_CLOSE);
    expect(f.method).toBe("");
    expect(f.payload.length).toBe(0);
    expect(Object.keys(f.headers).length).toBe(0);
  });
});
