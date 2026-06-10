import { describe, it, expect } from "vitest";
import { applyInterceptors } from "../src/interceptor.js";
import type { Interceptor, AnyFn, UnaryResponse } from "../src/interceptor.js";

describe("applyInterceptors", () => {
  it("runs interceptors outermost-first and lets them mutate the request header", async () => {
    const calls: string[] = [];
    const a: Interceptor = (next) => async (req) => {
      calls.push("a-before");
      req.header["x"] = "1";
      const r = await next(req);
      calls.push("a-after");
      return r;
    };
    const b: Interceptor = (next) => async (req) => {
      calls.push("b-before");
      const r = await next(req);
      calls.push("b-after");
      return r;
    };
    const terminal: AnyFn = async (req) => {
      calls.push("terminal:" + req.header["x"]);
      return {
        stream: false,
        method: req.method,
        header: {},
        message: undefined,
        trailer: {},
      } as unknown as UnaryResponse;
    };

    const chain = applyInterceptors(terminal, [a, b]);
    await chain({
      stream: false,
      method: { typeName: "s", name: "M", kind: "unary" } as never,
      header: {},
      message: {} as never,
    });

    // a is outermost (registered first), then b, then terminal; unwinds in reverse.
    expect(calls).toEqual(["a-before", "b-before", "terminal:1", "b-after", "a-after"]);
  });

  it("returns the terminal unchanged when there are no interceptors", async () => {
    const terminal: AnyFn = async (req) =>
      ({ stream: false, method: req.method, header: {}, message: 0, trailer: {} }) as unknown as UnaryResponse;
    expect(applyInterceptors(terminal, [])).toBe(terminal);
    expect(applyInterceptors(terminal, undefined)).toBe(terminal);
  });
});
