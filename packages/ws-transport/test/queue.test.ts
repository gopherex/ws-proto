import { describe, it, expect } from "vitest";
import { AsyncQueue } from "../src/queue.js";

describe("AsyncQueue", () => {
  it("delivers a pushed value to a waiting pull", async () => {
    const q = new AsyncQueue<number>();
    const pull = q.pull();
    q.push(42);
    expect(await pull).toEqual({ done: false, value: 42 });
  });

  it("buffers pushes that precede pulls (FIFO)", async () => {
    const q = new AsyncQueue<number>();
    q.push(1);
    q.push(2);
    expect(await q.pull()).toEqual({ done: false, value: 1 });
    expect(await q.pull()).toEqual({ done: false, value: 2 });
  });

  it("end() resolves the current and subsequent pulls as done", async () => {
    const q = new AsyncQueue<number>();
    const pull = q.pull();
    q.end();
    expect(await pull).toEqual({ done: true, value: undefined });
    expect(await q.pull()).toEqual({ done: true, value: undefined });
  });

  it("end() drains buffered values before reporting done", async () => {
    const q = new AsyncQueue<number>();
    q.push(7);
    q.end();
    expect(await q.pull()).toEqual({ done: false, value: 7 });
    expect(await q.pull()).toEqual({ done: true, value: undefined });
  });

  it("fail() rejects a waiting pull", async () => {
    const q = new AsyncQueue<number>();
    const pull = q.pull();
    q.fail(new Error("boom"));
    await expect(pull).rejects.toThrow("boom");
  });

  it("fail() rejects subsequent pulls too", async () => {
    const q = new AsyncQueue<number>();
    q.fail(new Error("boom"));
    await expect(q.pull()).rejects.toThrow("boom");
  });
});
