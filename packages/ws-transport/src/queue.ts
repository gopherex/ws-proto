/**
 * AsyncQueue bridges synchronous pushes (from the WebSocket onmessage handler)
 * to asynchronous pulls (recv() / async iteration).
 *
 * Semantics:
 *   - push(v): enqueue a value. If a pull is waiting, it is resolved immediately.
 *   - end():   signal a clean end-of-stream. Buffered values are still drained
 *              by subsequent pulls; once drained, pulls resolve `{ done: true }`.
 *   - fail(e): signal an error end-of-stream. Buffered values are discarded and
 *              all current/future pulls reject with `e`.
 *
 * Backpressure: v1 is intentionally UNBOUNDED. A slow consumer cannot slow a
 * fast producer; memory grows with the backlog. Credit-based per-stream flow
 * control is a documented future extension (see transport design spec).
 */
export interface PullResult<T> {
  done: boolean;
  value: T | undefined;
}

type Waiter<T> = {
  resolve: (r: PullResult<T>) => void;
  reject: (e: unknown) => void;
};

export class AsyncQueue<T> {
  private readonly values: T[] = [];
  private readonly waiters: Waiter<T>[] = [];
  private ended = false;
  private error: unknown = undefined;

  push(value: T): void {
    if (this.ended || this.error !== undefined) {
      return; // ignore late pushes after end/fail
    }
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ done: false, value });
      return;
    }
    this.values.push(value);
  }

  end(): void {
    if (this.ended || this.error !== undefined) {
      return;
    }
    this.ended = true;
    // No buffered values left -> resolve all pending pulls as done.
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      waiter.resolve({ done: true, value: undefined });
    }
  }

  fail(error: unknown): void {
    if (this.error !== undefined) {
      return;
    }
    this.error = error ?? new Error("AsyncQueue failed");
    this.values.length = 0;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      waiter.reject(this.error);
    }
  }

  pull(): Promise<PullResult<T>> {
    if (this.values.length > 0) {
      const value = this.values.shift()!;
      return Promise.resolve({ done: false, value });
    }
    if (this.error !== undefined) {
      return Promise.reject(this.error);
    }
    if (this.ended) {
      return Promise.resolve({ done: true, value: undefined });
    }
    return new Promise<PullResult<T>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }
}
