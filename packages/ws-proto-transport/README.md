# @gopherex/ws-proto-transport

WebSocket multiplexing transport for protobuf RPC services — the TypeScript
client runtime of [ws-proto](https://github.com/gopherex/ws-proto). Generated
clients (`protoc-gen-ws-es`) run unary and streaming RPCs over one multiplexed
WebSocket; see the [repo README](../../README.md) for the wire model, codegen,
usage, and interceptors.

## Stream teardown contract

Guaranteed on **every** terminal stream event (clean END, error END, RST,
abort, deadline, connection loss, deliberate close) and pinned by the invariant
test suite:

1. **A terminal read-side event finishes the whole call.** Response iteration
   settles and `trailer` is populated; the write side is force-finished.
2. **The transport never waits on the caller's request source.** An infinite,
   mostly idle source (long-lived bidi sync) is a supported pattern. On a
   terminal event the transport finalizes the source via its iterator
   `return()` — queued per the async generator protocol, it runs as soon as the
   source's pending `await` settles — instead of waiting for it to yield.
3. **The read-side error wins.** If both sides fail, the root cause (e.g.
   `CODE_UNAVAILABLE` "connection lost") is surfaced; a request-source error
   surfaces only when the read side ended cleanly.
4. **A dead transport is observable.** "Not yet open" buffers frames and
   flushes on open; "gone for good" fails new streams immediately with the
   terminal error and releases all buffered frames and stream registrations.
   A send into the void can never look successful.
5. **No teardown path blocks unboundedly.** Trailers resolve on every terminal
   path.

## Testing

`FakeSocket` (exported) drives the transport without networking: it captures
outbound frames, injects inbound frames (`inject`), arbitrary bytes
(`injectRaw`), and simulates hard connection loss (`error()` / browser-style
`drop()`). The lifecycle suite in `test/` formulates invariants once and runs
them across all RPC shapes and socket states with explicit hang detection.
