# Changelog

## 1.4.0

### Fixed

- **Long-lived bidi streams went permanently deaf after any connection loss.**
  After a network drop (laptop sleep, Wi-Fi change, proxy timeout) the response
  iterator of a streaming call whose request source was still open — the normal
  long-lived bidi pattern — never rejected and never completed: the stream's
  teardown path waited for the caller's (infinite, idle) request source before
  surfacing the error. The caller's reconnect loop therefore never ran, no new
  stream was opened, and the app silently stopped receiving pushes while unary
  RPCs kept working — the failure masqueraded as "everything is fine" until a
  full restart. Terminal events now finish the whole call immediately: the
  response iterator rejects with `CODE_UNAVAILABLE`, trailers resolve, and the
  request source is finalized via its iterator `return()` instead of being
  awaited.
- The read-side error now takes priority over a request-source error; the
  surfaced error is the root cause (e.g. connection loss), no longer masked by
  a later source failure.
- Early `break` out of a streaming response no longer hangs when the request
  source is still pending; the loop exit settles and an RST is sent.
- A dropped mux released neither its pre-open frame buffer nor stream
  registrations; both are now freed on drop, close, and protocol failure.
- Frames written after a connection was gone for good were silently buffered
  forever ("send into the void looked successful"). Writing into a defunct mux
  now fails loudly, and `openStream()` on a defunct mux fails the new stream
  immediately with the terminal error (`CODE_UNAVAILABLE` "connection lost" /
  `CODE_CANCELLED` "transport closed") instead of parking it forever — this
  includes streams opened in the reconnect backoff window before the
  replacement socket exists.
- Undecodable inbound frames are now ignored (like unknown frame kinds)
  instead of throwing from the socket message handler.

### Added

- Documented stream teardown contract (see "Stream teardown contract" in the
  repo README): terminal events finish both sides, the transport never waits
  on the caller's request source, read-side errors win, dead transports are
  observable, no teardown path blocks unboundedly.
- `FakeSocket.drop()` (browser-style error+close hard drop) and
  `FakeSocket.injectRaw()` (arbitrary inbound bytes) for lifecycle testing.
- Invariant-based lifecycle test harness: every invariant runs across all four
  RPC shapes and socket states, with explicit hang detection
  (`expectSettles`/`expectNeverSettles`) instead of runner timeouts.

## 1.3.1 and earlier

See git history (`git log --oneline -- packages/ws-proto-transport`).
