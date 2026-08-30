# ws-proto

A WebSocket transport for protobuf RPC services — the WebSocket analog of gRPC /
[Connect](https://connectrpc.com/). One persistent connection multiplexes any
number of concurrent RPC streams (unary, server-streaming, client-streaming,
bidi) using a small protobuf framing envelope.

It ships two code generators and two runtimes:

| Unit | Language | Analog | Provides |
|---|---|---|---|
| `protoc-gen-go-ws` | Go (protogen) | `protoc-gen-go-grpc` / `connect-go` | `*_ws.pb.go`: typed handler interface, typed client, stream wrappers, gRPC bridge |
| `@gopherex/protoc-gen-ws-es` | TypeScript (`@bufbuild/protoplugin`) | `protoc-gen-connect-es` | `*_ws_pb.ts`: typed service client |
| `wsrpc` (Go module) | Go | grpc-go / connect runtime | mux, framing, HTTP-upgrade server, dial client |
| `@gopherex/ws-proto-transport` | TypeScript | connect-web transport | mux, framing, browser/node client |

The ES generator interoperates with [`protoc-gen-es`](https://github.com/bufbuild/protobuf-es)
v2 output (it imports the generated `*_pb.ts` message types and schemas).

---

## Wire model

- **One** WebSocket connection per peer link, carrying **binary** frames.
- Each binary message is a marshaled `Frame` envelope:

  ```proto
  message Frame {
    uint32 stream_id = 1;             // client-allocated, monotonic odd ids
    Kind   kind      = 2;             // OPEN | MSG | HALF_CLOSE | END | RST | HEADER | WINDOW_UPDATE
    map<string,string> headers = 3;   // OPEN: metadata + deadline; END: trailers
    bytes  payload   = 4;             // MSG: one marshaled request/response message
    Status status    = 5;             // END only: gRPC-style code/message/details
    string method    = 6;             // OPEN only: "/pkg.Service/Method"
    uint32 window    = 8;             // WINDOW_UPDATE only: credit delta (bytes) returned to the sender
  }
  ```

- Streams are **client-initiated**, one stream per RPC. Flow:
  `OPEN → MSG* → HALF_CLOSE → MSG* → END(status)`; `RST` aborts either side.
- The handshake negotiates the `Sec-WebSocket-Protocol` subprotocol **`wsrpc.v1`**.
- Per-RPC metadata and deadlines ride **in-band** on the `OPEN` frame.
  Connection-level concerns (auth, tenant routing) ride the **HTTP Upgrade
  request headers**, which proxies can inspect — see
  [Deploying behind a proxy](#deploying-behind-a-proxy).

---

## Flow control

Each stream has a **per-stream credit window** (default **256 KiB**) for
backpressure, symmetric in both directions. The receiver advertises credit; a
sender's `Send` **blocks** (it never drops) until enough credit is available, so
a well-behaved sender can't overrun a slow receiver:

- The receiver returns credit (a `WINDOW_UPDATE` frame carrying a byte delta) as
  it **consumes** messages — not merely as they arrive. That is what makes the
  window real backpressure.
- A single message **larger than the whole window** is still delivered: a `MSG`
  may be sent whenever the window is `> 0`, after which the window is allowed to
  go negative; subsequent sends then wait for credit. This matches gRPC/HTTP2
  semantics and avoids a deadlock on an oversized message.
- The window assumes the same default on both peers with **no handshake**. Tune
  it with `WithInitialWindow(n)` (Go server), `WithDialInitialWindow(n)` (Go
  client), or `initialWindow` (TS `WsTransportOptions`); `n <= 0` keeps the
  default. Mixing a windowed peer with a non-windowed one can stall a sender once
  the initial window is exhausted, so ship both sides together.

The **receive backlog** is a **backstop** bounded by **bytes**, aligned with the
window: a misbehaving peer that ignores the window can't grow memory without
limit — its stream is reset with `ResourceExhausted` — while a well-behaved peer
(which may have many tiny frames but ≤ a window of bytes in flight) never hits
it. The **send backlog** of unsent `send()` data is likewise bounded (TS
`maxSendBuffer`, default 16 MiB): a caller that floods `send()` while a stalled
peer withholds credit aborts its own stream rather than buffering without limit.

> `WithReceiveBuffer` / `WithDialReceiveBuffer` (Go) and `maxReceiveQueue` (TS)
> are **deprecated no-ops** — the old frame-count bound was replaced by the
> byte-based bound above.

---

## Install

### Go plugin + runtime

```bash
# the code generator (a protoc/easyp/buf plugin)
go install github.com/gopherex/ws-proto/cmd/protoc-gen-go-ws@latest

# the runtime, imported by generated code
go get github.com/gopherex/ws-proto/wsrpc
```

### npm packages (GitHub Packages)

Both packages are published to **GitHub Packages** under the `@gopherex` scope.
Point the scope at the GitHub registry and authenticate with a token that has
`read:packages` (add to `~/.npmrc` or the project `.npmrc`):

```ini
@gopherex:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

```bash
npm install @gopherex/ws-proto-transport
npm install -D @gopherex/protoc-gen-ws-es @bufbuild/protoc-gen-es @bufbuild/buf
```

---

## Define your service

```proto
// echo/v1/echo.proto
syntax = "proto3";
package echo.v1;
option go_package = "example.com/gen/echo/v1;echov1";

service EchoService {
  rpc Unary(UnaryRequest) returns (UnaryResponse);
  rpc ServerStream(ServerStreamRequest) returns (stream ServerStreamResponse);
  rpc ClientStream(stream ClientStreamRequest) returns (ClientStreamResponse);
  rpc Bidi(stream BidiRequest) returns (stream BidiResponse);
}
// ... messages ...
```

No proto options are required; routes derive from `/pkg.Service/Method`.

---

## Generate

### Go (protoc-gen-go + protoc-gen-go-grpc + protoc-gen-go-ws)

Using [`easyp`](https://github.com/easyp-tech/easyp) (see `easyp.yaml`):

```yaml
generate:
  inputs:
    - directory: proto
  plugins:
    - { name: go,      out: gen, opts: { paths: source_relative } }
    - { name: go-grpc, out: gen, opts: { paths: source_relative } }   # only if you use the gRPC bridge
    - { name: go-ws,   out: gen, opts: { paths: source_relative } }
```

```bash
easyp generate   # emits *.pb.go, *_grpc.pb.go, *_ws.pb.go
```

### TypeScript (protoc-gen-es + protoc-gen-ws-es)

`buf.gen.yaml`:

```yaml
version: v2
plugins:
  - local: protoc-gen-es
    out: src/gen
    opt: [target=ts, import_extension=js]
  - local: protoc-gen-ws-es
    out: src/gen
    opt: [import_extension=js]
inputs:
  - directory: proto
```

```bash
npx buf generate   # emits *_pb.ts (messages) and *_ws_pb.ts (clients)
```

---

## Use it

### Go server

```go
srv := wsrpc.NewServer(
    // REQUIRED: an origin policy. The server fails closed — it rejects every
    // upgrade (HTTP 403) unless you choose one of:
    //   WithOriginPatterns("app.example.com")  // restrict browser origins (CSRF defense)
    //   WithInsecureSkipOriginCheck()          // accept any origin (only if auth gates the Upgrade)
    wsrpc.WithOriginPatterns("app.example.com"),
    // Cap concurrent server streams per connection (default 1000); excess OPENs
    // are rejected with codes.ResourceExhausted. 0 disables (unlimited).
    wsrpc.WithMaxConcurrentStreams(1000),
    // Active keepalive pings keep the connection alive through proxy idle timeouts.
    wsrpc.WithKeepalive(20*time.Second, 10*time.Second),
    // Read auth/tenant info off the proxy-visible Upgrade request headers.
    wsrpc.WithConnContext(func(ctx context.Context, r *http.Request) context.Context {
        return context.WithValue(ctx, authKey{}, r.Header.Get("Authorization"))
    }),
)
echov1.RegisterEchoServiceHandler(srv, myHandler{})

http.Handle("/rpc", srv)          // srv is an http.Handler (a WebSocket upgrade endpoint)
log.Fatal(http.ListenAndServe(":8080", nil))
```

`myHandler` implements the generated `EchoServiceHandler` interface
(`Unary(ctx, *Req) (*Res, error)`, `ServerStream(ctx, *Req, stream) error`, …).
You can also wrap an existing protoc-gen-go-grpc server with
`echov1.EchoServiceFromGRPC(grpcImpl)`.

### Go client

```go
cc, err := wsrpc.Dial(ctx, "ws://localhost:8080/rpc",
    wsrpc.WithHeader(http.Header{"Authorization": {"Bearer " + token}}),
)
defer cc.Close()

client := echov1.NewEchoServiceWSClient(cc)
res, err := client.Unary(ctx, &echov1.UnaryRequest{Name: "bob"})
```

### In-memory transport (no socket)

For tests, single-process topologies, or any case where a WebSocket round-trip is
unwanted, `wsrpc.NewPipe` returns two connected in-memory `FrameConn` ends — the
ws-proto analog of [`google.golang.org/grpc/test/bufconn`](https://pkg.go.dev/google.golang.org/grpc/test/bufconn).
Feed one end to `Server.ServeConn` and the other to `wsrpc.DialConn`:

```go
srvEnd, cliEnd := wsrpc.NewPipe()
go srv.ServeConn(ctx, srvEnd) // dispatches handlers exactly like ServeHTTP

cc, err := wsrpc.DialConn(ctx, cliEnd, wsrpc.WithDialInitialWindow(64*1024))
defer cc.Close()

client := echov1.NewEchoServiceWSClient(cc) // generated client works unchanged
res, err := client.Unary(ctx, &echov1.UnaryRequest{Name: "bob"})
```

`ServeConn` runs the same mux, middleware, interceptor bridge, and max-streams
cap as `ServeHTTP`, but skips origin policy, subprotocol negotiation, and
keepalive — those defend against browser/proxy concerns with no meaning on a
handed-off in-process channel. `DialConn` accepts the same `DialOption`s as
`Dial` (WebSocket-handshake options like `WithHeader`/`WithReconnect` are
ignored; `WithClientInterceptor` and `WithDialInitialWindow` do take effect).
The transport is abstracted by the exported `wsrpc.FrameConn` interface, so you
may also plug your own (over `net.Pipe`, a Unix socket, etc.).

### TypeScript client (browser or node)

```ts
import { WsTransport } from "@gopherex/ws-proto-transport";
import { EchoServiceClient } from "./gen/echo_ws_pb.js";

const client = new EchoServiceClient(new WsTransport("wss://api.example.com/rpc"));

// unary
const res = await client.unary({ name: "bob" });

// server streaming
for await (const msg of client.serverStream({ count: 3 })) {
  console.log(msg.index);
}
```

Every method takes an optional `CallOptions` as its last argument:

```ts
const controller = new AbortController();

const res = await client.unary(
  { name: "bob" },
  {
    // request metadata -> headers on the opening frame
    headers: { authorization: "bearer …" },
    // cancellation: aborting sends RST; the call rejects with a WsStatusError
    signal: controller.signal,
    // optional leading response headers, sent before the first message
    onHeader: (header) => console.log(header["x-served-by"]),
    // response trailers, reported once the server ends the stream
    onTrailer: (trailer) => console.log(trailer["x-elapsed-ms"]),
  },
);

// later, to cancel an in-flight call:
controller.abort();
```

`CallOptions` (`headers`, `signal`, `onHeader`, `onTrailer`) is generated into
each `*_ws_pb.ts` and accepted by all four RPC kinds. `onHeader` fires with the
optional **leading** response metadata the server may send before the first
message (or `{}` if none was sent); `onTrailer` fires with the END trailers
after the response stream completes cleanly.

In Node, provide a `WebSocket` global (e.g. the [`ws`](https://www.npmjs.com/package/ws)
package) before constructing the transport.

### Auto-reconnect (opt-in)

By default a connection is a single socket: if it drops, every in-flight RPC
fails and the connection stays down. You can opt into automatic reconnection of
the **connection** (with exponential backoff + jitter) on both runtimes:

```go
// Go: redial on connection loss. Backoff defaults to 100ms initial / 30s max.
cc, err := wsrpc.Dial(ctx, "ws://localhost:8080/rpc", wsrpc.WithReconnect())
// tune the schedule:
cc, err = wsrpc.Dial(ctx, "ws://localhost:8080/rpc",
    wsrpc.WithReconnect(wsrpc.WithBackoff(200*time.Millisecond, 10*time.Second)),
)
```

```ts
// TS: only WsTransport reconnects (it owns dialing); fromSocket() does not.
const transport = new WsTransport("wss://api.example.com/rpc", {
  reconnect: true,
  backoff: { initialMs: 200, maxMs: 10_000 }, // optional; defaults 100ms / 30s
});
```

**There is no stream resume.** When the connection drops, in-flight RPCs fail
with **`codes.Unavailable`** (Go) / **`CODE_UNAVAILABLE` (14)** (TS) — message
`"connection lost"` — and **the caller must retry** them; the server keeps no
per-stream session state. RPCs opened in the backoff window **before** the
replacement socket exists fail immediately with `CODE_UNAVAILABLE` (the caller's
retry loop redials); once the replacement socket is being dialed, new RPCs
buffer their frames and flush when it opens. A deliberate `Close()` instead
fails in-flight streams with `codes.Canceled` ("transport closed"), so an
intentional shutdown stays distinguishable from a transport disconnect.

Without `WithReconnect` / `{ reconnect: true }` the behavior is unchanged: a
dropped connection fails everything with `Unavailable` and stays down.

### Stream teardown contract (TS transport)

The TypeScript transport guarantees the following on **every** terminal stream
event (clean END, error END, RST, abort, deadline, connection loss, deliberate
close). These are public contract, pinned by the invariant test suite:

1. **A terminal read-side event finishes the whole call.** Response iteration
   settles (completes or rejects) and `trailer` is populated. The write side is
   force-finished; nothing keeps the call half-alive.
2. **The transport never waits on the caller's request source.** A request
   source may be infinite and idle (a long-lived bidi sync channel is the
   normal pattern). On a terminal event the transport *finalizes* the source —
   it calls the iterator's `return()` so the caller's generator unwinds its
   `finally` blocks — rather than waiting for it to yield. Per the async
   generator protocol, a queued `return()` runs as soon as the source's pending
   `await` settles; JavaScript cannot interrupt the `await` itself.
3. **The read-side error is the root cause and wins.** If both the read side
   and the request source fail, the read-side error (e.g. `CODE_UNAVAILABLE`
   "connection lost") is surfaced; a source error surfaces only when the read
   side ended cleanly.
4. **A dead transport is observable.** "Socket not yet open" (frames buffer,
   then flush) and "socket gone for good" (dropped/closed) are distinct states.
   On a defunct connection, new streams fail immediately with the terminal
   error, and the old connection releases every buffered frame and stream
   registration — a send into the void can never look successful.
5. **No teardown path blocks unboundedly.** Trailers resolve on every terminal
   path, so awaiting `responseHeaders()` / draining the response iterator after
   an error always settles promptly.

---

## Proto-agnostic proxying (Go server)

A gateway can relay RPCs without importing any generated code: the method name
travels on the OPEN frame (`Stream.Method()`), payloads are opaque bytes.

- `wsrpc.WithUnknownHandler(h)` installs a fallback `Handler` invoked for any
  method with no registered handler (instead of `codes.Unimplemented`).
  Middleware wraps it like any registered handler.
- `Stream.SendRaw(b)` / `Stream.RecvRaw()` send and receive MSG payloads without
  marshaling. Flow control, drain-first delivery and terminal semantics are
  identical to `Send`/`Recv` (`io.EOF` on clean END, `*Status` — code, message
  **and details** — as the error otherwise).

The relay shape (upstream may be another wsrpc conn or a gRPC client stream with
a passthrough codec): pump `down.RecvRaw() → up.Send`, `up.Recv → down.SendRaw()`;
on downstream `io.EOF` half-close the upstream; on upstream end copy trailers
with `down.SetTrailer(...)` and return the upstream error as-is — `FromError`
preserves gRPC status codes and details. See `wsrpc/proxy_test.go` for a
complete two-hop example.

## Middleware & gRPC interceptors (Go)

Two complementary mechanisms add cross-cutting behavior on the server.

### Native wsrpc middleware

`wsrpc.WithMiddleware` wraps every RPC (all four kinds) with a `Middleware`. The
first listed runs outermost; a middleware can read the stream, short-circuit by
returning an error, or wrap the call. Handler panics, middleware panics and
interceptor panics are recovered and surface to the client as `codes.Internal`.

```go
func Logging(next wsrpc.Handler) wsrpc.Handler {
    return func(ctx context.Context, s *wsrpc.Stream) error {
        start := time.Now()
        err := next(ctx, s)
        log.Printf("%s -> %v (%s)", s.Method(), wsrpc.FromError(err).Code, time.Since(start))
        return err
    }
}

func Auth(next wsrpc.Handler) wsrpc.Handler {
    return func(ctx context.Context, s *wsrpc.Stream) error {
        if s.Header()["x-token"] == "" {
            return wsrpc.Errorf(codes.Unauthenticated, "missing token")
        }
        return next(ctx, s)
    }
}

srv := wsrpc.NewServer(wsrpc.WithMiddleware(Logging, Auth)) // Logging outermost
```

### Transport stats (Go server)

`wsrpc.WithStats` installs a `ServerStats` observer for transport-level events
that per-RPC middleware cannot see: connection open/close, rejected upgrades
(`origin_policy` / `upgrade`), stream start/end with the terminal status code,
rejected streams (`max_streams` / `recv_overflow`), and per-message flow with
payload sizes. Every field is optional (nil = skipped); callbacks must be fast
and non-blocking — `MsgReceived` runs on the shared read loop.

```go
srv := wsrpc.NewServer(wsrpc.WithStats(&wsrpc.ServerStats{
    ConnOpened:  func(ctx context.Context) { connections.Add(ctx, 1) },
    ConnClosed:  func(ctx context.Context) { connections.Add(ctx, -1) },
    MsgSent:     func(ctx context.Context, method string, n int) { egress.Add(ctx, int64(n)) },
    MsgReceived: func(ctx context.Context, method string, n int) { ingress.Add(ctx, int64(n)) },
}))
```

### Client interceptors (Go & TS)

Both clients support typed, **message-level** interceptors (the connect-web
analog), installed connection-wide and applied to every generated call. An
interceptor can read/modify request metadata and the typed request/response
**messages**, short-circuit a call, and read trailers — across all four method
kinds. The first interceptor listed is the outermost.

**Go** — `wsrpc.WithClientInterceptor`. Use `UnaryInterceptorFunc` /
`StreamInterceptorFunc` to wrap only one call shape:

```go
auth := wsrpc.UnaryInterceptorFunc(func(next wsrpc.UnaryFunc) wsrpc.UnaryFunc {
    return func(ctx context.Context, req *wsrpc.AnyRequest) (*wsrpc.AnyResponse, error) {
        req.Header["authorization"] = "Bearer " + token // injected onto OPEN
        return next(ctx, req)
    }
})
cc, _ := wsrpc.Dial(ctx, url, wsrpc.WithClientInterceptor(auth))
```

Streaming interceptors wrap the `StreamingClientConn` to observe each
`Send`/`Receive`:

```go
trace := wsrpc.StreamInterceptorFunc(func(next wsrpc.StreamingClientFunc) wsrpc.StreamingClientFunc {
    return func(ctx context.Context, spec wsrpc.MethodSpec, header map[string]string) (wsrpc.StreamingClientConn, error) {
        conn, err := next(ctx, spec, header)
        if err != nil { return nil, err }
        return &myWrapConn{StreamingClientConn: conn}, nil // override Send/Receive
    }
})
```

**TS** — `WsTransportOptions.interceptors`. One `Interceptor` type covers unary
and streaming (`req.stream` discriminates):

```ts
const auth: Interceptor = (next) => async (req) => {
  req.header["authorization"] = `Bearer ${token}`; // injected onto OPEN
  return next(req);
};
const transport = new WsTransport(url, { interceptors: [auth] });
const client = new EchoServiceClient(transport);
```

The interceptor sees `req.method` (the typed `MethodInfo`), may replace
`req.message` (unary) before serialization, and may wrap the response message
stream. This is the recommended way to propagate auth — see also "Auth & origin
behind a proxy" for the connection-level options.

### Response headers & trailers (Go)

A server handler may send **leading** response metadata once, before the first
message, with `stream.SendHeader(map[string]string{...})`, and **trailers**
(flushed with the `END` frame) with `stream.SetTrailer(map[string]string{...})`:

```go
func (impl) ServerStream(ctx context.Context, req *pb.Req, s *pb.Svc_StreamServerWS) error {
    _ = s.SendHeader(map[string]string{"x-served-by": "node-1"}) // before any Send
    defer s.SetTrailer(map[string]string{"x-elapsed-ms": "12"})
    return s.Send(&pb.Res{ /* … */ })
}
```

On the client, the leading headers are read with `stream.Header()` (returns `{}`
if the server sent none) and the trailers with `stream.Trailer()` (populated
once the stream ends). `SendHeader` after the first `Send` returns
`codes.FailedPrecondition`.

### Reusing existing gRPC interceptors via the bridge

When you serve an existing protoc-gen-go-grpc server through the bridge, you can
run your real `grpc.UnaryServerInterceptor` / `grpc.StreamServerInterceptor`
(auth, recovery, validation, otel, rate limiting, anything from
[go-grpc-middleware](https://github.com/grpc-ecosystem/go-grpc-middleware)) —
unchanged — over the WebSocket transport:

```go
handler := echov1.EchoServiceFromGRPC(grpcImpl,
    wsrpc.WithUnaryInterceptor(grpc.ChainUnaryInterceptor(
        recovery.UnaryServerInterceptor(),
        auth.UnaryServerInterceptor(authFn),
        otelgrpc.UnaryServerInterceptor(),
    )),
    wsrpc.WithStreamInterceptor(grpc.ChainStreamInterceptor(
        recovery.StreamServerInterceptor(),
        auth.StreamServerInterceptor(authFn),
    )),
)
echov1.RegisterEchoServiceHandler(srv, handler)
```

The bridge mirrors grpc-go's own handler/interceptor flow (a generic
`grpc.ServerStream` adapter wrapped in `grpc.GenericServerStream[Req,Res]`), so
`info.FullMethod` is the real `/pkg.Service/Method` and interceptor chaining
works as in a normal gRPC server. **Streaming response metadata** set on the
`grpc.ServerStream` via `SetHeader`/`SendHeader`/`SetTrailer` **is** propagated:
leading headers become a `KIND_HEADER` frame and trailers ride the `END` frame.
**Unary response metadata** set via `grpc.SetHeader`/`SendHeader`/`SetTrailer`
**is** propagated too: the unary path has no `ServerStream`, so the bridge
installs a `grpc.ServerTransportStream` that forwards into a per-call metadata
sink the generated registrar flushes after the handler returns (native unary
handlers can set the same via `wsrpc.SetHeader`/`wsrpc.SetTrailer`).
**Limitation:** a stream interceptor that wraps the `ServerStream` must delegate
`SendMsg`/`RecvMsg` to the embedded stream (the idiomatic pattern).

---

## Deploying behind a proxy

> **This is the part that breaks naive WebSocket deployments.** Read it.

**Transport facts to design around:**

- One long-lived connection per client. Use **`wss://`** (TLS) in production.
- Binary frames; negotiated subprotocol **`wsrpc.v1`**.
- The server sends **keepalive pings every ~20s** (`WithKeepalive`) so idle
  bidi/server-stream RPCs survive proxy idle timeouts. Browser clients cannot
  initiate WebSocket pings — they rely on the server's pings, so always keep
  server keepalive enabled.
- Default **max message size is 16 MiB** (`WithReadLimit`); keep it **below the
  smallest message cap in your proxy chain** (e.g. Cloudflare's 100 MB).
- **Compression is off** by default (payloads are already protobuf; `permessage-deflate`
  is occasionally mangled by intermediaries and costs memory). Enable per-message-deflate
  with `wsrpc.WithCompression(...)` on the server and `wsrpc.WithDialCompression(...)` on
  the client (e.g. `wsrpc.CompressionContextTakeover`). Browser clients negotiate it
  automatically when the server accepts it.
- The proxy → upstream hop must be **HTTP/1.1** with `Upgrade`/`Connection`
  preserved. (coder/websocket uses the RFC 6455 HTTP/1.1 handshake, not RFC 8441
  WebSocket-over-HTTP/2.)

### Caddy

Works out of the box — `reverse_proxy` auto-detects the WebSocket upgrade and
streams it with no idle timeout:

```caddyfile
api.example.com {
    reverse_proxy localhost:8080
}
```

### nginx

You **must** raise `proxy_read_timeout` above the keepalive interval and pass the
upgrade headers:

```nginx
location /rpc {
    proxy_pass http://upstream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_read_timeout 3600s;   # > server keepalive interval
    proxy_send_timeout 3600s;
    proxy_buffering off;
}
```

### Cloudflare / AWS ALB / other LBs

- **Cloudflare**: WSS works on all plans; idle WS is dropped at ~100s and the
  message cap is 100 MB — the 20s keepalive covers the former, the 16 MiB read
  limit stays under the latter.
- **AWS ALB**: default idle timeout is 60s; the keepalive keeps it warm, but you
  may also raise the LB idle timeout above the ping interval.

### Auth & origin behind a proxy

- Put connection-level auth (bearer token, cookie, tenant) on the **Upgrade
  request headers** — set them client-side with `wsrpc.WithHeader(...)` and read
  them server-side with `wsrpc.WithConnContext(...)`. These are visible to proxies
  for routing/authorization; per-RPC metadata stays in-band and is not.
- The server **fails closed** on origin policy: it rejects every upgrade (HTTP
  403) unless you set `wsrpc.WithOriginPatterns(...)` (your real frontend origins;
  correct CSRF behavior) **or** `wsrpc.WithInsecureSkipOriginCheck()` (accept any
  origin — only safe when auth gates the Upgrade request). Use `"*"` as a pattern
  to allow any origin while still satisfying the policy gate.
- The client validates the negotiated `wsrpc.v1` subprotocol after the handshake
  and rejects a connection where the server (or a proxy) failed to select it.
- Trust `X-Forwarded-For` / `X-Forwarded-Proto` only when set by a proxy you control.

---

## Compatibility & RFC notes

- **RFC 6455** WebSocket: standard HTTP/1.1 `Upgrade` handshake (via
  [`coder/websocket`](https://github.com/coder/websocket)); binary frames;
  `Sec-WebSocket-Protocol: wsrpc.v1` negotiated; ping/pong control frames used for
  keepalive; `close` on graceful teardown. Per-RPC errors are carried **in-band**
  in the `END`/`RST` frame `Status` (not WebSocket close codes), because one
  socket is shared by many streams.
- **RFC 8441** (WebSocket over HTTP/2) is not used; force HTTP/1.1 on the
  proxy→upstream hop.
- Read limits, keepalive interval/timeout, origin patterns, Upgrade headers and
  the gRPC bridge are all covered by tests, including a Go↔TypeScript
  cross-language integration test.

---

## Development

```bash
make help          # list targets
make test          # gofmt + go vet + go test -race
make test-ts       # build + test the npm workspaces
make gen           # regenerate transport/*.pb.go
make gen-example   # rebuild the plugin and regenerate the golden example/
make release       # interactive, tag-driven release (Go module + npm share one version)
```

Pushing a `vX.Y.Z` tag (produced by `make release`) triggers
`.github/workflows/release.yml`: it gates on CI, builds the `protoc-gen-go-ws`
binaries for all platforms and attaches them to a GitHub Release, and publishes
both npm packages to GitHub Packages.

---

## License

MIT © yaroher
