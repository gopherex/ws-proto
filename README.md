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
| `@gopherex/ws-transport` | TypeScript | connect-web transport | mux, framing, browser/node client |

The ES generator interoperates with [`protoc-gen-es`](https://github.com/bufbuild/protobuf-es)
v2 output (it imports the generated `*_pb.ts` message types and schemas).

---

## Wire model

- **One** WebSocket connection per peer link, carrying **binary** frames.
- Each binary message is a marshaled `Frame` envelope:

  ```proto
  message Frame {
    uint32 stream_id = 1;             // client-allocated, monotonic odd ids
    Kind   kind      = 2;             // OPEN | MSG | HALF_CLOSE | END | RST
    map<string,string> headers = 3;   // OPEN: metadata + deadline; END: trailers
    bytes  payload   = 4;             // MSG: one marshaled request/response message
    Status status    = 5;             // END only: gRPC-style code/message/details
    string method    = 6;             // OPEN only: "/pkg.Service/Method"
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
npm install @gopherex/ws-transport
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
Optional `(wsproto.transport.v1.service)` / `(...method)` options
(`transport/options.proto`) allow a route prefix or per-method overrides.

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
    // Restrict browser origins (CSRF). Use your real frontend origins;
    // "*" disables the check — only safe if auth is enforced on the Upgrade request.
    wsrpc.WithOriginPatterns("app.example.com"),
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

### TypeScript client (browser or node)

```ts
import { WsTransport } from "@gopherex/ws-transport";
import { EchoServiceClient } from "./gen/echo_ws_pb.js";

const client = new EchoServiceClient(new WsTransport("wss://api.example.com/rpc"));

// unary
const res = await client.unary({ name: "bob" });

// server streaming
for await (const msg of client.serverStream({ count: 3 })) {
  console.log(msg.index);
}
```

In Node, provide a `WebSocket` global (e.g. the [`ws`](https://www.npmjs.com/package/ws)
package) before constructing the transport.

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
  is occasionally mangled by intermediaries and costs memory). This is intentional.
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
- Set `wsrpc.WithOriginPatterns(...)` to your real frontend origins. The default
  Origin check rejects cross-origin browser handshakes (correct CSRF behavior);
  `"*"` disables it and is only safe when auth is enforced on the Upgrade request.
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
