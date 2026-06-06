# ws-proto — WebSocket Transport for Protobuf Services

**Status:** Design approved 2026-06-06
**Module:** `github.com/gopherex/ws-proto`

## Goal

A WebSocket-based transport layer for protobuf RPC services, with two code
generators and two runtimes. Layout heavily inspired by
`protoc-gen-go-ogen` (minimal plugin entrypoints, single generator package,
`GeneratedFile.P()` code emission, separate runtime packages, committed golden
example output).

The system is the WS analog of gRPC / Connect:

- `protoc-gen-go-ws` — Go generator, analog of `protoc-gen-go-grpc` /
  `protoc-gen-connect-go`.
- `protoc-gen-ws-es` — TypeScript generator, analog of
  `protoc-gen-connect-es`, interoperating with `protoc-gen-es` output.
- A shared wire contract (`transport.proto`) marshaled by both runtimes.

## Foundational decisions

| Decision | Choice |
|---|---|
| Multiplexing | One WS connection, `stream_id`-multiplexed envelope, many interleaved concurrent streams (HTTP/2-over-WS style). |
| ES compatibility | `protoc-gen-es` **v2** (`@bufbuild/protobuf` v2, ESM, schema-based `create`/`toBinary`/`fromBinary`). |
| Streaming kinds | All four: unary, server-stream, client-stream, bidi. |
| Go service binding | Standalone Connect-style Handler interfaces + Client, **plus** a gRPC bridge adapter (ogen-style) wrapping existing grpc `ServiceServer` impls. |
| ES generator language | TypeScript plugin via `@bufbuild/protoplugin` (true analog of protoc-gen-connect-es, interops with protoc-gen-es). |
| Go WS library | `github.com/coder/websocket` (context-aware, modern). |
| TS WS client | Browser-native `WebSocket`. |

## Architecture — four units + shared contract

| Unit | Lang | Analog | Provides |
|---|---|---|---|
| `protoc-gen-go-ws` | Go (protogen) | protoc-gen-go-grpc / connect-go | `*_ws.pb.go`: Handler ifaces, Client, typed stream wrappers, gRPC bridge adapter |
| `protoc-gen-ws-es` | TS (`@bufbuild/protoplugin`) | protoc-gen-connect-es | `*_ws_pb.ts`: service descriptors, typed client |
| Go runtime `wsrpc/` | Go | grpc-go / connect runtime | mux, frame codec, HTTP-upgrade server, dial client |
| TS runtime `@gopherex/ws-transport` | TS | connect-web transport | mux, frame codec, browser client |
| `transport.proto` | shared proto | — | `Frame` envelope, marshaled by both runtimes |

## Wire protocol — `transport.proto`

One WS connection per peer link. Streams are client-initiated; one stream per
RPC call. `Frame` is a protobuf message; each runtime marshals it with its own
protobuf library and sends it as a binary WS frame.

```proto
message Frame {
  uint32 stream_id = 1;            // client-allocated, monotonic per connection
  Kind   kind      = 2;
  map<string,string> headers = 3;  // OPEN: metadata + deadline; END: trailers
  bytes  payload   = 4;            // MSG: one marshaled request/response message
  Status status    = 5;            // END only: code + message + details
  string method    = 6;            // OPEN only: "/pkg.Service/Method"
}

enum Kind { OPEN = 0; MSG = 1; HALF_CLOSE = 2; END = 3; RST = 4; }

message Status { int32 code = 1; string message = 2; repeated bytes details = 3; }
```

### Frame kinds and flow

- `OPEN` — opens a stream. Carries `method` and request `headers`
  (metadata + deadline). Client only.
- `MSG` — one marshaled message. Request messages (client→server) or response
  messages (server→client).
- `HALF_CLOSE` — sender finished sending request messages (relevant for
  client-stream and bidi). Client only.
- `END` — server closes the stream with `status` and trailer `headers`.
  Server only.
- `RST` — abort/cancel the stream from either side (client cancellation,
  server abort).

Per-RPC sequence:
`OPEN(method, headers) → MSG*(req) → HALF_CLOSE ⇄ MSG*(res) → END(status, trailers)`.

Stream-kind specialization:
- **unary**: `OPEN`, one req `MSG`, `HALF_CLOSE`; server one res `MSG`, `END`.
- **server-stream**: `OPEN`, one req `MSG`, `HALF_CLOSE`; server N res `MSG`, `END`.
- **client-stream**: `OPEN`, N req `MSG`, `HALF_CLOSE`; server one res `MSG`, `END`.
- **bidi**: `OPEN`, interleaved req `MSG`/`HALF_CLOSE` ⇄ res `MSG`, `END`.

### Cross-cutting semantics

- **Deadline**: encoded as a header on `OPEN`; both sides enforce locally.
- **Cancellation**: `RST` frame; client cancel or server abort.
- **Error status**: `END` with non-OK `Status` (gRPC-style code/message/details).
- **Backpressure (v1)**: bounded buffered channels + native WS backpressure.
  Credit-based per-stream flow control is a documented future extension, not v1.
- **Stream-id space**: client-allocated, monotonic per connection; server never
  initiates streams.

## Go side

### Generator `protoc-gen-go-ws`

Mirrors ogen conventions:
- Minimal `cmd/protoc-gen-go-ws/main.go` → `protogen.Options{}.Run(...)`,
  delegating to the `generator/` package.
- Pure `GeneratedFile.P()` line-by-line emission. No `text/template`.
- `SupportedFeatures = FEATURE_PROTO3_OPTIONAL`.

Per service, emits into `*_ws.pb.go`:
- `XxxServiceHandler` interface — Connect-style method signatures:
  - unary: `GetUser(context.Context, *Req) (*Res, error)`
  - server-stream: `Watch(context.Context, *Req, ServerStream[Res]) error`
  - client-stream: `Upload(context.Context, ClientStream[Req]) (*Res, error)`
  - bidi: `Chat(context.Context, BidiStream[Req, Res]) error`
- `NewXxxServiceHandler(impl XxxServiceHandler) http.Handler` — registers the
  service on the WS dispatch router.
- `XxxServiceClient` interface + `NewXxxServiceClient(conn *wsrpc.ClientConn)`.
- Typed stream wrappers generated/aliased: `ServerStream[T]`, `ClientStream[T]`,
  `BidiStream[Req, Res]`.
- `XxxServiceGRPCAdapter(srv XxxServer) XxxServiceHandler` — bridge adapter that
  wraps an existing `protoc-gen-go-grpc` `XxxServer` implementation so it can be
  served over WS unchanged.

### Runtime `wsrpc/`

- `wsrpc/wsconn` — connection multiplexer: frame codec (marshal/unmarshal
  `Frame`), per-stream state machine, stream registry keyed by `stream_id`,
  read/write pumps.
- `wsrpc/server.go` — `http.Handler` that upgrades the request, runs the mux,
  dispatches `OPEN` frames to registered service handlers by `method`.
- `wsrpc/client.go` — `Dial` to establish a `ClientConn`; per-call stream
  allocation and lifecycle.
- `wsrpc/stream.go` — `ServerStream`/`ClientStream`/`BidiStream` implementations
  over the mux.
- `wsrpc/status.go` — status code/message/details mapping (gRPC-style codes).
- WS library: `github.com/coder/websocket`.

## ES side

### Generator `protoc-gen-ws-es`

- TypeScript plugin built on `@bufbuild/protoplugin`, run via node — true
  analog of `protoc-gen-connect-es`.
- Imports generated types from `protoc-gen-es` v2 output (`*_pb.ts`): schema
  objects plus `create`, `toBinary`, `fromBinary` from `@bufbuild/protobuf`.
- Emits `*_ws_pb.ts`: per-service descriptors and a typed client; all four
  stream kinds exposed via async iterables.
- Lives at `packages/protoc-gen-ws-es/`.

### Runtime `@gopherex/ws-transport`

- Browser-native `WebSocket`.
- Mux + frame codec implemented against the generated `FrameSchema` using
  `@bufbuild/protobuf`.
- Typed client; streaming via async iterables (`for await`).
- Lives at `packages/ws-transport/`.

## Options proto (minimal, optional)

A small `options.proto` alongside `transport.proto`:
- `(ws.service)` — path prefix override.
- `(ws.method)` — route override, max message size, compression hint.

Default behavior requires **zero** options — route derives from
`/pkg.Service/Method`. Ship minimal; extend later (YAGNI).

## Repo layout (ogen-heavy)

```
ws-proto/
  cmd/protoc-gen-go-ws/main.go    # Go generator entrypoint
  generator/                      # Go gen logic, pure P()
    generator.go settings.go service.go client.go adapter.go stream.go
  transport/                      # transport.proto + options.proto + *.pb.go
  wsrpc/                          # Go runtime
    wsconn/  server.go  client.go  stream.go  status.go
  packages/
    protoc-gen-ws-es/             # TS plugin: src/ package.json tsconfig.json
    ws-transport/                 # TS runtime: src/ package.json tsconfig.json
  example/                        # golden.proto + committed gen/ output
  Makefile  buf.gen.yaml  go.mod  package.json
```

## Build pipeline

- `Makefile`: build `protoc-gen-go-ws`; regenerate `transport/*.pb.go`; run the
  golden codegen with all plugins (`protoc-gen-go`, `protoc-gen-go-ws`,
  `protoc-gen-es`, `protoc-gen-ws-es`); `gofmt`, `go vet`, `go test`.
- `buf.gen.yaml`: orchestrates the multi-plugin generation for `example/`.
- TS workspace (`package.json`) builds `protoc-gen-ws-es` and `ws-transport`.

## Testing

Golden `example/` committed to the repo (ogen-style), exercising all four stream
kinds plus cancel / deadline / error-status.

- **Go**: round-trip client↔server over an in-memory WS pipe — unary,
  server-stream, client-stream, bidi; cancellation (`RST`), deadline expiry,
  non-OK `END` status; the gRPC bridge adapter path.
- **TS**: vitest against shared `Frame` fixtures; one integration test against a
  running Go WS server.

## Dependencies

- Go: `google.golang.org/protobuf` (protogen + runtime),
  `github.com/coder/websocket`, `google.golang.org/grpc` (for bridge adapter
  types only).
- TS: `@bufbuild/protobuf` v2, `@bufbuild/protoplugin`, `vitest` (dev).

## Out of scope (v1)

- Credit-based per-stream flow control (documented future extension).
- Server-initiated streams / server push beyond RPC responses.
- Reconnect/resume of in-flight streams across connection loss.
- Compression (option hint reserved, not implemented).
- Auth/TLS specifics — delegated to the HTTP/WS layer.
```
