# ws-proto

Rust runtime for the ws-proto WebSocket RPC transport (`wsrpc.v1`). Wire-compatible
with the Go `wsrpc` package and the TypeScript `@gopherex/ws-proto-transport`.

Message types are generated with [`protoc-gen-prost`](https://github.com/neoeinstein/protoc-gen-prost);
service bindings with `protoc-gen-ws-rs` from this repository.

## Status

| Layer | State |
|---|---|
| `conn::WsConn` + `tokio-tungstenite` adapter, in-memory `conn::pipe` | done |
| `Mux` / `Stream`: multiplexing, flow control, metadata, deadlines, `Status` | done, mirrors Go `wsrpc` |
| `Server` (origin policy, `wsrpc.v1` upgrade, registry, deadlines, panic → Internal) | done |
| `Client` (dial with subprotocol check, unary/streaming helpers) | done |
| typed bindings from `protoc-gen-ws-rs` | done |
| client auto-reconnect (`Reconnect`), keepalive pings (`Keepalive`) | done |
| client interceptors, server middleware, `ServerStats` | done |
| `Server::register_grpc` (feature `tonic`): any tonic/tower gRPC service served unchanged, status details 1:1 | done (Go `GRPCRegistrar`) |

Cross-language matrix (`make test-interop`): Rust client ↔ Go server, Go client ↔
Rust server, TS client ↔ Rust server — unary / server / client / bidi
streaming, metadata, unknown method, oversized payloads (flow control), 20k
small messages, deadlines, 50 concurrent streams.
