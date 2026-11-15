# WsProto for Swift

Native asynchronous protobuf RPC over a multiplexed WebSocket. Requires iOS 15+
or macOS 12+ and Swift 6.1+.

## Install

In Xcode, add `https://github.com/gopherex/ws-proto.git` with exact version
`1.10.0` and select the `WsProto` product. Add
`https://github.com/apple/swift-protobuf.git` at `1.39.0` and its
`SwiftProtobuf` product to the target that compiles your generated messages.

For a standalone Swift package, use this `Package.swift`:

```swift
// swift-tools-version: 6.1
import PackageDescription

let package = Package(
    name: "EchoClient",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [.library(name: "EchoClient", targets: ["EchoClient"])],
    dependencies: [
        .package(url: "https://github.com/gopherex/ws-proto.git", exact: "1.10.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", exact: "1.39.0"),
    ],
    targets: [
        .target(name: "EchoClient", dependencies: [
            .product(name: "WsProto", package: "ws-proto"),
            .product(name: "SwiftProtobuf", package: "swift-protobuf"),
        ]),
    ],
    swiftLanguageModes: [.v6]
)
```

## Generate and call

Install `protoc` first; protoc 29.3 and SwiftProtobuf 1.39.0 generate the checked-in
Swift fixtures. Building the RPC generator requires Go 1.25+. Build Apple's
message generator with the same SwiftProtobuf version as the runtime:

```sh
go install github.com/gopherex/ws-proto/cmd/protoc-gen-ws-swift@v1.10.0
mkdir -p "$HOME/.cache/ws-proto"
git clone --depth 1 --branch 1.39.0 https://github.com/apple/swift-protobuf.git \
    "$HOME/.cache/ws-proto/swift-protobuf-1.39.0"
swift build --package-path "$HOME/.cache/ws-proto/swift-protobuf-1.39.0" \
    -c release --product protoc-gen-swift
export PATH="$HOME/.cache/ws-proto/swift-protobuf-1.39.0/.build/release:$(go env GOPATH)/bin:$PATH"
```

Run the following in the package directory containing the manifest above.
The downloaded Echo schema supplies all four RPC shapes; replace it with your
own schema for your application.

```sh
mkdir -p proto/echo/v1 Sources/EchoClient
curl -fsSL https://raw.githubusercontent.com/gopherex/ws-proto/v1.10.0/example/proto/echo/v1/echo.proto \
    -o proto/echo/v1/echo.proto
protoc -I proto \
    --swift_out=Visibility=Public:Sources/EchoClient \
    --ws-swift_out=Sources/EchoClient \
    echo/v1/echo.proto
swift build
```

`Visibility=Public` makes message types usable by the generated public client.
Both generated files belong in the target that depends on `WsProto` and
`SwiftProtobuf`.

```swift
import Foundation
import SwiftProtobuf
import WsProto

let transport = WsTransport(url: URL(string: "wss://example.com/rpc")!)
let client = Echo_V1_EchoServiceClient(transport: transport) // generated from your .proto
let response = try await client.unary(
    Echo_V1_UnaryRequest.with { $0.name = "Swift" },
    options: CallOptions(headers: ["authorization": "Bearer ..."], timeoutMs: 5_000)
)
// response.message, response.headers, response.trailers
await transport.close()
```

Generate protobuf messages with Apple's `protoc-gen-swift`, and RPC clients with
this repository's `protoc-gen-ws-swift`. The client exposes unary, server-streaming,
client-streaming, and bidirectional RPCs. Request streams accept an
`AsyncSequence & Sendable` of protobuf messages. Response streams support
`for try await`, `headers()`, `trailers()`, and explicit `cancel()`.
A response iterator cancels its RPC when
released before completion; retain neither an unused iterator nor an abandoned
response. Call `cancel()` explicitly if response ownership outlives iteration.

The transport starts on the first call, state subscription, or explicit
`await transport.connect()`. Connection state is available through `state` and
`states()`. `TransportOptions` configures reconnect, full-jitter exponential
backoff, stable-connection reset, dial timeout, initial paused state, upgrade
headers, frame size, and send-buffer limits. Reconnect is disabled by default.
`pause()` suspends future dials, preserving an already-open connection;
`resume()` and `retryNow()` control reconnect. A reconnect always fails existing
calls: it does not replay requests or resume streams. Applications decide which
operations are safe to retry. Background execution remains subject to iOS rules;
the library does not keep an application alive in the background.

Task cancellation sends RST and fails the call with status 1. A positive
`CallOptions.timeoutMs` sends `ws-timeout-ms` and enforces a local deadline with
status 4. `WsStatusError` retains the numeric gRPC-style code, message, and raw
protobuf Any detail bytes. Headers and trailers settle on every terminal path,
including errors; `onHeader`/`onTrailer` callbacks observe these independently of
response consumption. Unary calls check the terminal status after their message.

Per-stream flow-control credit defaults to 256 KiB and returns on consumption.
A single oversized message is accepted; subsequent messages wait for credit.
HALF_CLOSE follows every queued request message. Receive queues are bounded by
bytes; the default unsent per-stream backlog limit is 16 MiB and the pre-connect
frame buffer limit is 4 MiB. Both peers must use the same initial window.

`ClientInterceptor` wraps an `RPCHandler` with an `RPCRequest` containing the
method, kind, options and `MessageSequence`. It can modify metadata, transform
protobuf messages with `mapMessages`, wrap response messages, inspect metadata,
or supply an `RPCResponse` without calling the next handler. The first registered
interceptor is outermost. Keep message iteration serial, as for the underlying
RPC stream. Custom sockets implement `WsSocket` and can be injected into the
transport for deterministic tests.

Run `swift test` for transport tests. Run
`scripts/with-mobile-server.sh swift test` from the repository root to also test
all four RPC shapes, metadata, status details, deadlines, concurrent large-message
streams and early server termination against the real Go server. Native Apple
networking is tested on macOS in CI. Swift 6.1's Linux FoundationNetworking
delivers individual libcurl receive chunks as complete WebSocket messages
([upstream implementation](https://github.com/swiftlang/swift-corelibs-foundation/blob/swift-6.1.2-RELEASE/Sources/FoundationNetworking/URLSession/WebSocket/WebSocketURLProtocol.swift#L127-L172)).
On Linux, interoperability tests inject a test-only Node `ws` socket relay to
preserve message boundaries; install the repository's npm dependencies with
`npm ci` first. The actual Swift mux, codecs, flow control and lifecycle run
against the real Go server, including 400 KiB messages and concurrent streams.
A separate small unary smoke test also exercises Linux URLSession. On macOS the
entire suite uses the shipping URLSession socket, without the relay. Linux is
not a supported production target for this Apple client; passing Linux protocol
tests does not establish Apple networking or iOS runtime validation.

The generated `transport.pb.swift` comes from `transport/transport.proto` and
`echo.pb.swift` in tests comes from `example/proto/echo/v1/echo.proto`. Regenerate
with SwiftProtobuf 1.39.0 `protoc-gen-swift`, using `Visibility=Public` for
the transport target. Do not edit generated files.
