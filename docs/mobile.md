# Mobile clients

Swift and Kotlin implement the `wsrpc.v1` client protocol in the same repository
as Go and TypeScript. Applications keep their `.proto` service definitions;
the standard protobuf generators produce messages and the ws-proto generators
produce clients that use the corresponding runtime. The server does not need a
mobile-specific endpoint.

## Protocol contract

Each connection negotiates `wsrpc.v1` and carries protobuf `Frame` messages from
[`transport.proto`](../transport/transport.proto). Client stream IDs are odd and
monotonically allocated per connection. Unary, server-streaming,
client-streaming and bidirectional calls share that connection.

Request metadata travels in `OPEN`; leading response metadata travels in
`HEADER`; trailers and terminal status travel in `END`. Status details retain
the serialized `google.protobuf.Any` bytes. A unary response is not successful
until its terminal status arrives. An empty or multi-message unary response is
an error.

Each stream starts with 256 KiB of send credit. Credit returns when the caller
consumes a message, not when the network receives it. A message larger than the
window can be sent once when credit is positive; following messages wait for
credit. Receive and send backlogs are bounded by bytes. `HALF_CLOSE` is queued
after all accepted request messages. Closing twice sends one frame; ending or
cancelling the call discards messages waiting for send credit and pending half-close.

Cancellation resets the individual RPC. A deadline sets `ws-timeout-ms` on
`OPEN` and arms a local timeout. Go handlers returning context cancellation or
deadline errors, including wrapped errors, produce `Canceled` or
`DeadlineExceeded`; explicit ws-proto and gRPC statuses take precedence.
Every terminal path settles response metadata
and wakes blocked readers. Finishing a streaming response does not wait for an
idle request producer. Breaking response iteration cancels the call.

Reconnect is opt-in and never replays RPCs. Connection loss fails active calls;
applications decide which operations can be retried. New calls fail during the
backoff gap and can buffer during a new handshake. Subprotocol mismatch,
connection-ending status and configured frame-size violations stop automatic
redial. Pause suspends dialing while keeping an already-open socket; resume
re-enables dialing and retry-now bypasses a scheduled backoff. Mobile applications
own their foreground/background and network-reachability integration.

## Distribution

One `vX.Y.Z` tag identifies all implementations. The release workflow waits for
Go, TS and mobile CI checks before publishing artifacts.

* Swift: add `https://github.com/gopherex/ws-proto.git` in Xcode or SwiftPM and
  select exact version `1.10.0`. The root `Package.swift` exports `WsProto`.
* Kotlin: add `https://maven.pkg.github.com/gopherex/ws-proto` as a Maven
  repository and depend on `io.gopherex:ws-proto-kotlin:1.10.0`. GitHub requires
  package-read credentials even for public Maven packages. Credentials belong
  in local Gradle properties or CI secrets, never in application source.
* Generators: install `protoc-gen-ws-swift` and `protoc-gen-ws-kotlin` at
  `v1.10.0`. GitHub Releases contains native executables for Linux, macOS and
  Windows on amd64 and arm64.

```sh
go install github.com/gopherex/ws-proto/cmd/protoc-gen-ws-swift@v1.10.0
go install github.com/gopherex/ws-proto/cmd/protoc-gen-ws-kotlin@v1.10.0
export PATH="$(go env GOPATH)/bin:$PATH"
```

These commands require Go 1.25+. The
[Swift guide](../packages/ws-proto-swift/README.md#generate-and-call) supplies
`protoc-gen-swift` installation and `Visibility=Public` generation commands;
the [Kotlin guide](../packages/ws-proto-kotlin/README.md#generate-and-call)
uses protoc 36.2 and Java-lite messages. Both guides include the exact runtime
dependency and a working Echo schema download.

The repository publishes its own artifacts only to GitHub. Builds still resolve
third-party dependencies through their normal repositories, including Maven
Central and Google's Android repository.

## Verification

`make test-swift` and `make test-kotlin` build a real Go server, allocate a free
loopback port, export `WS_PROTO_TEST_URL`, run the client tests and stop the
server. Running the native test commands directly exercises unit tests; the
real-server cases require that environment variable.

On Apple platforms these tests use the shipping URLSession socket. Linux test
runs use a Node `ws` relay to preserve WebSocket message boundaries because
Swift 6.1 FoundationNetworking exposes libcurl chunks as complete messages;
run `npm ci` before the Linux suite. The relay is test-only and does not ship in
`WsProto`. A separate small native URLSession test checks its handshake and unary
call. Linux protocol tests do not establish Apple networking compatibility.

The fixture uses the existing EchoService schema. All methods return
`x-header=ready`, echo request `x-test` into response `x-echo`, and return
`x-trailer=done`. Unary `!error` produces InvalidArgument with an Any-wrapped
StringValue detail; `!wait` waits for cancellation; `!empty` returns no message;
`!conn-end` terminates the connection with PermissionDenied. Bidi `!end` and
`!error` terminate before the request source finishes. These exercise lifecycle
behavior against the Go runtime, including flows larger than the credit window.

CI also builds the Swift package for iOS Simulator. The Android consumer in
`tests/android-consumer` compiles the public API and dexes its dependencies into
an APK (`make test-android`, with Android SDK and JDK 17 installed); this
complements JVM protocol tests but does not replace device testing.
