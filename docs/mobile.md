# Mobile clients

Swift and Kotlin implement the `wsrpc.v1` client protocol in the same repository
as Go and TypeScript. Applications keep their `.proto` service definitions;
the standard protobuf generators produce messages and the ws-proto generators
produce clients that use the corresponding runtime. The server does not need a
mobile-specific endpoint.

## Kotlin Multiplatform

The Kotlin client implements the protocol once in `commonMain`. Wire generates
protobuf messages from the existing schemas; generated RPC clients use their
`ProtoAdapter` instances. Ktor supplies WebSockets through OkHttp on JVM/Android
and Darwin on iOS. The shared runtime owns stream ordering, credit, bounded
queues, deadlines, cancellation and reconnect. Platform engines own socket I/O.

An application can put its business rules and state in a KMP module, consume
that module from Android Compose, and export it as a framework for SwiftUI.
Apply SKIE to the application's framework module to expose `Flow` as Swift
`AsyncSequence`. Neither SKIE nor a UI toolkit is a library dependency. Expose
application operations and state to Swift; the application controls how much
of its protobuf and transport API crosses that boundary.

SKIE does not turn arbitrary Kotlin Flow exceptions into Swift errors. The
application facade maps expected failures to typed state/events before exporting
a flow, and rethrows `CancellationException` so Swift task cancellation still
resets the RPC. The consumer fixture demonstrates this boundary. Suspending
operations declare `@Throws` for errors that Swift must receive.

The Kotlin API uses Wire models rather than the Java-lite models used by
version 1.10.0. Regenerate messages with Wire and RPC clients with the matching
`protoc-gen-ws-kotlin`; Java builders and parsers are not part of this API.
The wire protocol and the Go, TypeScript and standalone Swift clients do not
require migration.

Wire cannot generate valid Kotlin copy constructors for message types named
with Kotlin hard keywords such as `class` or `when`. The RPC generator rejects
these names with a schema diagnostic. Use ordinary message type names; keyword
packages, fields and RPC methods have separate escaping rules. Generated files
are never patched to conceal compiler errors.

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

Cancellation resets the individual RPC. A received reset cancels the Go
handler's context immediately, even if the handler is not reading messages.
A deadline sets `ws-timeout-ms` on
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
uses Wire messages and the Kotlin RPC generator. Both guides include the exact runtime
dependency and a working Echo schema download.

The repository publishes its own artifacts only to GitHub. Builds still resolve
third-party dependencies through their normal repositories, including Maven
Central and Google's Android repository.

## Verification

`make test-swift` and `make test-kotlin` build a real Go server, allocate a free
loopback port, export `WS_PROTO_TEST_URL`, run the client tests and stop the
server. Running the native test commands directly exercises unit tests; the
real-server cases require that environment variable.

The standalone Swift client tests on Apple platforms use the shipping URLSession socket. Linux test
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

The KMP suite runs shared tests against Go on JVM and iOS Simulator. The
`tests/kmp-consumer` application module exports a SKIE framework and Swift tests
exercise it on the simulator. Cancellation tests start an RPC without a deadline,
cancel its Swift task, and query the Go fixture's `/probe?id=...` endpoint to
confirm the handler received cancellation. The `x-probe-id` header identifies
each test call; probes exist only in the test server.

Run `make test-kotlin-ios` and `make test-kmp-swift` on an Apple Silicon Mac.
Release checks resolve the published KMP dependency from GitHub Packages and
build the Swift framework and Android consumer without a local dependency
substitution. A successful local JVM build alone does not verify iOS behavior.
