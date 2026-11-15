# Native client generators

`protoc-gen-ws-swift` and `protoc-gen-ws-kotlin` generate clients for every RPC
shape: unary, server streaming, client streaming and bidirectional streaming.
They use the existing ws-proto wire protocol and delegate connection lifecycle,
flow control, deadlines, cancellation and interceptors to their native runtime.
They generate no message serializers and require no `go_package` option.

Install the plugins from this repository:

```sh
go install ./cmd/protoc-gen-ws-swift ./cmd/protoc-gen-ws-kotlin
```

With the official message generators installed, generate Swift messages and
clients together (`generated/` must already exist):

```sh
protoc -I example \
  --swift_out=Visibility=Public:generated \
  --ws-swift_out=generated \
  proto/echo/v1/echo.proto
```

The client is `Echo_V1_EchoServiceClient`, initialized with a `WsTransport`.
Unary and client-streaming methods return `UnaryResponse<Message>`; server and
bidirectional streaming return `ResponseStream<Message>`. These results expose
metadata in addition to the response messages. Request streams are Swift
`AsyncSequence & Sendable` values. Clients are `Sendable` and can be shared
between tasks. Every method accepts `CallOptions`.

The Swift generator supports `option swift_prefix`, nested messages, and the
SwiftProtobuf naming of reserved message names. By default application messages
and clients belong to the same Swift module. To reference messages in a separate
module, pass `--ws-swift_opt=Mpath/to/messages.proto=MessagesModule` for each
mapped proto file. Well-known protobuf types use the `SwiftProtobuf` module
automatically. `--ws-swift_opt=Visibility=Internal` changes client visibility;
the default is `Public`. Match visibility with the official message generator.

For Kotlin Multiplatform, generate immutable message models with the Square Wire
5.4.0 Gradle plugin (`kotlin {}` output), then generate RPC clients into a
Kotlin source directory:

```sh
protoc -I example \
  --ws-kotlin_out=generated \
  proto/echo/v1/echo.proto
```

The client is `echo.v1.EchoServiceClient`, initialized with a `WsTransport`.
Unary and client-streaming methods are suspend functions returning the response
message. Server and bidirectional streaming return cold `Flow<Message>` values;
each collection starts its own call. Client and bidirectional request streams
are `Flow<Message>` values. `CallOptions` carries metadata, deadlines and response
header/trailer callbacks; coroutine cancellation cancels the call. Models use
Wire `ADAPTER` codecs on every platform. Packages follow `wire.wire_package`,
then `java_package`, then the proto package; nested messages stay nested.
`java_multiple_files` and `java_outer_classname` have no effect. References escape
Kotlin keywords, including message names such as `class`.

Wire well-known types use their built-in representations and adapters: `Empty`
is `Unit`, timestamp/duration use Wire time types, and wrappers/JSON values can
be nullable. Keep Wire model generation at its default immutable setting;
`mutableTypes` and custom type remapping are not supported by this RPC plugin.
See [the Kotlin runtime setup](../packages/ws-proto-kotlin/README.md) for Gradle
source generation and platform dependencies.

RPC method names lowercase the first character and otherwise preserve spelling.
Swift constructor names `init`, `deinit` and `self` receive the suffix `RPC` on
both platforms. Ambiguous client or method names fail generation instead of
silently emitting conflicting declarations. Unknown plugin options are rejected.
Both generators support proto2 and proto3, including proto3 optional fields;
protobuf Editions are rejected explicitly.

## Tests and fixtures

```sh
go test -race ./mobilegen ./cmd/protoc-gen-ws-swift ./cmd/protoc-gen-ws-kotlin
go test ./mobilegen -run '^$' -bench . -benchmem
go run ./mobilegen/testdata/generate.go -language swift -out /tmp/ws-swift-fixture
go run ./mobilegen/testdata/generate.go -language kotlin -out /tmp/ws-kotlin-fixture
```

The fixture helper uses the checked-in Echo Go descriptor, so generating RPC
clients does not require protoc. `-descriptor-out /tmp/echo.bin` also exports a
descriptor set for the official message generators:

```sh
protoc --descriptor_set_in=/tmp/echo.bin \
  --swift_out=Visibility=Public:generated proto/echo/v1/echo.proto
```

Go tests exercise the plugin protocol, all RPC shapes, imported and nested
messages, naming options, reserved identifiers and invalid input. Native runtime
tests compile and exercise the generated Echo clients against the Go server.
The naming fixtures in `testdata/naming` are also compiled in the native test
targets. With the Swift runtime already built and `protoc`, `protoc-gen-swift`,
`swift` and `swiftc` on PATH, `sh mobilegen/check-swift-names.sh` regenerates and
typechecks those naming fixtures in Swift 6 mode. Those tool paths can also be
provided with `PROTOC`, `PROTOC_GEN_SWIFT`, `SWIFT` and `SWIFTC`; set
`SWIFT_MODULE_PATH` to override the built module directory.
