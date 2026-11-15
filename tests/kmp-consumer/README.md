# KMP framework consumed from Swift

This fixture keeps the RPC client in Kotlin and exports application operations
to Swift. `EchoFacade` returns a suspending greeting and a `Flow<WatchEvent>`;
SKIE exposes them as Swift async calls and a typed `AsyncSequence`. The Swift
target does not use the standalone WsProto Swift client.

The Gradle module uses Kotlin 2.2.21, Wire 5.4.0 and SKIE 0.10.7. Wire generates
messages, the ws-proto generator produces RPC clients, and the framework exports
the facade. An application can keep the same boundary while adding Android
Compose and iOS SwiftUI views around its shared state.

See [EchoFacade.kt](src/commonMain/kotlin/io/gopherex/fixture/EchoFacade.kt) and
[the Swift tests](swift/Tests/KmpConsumerTests/InteropTests.swift) for calls,
collection and cancellation. Flow failures become domain events before crossing
the Swift boundary because SKIE cannot propagate arbitrary Kotlin exceptions
from Flow. `CancellationException` is rethrown; cancellation must reach the RPC.

From the repository root on macOS with Xcode, an installed iOS Simulator runtime,
JDK 17 and Go 1.25:

```sh
make test-kmp-swift
```

The launcher starts the Go test server, builds device and host-simulator framework
slices, creates an XCFramework, and runs Swift XCTest on a temporary simulator.
It deletes only the simulator it created and stops its test server on exit.
Tests check unary calls, typed Flow collection, error events and cancellation of
both suspend calls and Flow collection. The cancellation checks query the Go
server to establish that its handler actually stopped without a deadline.

By default, Gradle uses a composite build of the runtime in this checkout. To
verify installation of a published version instead, supply GitHub package-read
credentials through the environment and run:

```sh
WS_PROTO_VERSION=1.11.0 make test-kmp-swift
```

Set `GITHUB_ACTOR` to your GitHub username and `GITHUB_TOKEN` to a token with
`read:packages`. Published mode disables the composite build and does not consult
Maven Local. It resolves the library's common metadata and native variants from
GitHub Packages. The application's framework is built locally; the library
publishes KMP artifacts, not an application-specific framework.
