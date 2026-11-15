# ws-proto Kotlin Multiplatform

A shared `commonMain` client for `wsrpc.v1`. Wire handles protobuf, Ktor handles
WebSockets, and Kotlin coroutines expose suspend calls and `Flow`. The same
multiplexer, deadlines, cancellation, flow control, interceptors and reconnect
controller run on JVM/Android and Kotlin/Native iOS. JVM uses Ktor's OkHttp
engine; iOS uses Darwin. No JNI or embedded Go runtime is required.

Published targets are JVM (JDK 17 bytecode), `iosArm64`, `iosSimulatorArm64` and
`iosX64`. Android applications consume the JVM variant and target API 26+.

## Install

Releases publish `io.gopherex:ws-proto-kotlin` to GitHub Packages. GitHub requires
authentication even for public Maven packages. Configure repository credentials
on the developer machine or CI; they are not application runtime credentials.

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven {
            url = uri("https://maven.pkg.github.com/gopherex/ws-proto")
            credentials {
                username = providers.environmentVariable("GITHUB_ACTOR").orNull
                password = providers.environmentVariable("GITHUB_TOKEN").orNull
            }
        }
    }
}
```

```kotlin
// Existing KMP shared module's build.gradle.kts
kotlin {
    sourceSets.commonMain.dependencies {
        implementation("io.gopherex:ws-proto-kotlin:1.11.0")
    }
}
```

In a JVM/Android-only module, use the same coordinate in its normal
`dependencies { implementation(...) }` block. Gradle selects the JVM variant.

Use a personal access token (classic) with `read:packages` for local downloads,
or an authorized `GITHUB_TOKEN` in Actions. Set `GITHUB_ACTOR` to your GitHub
username and `GITHUB_TOKEN` to that token in your shell or CI secrets before
running Gradle. Keep both credentials out of the repository and application.

Android applications also need this permission in `AndroidManifest.xml`:

```xml
<uses-permission android:name="android.permission.INTERNET" />
```

Use `wss://` in production. Configure the application's Java/Kotlin compilation
target to JVM 17; the library's Android consumer uses minSdk 26.

## Generate and call

Use Kotlin 2.2.21, Wire 5.4.0 and protoc 36.2. Wire generates the shared
protobuf models; `protoc-gen-ws-kotlin` generates RPC clients using their adapters.
Apply Wire to your Kotlin Multiplatform module:

```kotlin
plugins {
    kotlin("multiplatform") version "2.2.21"
    id("com.squareup.wire") version "5.4.0"
}
wire {
    sourcePath { srcDir("src/commonMain/proto") }
    kotlin { rpcRole = "none" }
}
```

Place your service schemas in `src/commonMain/proto`. This example downloads
the Echo schema used by the call example below; replace it with your own schema:

```sh
go install github.com/gopherex/ws-proto/cmd/protoc-gen-ws-kotlin@v1.11.0
export PATH="$(go env GOPATH)/bin:$PATH"
mkdir -p src/commonMain/kotlin src/commonMain/proto/echo/v1
curl -fsSL https://raw.githubusercontent.com/gopherex/ws-proto/v1.11.0/example/proto/echo/v1/echo.proto \
    -o src/commonMain/proto/echo/v1/echo.proto
protoc -I src/commonMain/proto \
    --ws-kotlin_out=src/commonMain/kotlin \
    echo/v1/echo.proto
```

Use the generator version matching the runtime release. Wire generates models
when Gradle compiles the module. RPC clients honor `wire.wire_package`, then
`java_package`, then the proto package; Java outer-class and multiple-file options
do not affect Wire's Kotlin names. Protobuf message names that are Kotlin hard
keywords are rejected because Wire emits invalid copy constructors for them.
Keyword package segments, field names and RPC methods remain supported.

For Swift use of a KMP framework, apply SKIE 0.10.7 in the consuming framework
module to bridge suspend functions and Flow. The library publishes Kotlin klibs;
[tests/kmp-consumer](../../tests/kmp-consumer) demonstrates an application framework with a Swift-facing
facade. The separate `packages/ws-proto-swift` client remains available for
applications that do not use Kotlin.

```kotlin
import io.gopherex.wsproto.CallOptions
import io.gopherex.wsproto.WsTransport
import io.gopherex.wsproto.WsTransportOptions
import echo.v1.UnaryRequest
import echo.v1.BidiRequest
import echo.v1.EchoServiceClient
import kotlinx.coroutines.flow.flowOf

val transport = WsTransport(
    "wss://api.example.com/rpc",
    WsTransportOptions(reconnect = true),
)
val client = EchoServiceClient(transport)

// In a coroutine:
val reply = client.unary(
    UnaryRequest(name = "Kotlin"),
    CallOptions(headers = mapOf("authorization" to "Bearer $token"), timeoutMs = 5_000),
)
client.bidi(flowOf(BidiRequest(text = "hello")))
    .collect { response -> consume(response.echo) }

// When the connection owner is disposed:
transport.close()
```

Generated unary/client-streaming calls suspend; generated server/bidi calls
return a cold `Flow` that opens one RPC per collection. Cancelling the collecting
coroutine or stopping collection early resets its RPC. `CallOptions.onHeader`
and `onTrailer` observe response metadata. Raw `transport.unary()` additionally
returns a `UnaryResponse` containing the payload and both metadata maps;
`transport.stream()` returns a single-consumer response flow plus deferred header
and trailer maps and an explicit `cancel()` method.

`WsStatusException` exposes the numeric gRPC status code, message, and serialized
`google.protobuf.Any` details. Per-call deadlines are enforced locally and sent
as `ws-timeout-ms` metadata. A terminal error is observed even if response
messages preceded it; queued response messages drain before the error surfaces.
The unary runtime rejects missing or extra messages. Client-streaming matches the
TypeScript helper: it drains through END and returns the last response.

Transport-wide `Interceptor` instances wrap typed unary and streaming calls.
The first interceptor is outermost; an interceptor may replace a request,
transform response messages, inspect metadata, or return its own response.

## Connection and buffer ownership

`state` is a `StateFlow<ConnectionState>`. Reconnection is opt-in and uses capped
exponential backoff with full jitter; `backoff.stableMs` delays resetting the
attempt count until the connection remains open. `dialTimeoutMs` bounds a
handshake. Failed RPCs are never replayed or resumed automatically.

`pause()` stops dialing and aborts a pending handshake, but preserves an open
socket. `resume()` starts dialing again with fresh backoff. `retryNow()` skips a
scheduled delay or retries a disconnected transport. `close()` is terminal and
cancels every active call. Protocol mismatch, frame-size overflow, pre-open
buffer overflow and `CONN_END` suppress automatic reconnect; `retryNow()` is an
explicit override for a disconnected transport.

The default per-stream credit window is 256 KiB. Credit returns as messages are
consumed, allowing a single message larger than the window. Receive backlog is
bounded by that window; unsent request backlog defaults to 16 MiB; pre-handshake
frames are capped at 4 MiB per connection. HALF_CLOSE follows all accepted
request messages, including messages awaiting credit. `maxFrameBytes` optionally
caps inbound frames before decoding. Both peers must use the same initial
window because the protocol does not negotiate it.

An injected `CoroutineScope` owns the transport's background work. An injected
`KtorSocketFactory(client)` uses a supplied `HttpClient` with Ktor's `WebSockets`
plugin installed. The client owns TLS, proxy and networking configuration.
Closing a transport closes its socket but not the supplied client. The default
factory shares one process-lifetime client per platform. The socket writer
bounds queued frames to 16 MiB by default and preserves message order. `WsSocketFactory` is also the test seam for deterministic
connection and framing tests. Mobile lifecycle integration is explicit: call
pause/resume from the application's lifecycle owner as appropriate.

## Build and verify

From the repository root:

```sh
./scripts/with-mobile-server.sh ./packages/ws-proto-kotlin/gradlew -p packages/ws-proto-kotlin jvmTest
./packages/ws-proto-kotlin/gradlew -p packages/ws-proto-kotlin publishToMavenLocal
```

On an Apple Silicon Mac, run the same common tests with the Darwin engine:

```sh
./scripts/with-ios-simulator.sh ./scripts/with-mobile-server.sh ./packages/ws-proto-kotlin/gradlew -p packages/ws-proto-kotlin iosSimulatorArm64Test
```

The build generates Wire messages from `transport/transport.proto`, and test
messages/clients from Echo plus naming, optional-presence and well-known-type
fixtures. All runtime tests live in `commonTest`. Unit tests exercise framing,
credit, terminal events, concurrency, cancellation, metadata, interceptors and
reconnect controls. Integration tests require `WS_PROTO_TEST_URL`; the launcher builds a real Go
server and supplies that variable. For unit-only JVM runs use
`jvmTest --tests '*TransportTest' --tests '*LifecycleTest' --tests '*TypedCallsTest' --tests '*ProtobufTest'`.
Simulator execution receives the endpoint through `SIMCTL_CHILD_WS_PROTO_TEST_URL`
and uses `WS_PROTO_IOS_DEVICE` when a simulator UDID is supplied. They cover all four
generated RPC methods, concurrent calls, large messages, flow-controlled input,
error details, deadlines and early server termination with an idle input source.

`-PreleaseVersion=X.Y.Z` selects the published version. Without it the build uses
the workspace version with a `-SNAPSHOT` suffix. GitHub Actions supplies
`GITHUB_ACTOR` and `GITHUB_TOKEN` for `publish`. Publish from macOS so all Apple
klibs and the JVM variant are available together. The root KMP coordinate carries
Gradle metadata selecting `ws-proto-kotlin-jvm` or the appropriate Apple artifact.
