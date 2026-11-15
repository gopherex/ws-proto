# ws-proto Kotlin

Android/JVM client for `wsrpc.v1`, using OkHttp, protobuf Java lite and Kotlin
coroutines. One WebSocket carries concurrent unary, server-streaming,
client-streaming and bidirectional RPCs. The module builds a regular JAR;
Android does not need JNI or an embedded Go runtime. JVM builds require JDK 17+;
the Android consumer targets API 26+.

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
// build.gradle.kts
dependencies {
    implementation("io.gopherex:ws-proto-kotlin:1.10.0")
}
```

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

Install protoc 36.2 (`protoc --version` prints `libprotoc 36.2`) and Go 1.25+.
The library depends on protobuf-javalite 4.36.2; use its matching compiler to
generate Java-lite messages. In your Android/JVM module directory:

```sh
go install github.com/gopherex/ws-proto/cmd/protoc-gen-ws-kotlin@v1.10.0
export PATH="$(go env GOPATH)/bin:$PATH"
mkdir -p src/main/proto/echo/v1 src/main/java src/main/kotlin
curl -fsSL https://raw.githubusercontent.com/gopherex/ws-proto/v1.10.0/example/proto/echo/v1/echo.proto \
    -o src/main/proto/echo/v1/echo.proto
protoc -I src/main/proto \
    --java_out=lite:src/main/java \
    --ws-kotlin_out=src/main/kotlin \
    echo/v1/echo.proto
```

The module must apply its Kotlin JVM or Android Kotlin plugin so it compiles
`src/main/kotlin`. The RPC generator reads the same schema and honors its Java
package/outer-class options. The protobuf message runtime is already a transitive
dependency of this library. Replace the downloaded Echo schema with your own
service definitions.

```kotlin
import io.gopherex.wsproto.CallOptions
import io.gopherex.wsproto.WsTransport
import io.gopherex.wsproto.WsTransportOptions
import echo.v1.Echo
import echo.v1.EchoServiceClient
import kotlinx.coroutines.flow.flowOf

val transport = WsTransport(
    "wss://api.example.com/rpc",
    WsTransportOptions(reconnect = true),
)
val client = EchoServiceClient(transport)

// In a coroutine:
val reply = client.unary(
    Echo.UnaryRequest.newBuilder().setName("Kotlin").build(),
    CallOptions(headers = mapOf("authorization" to "Bearer $token"), timeoutMs = 5_000),
)
client.bidi(flowOf(Echo.BidiRequest.newBuilder().setText("hello").build()))
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
`OkHttpSocketFactory(client)` uses the supplied client's TLS, proxy and networking
configuration; closing the transport closes its socket but does not shut down
the shared client. `WsSocketFactory` is also the test seam for deterministic
connection and framing tests. Mobile lifecycle integration is explicit: call
pause/resume from the application's lifecycle owner as appropriate.

## Build and verify

From the repository root:

```sh
./packages/ws-proto-kotlin/gradlew -p packages/ws-proto-kotlin test
./scripts/with-mobile-server.sh ./packages/ws-proto-kotlin/gradlew -p packages/ws-proto-kotlin test --rerun-tasks
./packages/ws-proto-kotlin/gradlew -p packages/ws-proto-kotlin publishToMavenLocal
```

The build generates wire messages from `transport/transport.proto`, and test
messages/clients from the existing Echo schema. Unit tests exercise framing,
credit, terminal events, concurrency, cancellation, metadata, interceptors and
reconnect controls. Integration tests run when `WS_PROTO_TEST_URL` is set; the
launcher builds a real Go server and supplies that variable. They cover all four
generated RPC methods, concurrent calls, large messages, flow-controlled input,
error details, deadlines and early server termination with an idle input source.

`-PreleaseVersion=X.Y.Z` selects the published version. Without it the build uses
the workspace version with a `-SNAPSHOT` suffix. GitHub Actions supplies
`GITHUB_ACTOR` and `GITHUB_TOKEN` for `publish`.
