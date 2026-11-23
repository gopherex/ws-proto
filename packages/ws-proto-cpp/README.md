# C++23 client

Generate protobuf messages and typed clients with EasyP. Link the runtime into a native C++23 application or a WebAssembly module. The host provides one ordered, binary, message-oriented connection per session.

## Install with vcpkg

Use manifest mode. Copy the [registry configuration](../../tests/cpp/registry/vcpkg-configuration.json) alongside the application's `vcpkg.json`. It selects the ports in this repository by Git commit. The following manifest selects the protobuf baseline used by the interoperability test:

```json
{
  "builtin-baseline": "9eec002df1cd109f8f104cd966a763daf9751489",
  "dependencies": [
    "ws-proto",
    { "name": "protoc-gen-ws-cpp", "host": true }
  ]
}
```

Configure CMake with the vcpkg toolchain. Both static and shared native builds use the target triplet selected by the application. For WASM, use the Emscripten toolchain and the `wasm32-emscripten` target triplet.

```cmake
find_package(ws-proto CONFIG REQUIRED)
target_link_libraries(app PRIVATE ws-proto::client)
```

## Generate

Add the installed host tool directory `<installed-dir>/<host-triplet>/tools/protoc-gen-ws-cpp` to `PATH`. It contains two executables. `protoc-gen-cpp` exposes the upstream protobuf C++ generator as an EasyP plugin. It comes from the same protobuf release as the target runtime. `protoc-gen-ws-cpp` emits the service clients. The plugins support application schemas in proto2 and proto3. A consumer that already builds its protobuf messages from the matching release can enable just `ws-cpp` and link its existing message target.

```yaml
version: v1
generate:
  paths: [proto]
plugins:
  - name: cpp
    out: gen
  - name: ws-cpp
    out: gen
```

Run `easyp generate --frozen` in the schema project. For the repository's Echo example with module root `.`, this produces `gen/proto/echo/v1/echo.pb.{h,cc}` and `gen/proto/echo/v1/echo.ws.pb.h`. Compile each application's generated `.pb.cc` once, and put `gen` on the include path. The runtime builds the transport envelope privately. The generated client methods use the prefix `call_` followed by the proto method name, including its original case.

## Use the host connection

Construct `ws_proto::Session` with a send callback taking ownership of one `std::string` of binary data, and a close callback. Returning true from send means the host accepted the message into its ordered, bounded write queue. A refusal fails the connection and its active calls. Callbacks must complete without blocking or throwing. All session operations, host events, and coroutine resumptions run on a single serialized event loop. Keep the host alive until the session is destroyed.

1. Negotiate the `wsrpc.v1` WebSocket subprotocol. Call `session.opened(selected_protocol)` with the actual result before starting calls. The host can supply an already-open channel.
2. Pass each complete binary WebSocket message to `session.receive(bytes)`. The runtime consumes the view during the call. Do not concatenate messages.
3. Drive deadlines by calling `session.expire()` on the event loop. A host can arm its timer for `session.next_deadline()` and re-arm it when calls or events change the deadlines. Pass an absolute `steady_clock` time point in `Options::deadline`.
4. Call `session.disconnected()` when the channel is lost. Create a fresh session for the next connection. Each session owns its stream IDs. Interrupted calls finish with a status and are never replayed.

The session must outlive generated clients that refer to it. A typed RPC handle owns the call. Destroying the handle cancels an active RPC. A waiting read resumes on the host event loop when a message or terminal status arrives. Streaming reads return credit when the caller consumes the message. At most one read can be outstanding on a call. After the last message, a successful stream returns an empty optional.

Inside an application coroutine:

```cpp
echo::v1::EchoServiceClient client(session);
echo::v1::BidiRequest request;
request.set_text("hello");
auto call = client.call_Bidi();

if (auto sent = call.write(request); !sent) {
    co_return std::unexpected(sent.error());
}
auto first = co_await call.read();  // available before the request side closes
call.close_send();
```

A write accepts a request into the bounded send queue. The runtime drains admitted writes in order as credit becomes available. Repeated `close_send()` sends one half-close after those writes. An individual `cancel()` resets that RPC. Unary and client-streaming methods expose `result()`, which waits for the final status and exactly one response. Server-streaming and bidi methods expose `read()`. Inspect `headers()` for received leading metadata and `trailers()` after completion. Status details retain serialized protobuf Any messages.

The Firefox consumer in [tests/cpp/browser](../../tests/cpp/browser) supplies the browser WebSocket and clock, and uses all four generated method shapes against the Go server. Run `make test-cpp` on Linux with a bootstrapped `VCPKG_ROOT`, an activated Emscripten SDK, EasyP, Go, Node 22+, Ninja, CMake and Firefox. CI pins the build tools used by this test.

To run the same consumer from the installed registry ports, run `scripts/test-cpp-registry.sh`. The `--local` option reads the registry and its pinned source from the current repository's Git history, including unpublished commits.

When publishing a new C++ version, commit the library and generators first. Pin both portfiles to that source commit, update their manifest versions and commit the recipes. Update the version database with `vcpkg x-add-version --all --x-builtin-ports-root=ports --x-builtin-registry-versions-dir=versions` from the repository root. Point the standalone consumer's registry baseline at the commit containing the updated database. Registry and source commits must be reachable in the published Git history.
