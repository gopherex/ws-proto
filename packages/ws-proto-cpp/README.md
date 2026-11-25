# C++23 client and server

Generate protobuf messages, typed clients and server method descriptors with EasyP. Link the runtime into a native C++23 application or a WebAssembly module. The host provides one ordered, binary, message-oriented connection per session.

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

Run `easyp generate --frozen` in the schema project. For the repository's Echo example with module root `.`, this produces `gen/proto/echo/v1/echo.pb.{h,cc}` and `gen/proto/echo/v1/echo.ws.pb.h`. Compile each application's generated `.pb.cc` once, and put `gen` on the include path. The runtime builds the transport envelope privately. The generated client methods use the prefix `call_` followed by the proto method name, including its original case. If a generated client name conflicts with a protobuf C++ symbol, the generator adds the first available numeric suffix starting at 2 (for example, `EchoClient2` and `EchoClient2T`).

## Use the host connection

Construct `ws_proto::Session` with a send callback taking ownership of one `std::string` of binary data, and a close callback. Returning true from send means the host accepted the message into its ordered, bounded write queue. A refusal fails the connection and its active calls. Callbacks must complete without blocking or throwing. All session operations, host events, and coroutine resumptions run on a single serialized event loop. Keep the host alive until the session is destroyed.

1. Negotiate the `wsrpc.v1` WebSocket subprotocol. Call `session.opened(selected_protocol)` with the actual result. A call started during the handshake queues its frames in order. A cancelled or expired call is removed before the queue is flushed. The default pre-open buffer limit is 4 MiB across all calls, including frames waiting for stream credit. An incompatible subprotocol ends the session with `Unavailable`.
2. Pass each complete binary WebSocket message to `session.receive(bytes)`. The runtime consumes the view during the call. Do not concatenate messages.
3. Drive deadlines by calling `session.expire()` on the event loop. A host can arm its timer for `session.next_deadline()` and re-arm it when calls or events change the deadlines. Pass an absolute `steady_clock` time point in `Options::deadline`.
4. Call `session.disconnected()` when the channel is lost. Create a fresh session for the next connection. Each session owns its stream IDs. Interrupted calls finish with a status. To observe shutdown even when there are no active calls, pass an `on_end(const SessionEnd&)` callback as the fourth constructor argument. It runs once. `SessionEnd::status` preserves the peer’s message and details. `retryable` is true for transport loss or a rejected host write. A peer’s `CONN_END`, a protocol mismatch, and exceeding a session limit are terminal. Use `is_open()` and `end()` to inspect the current state.

The session must outlive generated clients that refer to it. A typed RPC handle owns the call. Destroying the handle or destroying a coroutine suspended in `read()` or `result()` cancels that RPC. A `SignalSource` can also cancel one or more calls. Set `Options::cancellation = source.signal()` and call `source.notify()` on the session event loop. A waiting read resumes on the host event loop when a message or terminal status arrives. Streaming reads return credit when the caller consumes the message. At most one read can be outstanding on a call. After the last message, a successful stream returns an empty optional.

Inside an application coroutine

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

A write accepts a request into the bounded send queue. The runtime drains admitted writes in order as credit becomes available. Repeated `close_send()` sends one half-close after those writes. An individual `cancel()` resets that RPC. Streaming reads deliver queued responses before reporting a remote reset, connection termination, or transport loss. `Limits::max_frame_bytes` and `max_queued_messages` are optional caps. Their default of zero disables the respective cap. Send-buffer and stream flow-control byte limits still apply.

Unary `result()` requires exactly one response and an OK final status. Client-streaming `result()` consumes responses until the final status and returns the last message on success. Each client-streaming response is decoded as it arrives. An invalid response fails the RPC with `DataLoss`. An empty successful response stream is an error for both. Status details retain serialized protobuf Any messages.

Use `co_await call.headers_ready()` before reading leading metadata through `headers()`. This also resolves with empty metadata when the first message or the end arrives without a header. `co_await call.finished()` waits for the terminal status and trailers independently of reading the data. `Options::on_header` and `on_trailer` provide the same notifications through callbacks. Each runs once, including on failure.

For a client-streaming or bidi RPC, `call.write_from(next)` accepts an asynchronous request source. The runtime calls `next(Signal stop)` for each item. Its awaitable returns `Result<std::optional<Request>>`. An empty optional half-closes the request side. An error resets the RPC. An idle source subscribes its pending operation to `stop` so an early server response, cancellation, or local half-close releases its resources. A subscription detaches on destruction.

## Serve RPCs

Link `ws-proto::server` and include `<ws_proto/server.hpp>`. A `ServerSession` takes the host's send and close callbacks. Drive `opened(negotiated_protocol)`, `receive(frame)`, `disconnected()`, `expire(now)` and `next_deadline()` on the same serialized loop as the handlers. The server derives the call's local deadline from the remaining milliseconds in the OPEN frame. An optional final constructor argument supplies the monotonic clock.

Register a method once using its generated descriptor. For example, given a callback or eager coroutine `serve_bidi` accepting a `TypedServerCall<BidiRequest, BidiResponse>` by value:

```cpp
ws_proto::ServerSession server(send_frame, close_socket);
server.register_method(echo::v1::EchoServiceClient::method_Bidi(), serve_bidi);
```

The handler receives the call on OPEN. Read `call.headers()` for request metadata, including application keys such as `authorization` and `komeet-layer`. `call.deadline()` gives an optional monotonic time point. `co_await call.read()` returns a message, an empty optional on HALF_CLOSE, or a Status error. Use `call.send_headers(metadata)` before the first response, `call.write(message)` for responses, and `call.finish(status, trailers)` exactly once for the final status. Finishing drains admitted responses in order as the client returns window credit. A reset, transport loss or deadline wakes pending reads and raises `call.cancellation()`. Retain a call by value across suspension. With a lazy coroutine framework, launch the coroutine from the registered callback on the session loop. All four handler shapes are exercised in the [Echo server fixture](../../tests/cpp/echo_server.hpp).

A raw handler can register a path and `MethodKind` and read/write serialized protobuf payloads through `ServerCall`. Registration returns false for duplicate paths. An unknown method finishes with `Unimplemented` and leaves sibling RPCs running.

## Run both sessions in a test

Link `ws-proto::testing` and include `<ws_proto/testing.hpp>`. A fresh `ws_proto::testing::Loopback` owns `client`, `server` and a manual monotonic clock. Register handlers on `pipe.server`. Queue the handshake with `pipe.open()`, or supply a different subprotocol. Leave the pipe unopened to test handshake deadlines. A send only queues bytes. `step()` delivers one eligible event, and `drain()` delivers until the queues are blocked or empty.

Use `hold(direction, true/false)` to pause or release a direction, `delay(direction, duration)` for newly enqueued frames, and `inject(direction, bytes)` to enqueue a complete raw frame. Both directions preserve FIFO order. `advance(duration)` advances the clock and expires both sessions. Call `drain()` to deliver eligible frames afterward. `disconnect()` discards frames still in transit and queues a disconnect for both peers. [Test cases](../../tests/cpp/server_test.cpp)

## Await operations in Boost.Asio

Enable the vcpkg feature `ws-proto[asio]` (or configure the source build with `WS_PROTO_WITH_ASIO=ON`). Consumers of the base client, server and test loop need only protobuf.

```cmake
find_package(ws-proto CONFIG REQUIRED COMPONENTS asio)
target_link_libraries(app PRIVATE ws-proto::asio)
```

Include `<ws_proto/asio.hpp>`. In an Asio coroutine on the serialized session executor:

```cpp
auto message = co_await ws_proto::asio::async_read(rpc, boost::asio::use_awaitable);
auto result = co_await ws_proto::asio::async_result(unary_rpc, boost::asio::use_awaitable);
auto ready = co_await ws_proto::asio::async_wait(rpc.headers_ready(), boost::asio::use_awaitable);
```

Results use the same `ws_proto::Result` types as the native operations, including remote status details and streaming EOF. Initiate the operation on the session loop. For a plain callback, or when its associated executor differs from the session loop, pass the session executor as the first argument. Deferred tokens need the RPC to live until initiation. Completion is posted to the handler's associated executor. Asio cancellation of a pending read or result cancels the RPC on the session loop. Cancellation of a signal wait detaches only that subscription. [Asio tests](../../tests/cpp/asio_test.cpp)

## Intercept generated calls

A generated `EchoServiceClient` uses a session directly. Its `EchoServiceClientT` form accepts a channel wrapped by one or more typed interceptors. The outermost interceptor runs first on requests and last on responses. Each receives the method path, kind, RPC type, options and, for unary or server-streaming calls, the request.

```cpp
auto authenticated = ws_proto::intercept(session,
    [&](auto method, auto next, ws_proto::Options options, const auto&... request) {
        options.headers["authorization"] = token_for(method.path);
        return next(std::move(options), request...);
    });
echo::v1::EchoServiceClientT client(authenticated);
```

Keep wrapped channels alive while their clients use them. A handler can pass a modified unary request to `next`, use `map_requests` on an upload or bidi call, and use `map_responses` on any call. For a cached response or local rejection, return `Rpc::completed(responses, options, status, headers, trailers)`, where `Rpc` is `typename decltype(method)::CallType`. This completes the call locally and delivers the metadata callbacks.

The Firefox consumer in [tests/cpp/browser](../../tests/cpp/browser) supplies the browser WebSocket and clock, and uses all four generated method shapes against the Go server. Run `make test-cpp` on Linux with a bootstrapped `VCPKG_ROOT`, an activated Emscripten SDK, EasyP, Go, Node 22+, Ninja, CMake and Firefox. CI pins the build tools used by this test.

To run the same consumer from the installed registry ports, run `scripts/test-cpp-registry.sh`. The `--local` option reads the registry and its pinned source from the current repository's Git history, including unpublished commits.

When publishing a new C++ version, commit the library and generators first. Pin both portfiles to that source commit, update their manifest versions and commit the recipes. Update the version database with `vcpkg x-add-version --all --x-builtin-ports-root=ports --x-builtin-registry-versions-dir=versions` from the repository root. Point the standalone consumer's registry baseline at the commit containing the updated database. Registry and source commits must be reachable in the published Git history.
