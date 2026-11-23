#include "proto/echo/v1/echo.ws.pb.h"
#include <emscripten.h>
#include <exception>

using namespace echo::v1;
using namespace ws_proto;

EM_JS(bool, host_send, (const char* bytes, std::size_t size), {
    const ws = Module.socket;
    if (ws.readyState !== WebSocket.OPEN) return false;
    ws.send(HEAPU8.subarray(bytes, bytes + size));
    return true;
});
EM_JS(void, host_close, (), { Module.socket.close(); });
EM_JS(void, report, (const char* result), { Module.report(UTF8ToString(result)); });

EM_JS(void, verify_go_cancellation, (), {
    fetch('/cancelled').then(response => Module._verified(Number(response.ok)), () => Module._verified(0));
});
static SignalSource probe_finished;
static bool go_cancelled = false;

static Session session([](std::string bytes) { return host_send(bytes.data(), bytes.size()); }, host_close);

struct TestTask {
    struct promise_type {
        TestTask get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { report("unexpected C++ exception"); }
    };
};

#define CHECK(expr) do { if (!(expr)) { report(#expr); co_return; } } while (false)

TestTask run() {
    EchoServiceClient echo(session);
    UnaryRequest request;
    request.set_name("WASM");
    Options options;
    options.headers["x-test"] = "web";
    auto unary = echo.call_Unary(request, options);
    co_await unary.headers_ready();
    CHECK(unary.headers().at("x-header") == "ready");
    auto reply = co_await unary.result();
    CHECK(reply && reply->greeting() == "hello WASM");
    CHECK(unary.headers().at("x-echo") == "web" && unary.trailers().at("x-trailer") == "done");

    request.set_name("!error");
    auto error = co_await echo.call_Unary(request).result();
    CHECK(!error && error.error().code == Code::invalid_argument && error.error().details.size() == 1);

    ServerStreamRequest list;
    list.set_count(3);
    auto download = echo.call_ServerStream(list);
    for (int i = 0; i < 3; ++i) {
        auto item = co_await download.read();
        CHECK(item && *item && (**item).index() == i);
    }
    auto eof = co_await download.read();
    CHECK(eof && !*eof);

    auto upload = echo.call_ClientStream();
    ClientStreamRequest number;
    for (int i = 1; i <= 10; ++i) {
        number.set_value(i);
        CHECK(upload.write(number));
    }
    upload.close_send();
    auto sum = co_await upload.result();
    CHECK(sum && sum->sum() == 55);

    auto duplex = echo.call_Bidi();
    BidiRequest text;
    text.set_text("before close");
    CHECK(duplex.write(text));
    auto first = co_await duplex.read();
    CHECK(first && *first && (**first).echo() == "echo:before close");

    text.set_text(std::string(300000, 'a'));
    CHECK(duplex.write(text));
    text.set_text("last");
    CHECK(duplex.write(text));
    duplex.close_send();
    auto large = co_await duplex.read();
    CHECK(large && *large && (**large).echo().size() == 300005);
    auto last = co_await duplex.read();
    CHECK(last && *last && (**last).echo() == "echo:last");
    auto bidi_eof = co_await duplex.read();
    CHECK(bidi_eof && !*bidi_eof);

    request.set_name("!wait");
    SignalSource stop;
    Options tracked{.headers = {{"x-probe-id", "cpp-browser"}}, .cancellation = stop.signal()};
    auto cancelled = echo.call_Unary(request, tracked);
    co_await cancelled.headers_ready();
    CHECK(cancelled.headers().at("x-header") == "ready");
    stop.notify();
    auto cancelled_result = co_await cancelled.result();
    CHECK(!cancelled_result && cancelled_result.error().code == Code::cancelled);

    verify_go_cancellation();
    co_await probe_finished.signal();
    CHECK(go_cancelled);

    options.deadline = Clock::now() + std::chrono::milliseconds(100);
    auto timeout = co_await echo.call_Unary(request, options).result();
    CHECK(!timeout && timeout.error().code == Code::deadline_exceeded);

    auto pending = echo.call_Unary(request);
    host_close();
    auto lost = co_await pending.result();
    CHECK(!lost && lost.error().code == Code::unavailable);
    report("ok");
}

extern "C" {
void start() { run(); }
void verified(int cancelled) {
    go_cancelled = cancelled != 0;
    probe_finished.notify();
}
void opened(int negotiated) {
    session.opened(negotiated ? "wsrpc.v1" : "");
}
void message(const char* bytes, int size) { session.receive({bytes, static_cast<std::size_t>(size)}); }
void closed() { session.disconnected(); }
void expire() { session.expire(); }
}
