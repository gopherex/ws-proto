#pragma once

#include "proto/echo/v1/echo.ws.pb.h"
#include <ws_proto/server.hpp>

namespace echo_fixture {
struct Task {
    struct promise_type {
        Task get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
};

inline void register_echo(ws_proto::ServerSession& server) {
    using namespace echo::v1;
    server.register_method(EchoServiceClient::method_Unary(), [](auto call) -> Task {
        auto req = co_await call.read();
        if (!req || !*req) co_return;
        UnaryResponse response;
        response.set_greeting("hello " + (**req).name());
        if (!call.write(response)) co_return;
        call.finish();
    });
    server.register_method(EchoServiceClient::method_ServerStream(), [](auto call) -> Task {
        auto req = co_await call.read();
        if (!req || !*req) co_return;
        for (int i = 0; i < (**req).count(); ++i) {
            ServerStreamResponse response;
            response.set_index(i);
            if (!call.write(response)) co_return;
        }
        call.finish();
    });
    server.register_method(EchoServiceClient::method_ClientStream(), [](auto call) -> Task {
        std::int32_t sum = 0;
        for (;;) {
            auto req = co_await call.read();
            if (!req) co_return;
            if (!*req) break;
            sum += (**req).value();
        }
        ClientStreamResponse response;
        response.set_sum(sum);
        if (!call.write(response)) co_return;
        call.finish();
    });
    server.register_method(EchoServiceClient::method_Bidi(), [](auto call) -> Task {
        for (;;) {
            auto req = co_await call.read();
            if (!req) co_return;
            if (!*req) break;
            BidiResponse response;
            response.set_echo((**req).text());
            if (!call.write(response)) co_return;
        }
        call.finish();
    });
}
} // namespace echo_fixture
