#pragma once

#include "ws_proto/client.hpp"

#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

namespace ws_proto::asio {
namespace detail {
namespace net = ::boost::asio;

// All mutable state except the completion handler runs on the session's executor.
template<class Value, class Handler, class Executor>
struct Pending {
    using CompletionExecutor = net::associated_executor_t<Handler, Executor>;
    net::executor_work_guard<Executor> session_work;
    net::executor_work_guard<CompletionExecutor> completion_work;
    std::optional<Handler> handler;
    Subscription subscription;
    std::function<void()> cancel;

    Pending(Handler h, Executor executor)
        : session_work(executor), completion_work(net::get_associated_executor(h, executor)), handler(std::move(h)) {}

    void complete(Value value) {
        if (!handler) return;
        auto h = std::move(*handler);
        handler.reset();
        subscription.reset();
        cancel = {};
        net::post(completion_work.get_executor(), [h = std::move(h), value = std::move(value)]() mutable {
            std::move(h)(std::move(value));
        });
    }
};

struct FromToken {};
template<class Value, class Executor, class Start, class Token>
auto initiate(Executor executor, Start start, Token&& token) {
    return net::async_initiate<Token, void(Value)>(
        [executor = std::move(executor), start = std::move(start)](auto h) mutable {
            auto session = [&] {
                if constexpr (std::is_same_v<Executor, FromToken>) {
                    auto ex = net::get_associated_executor(h);
                    static_assert(!std::is_same_v<net::associated_executor_t<decltype(h), FromToken>, FromToken>,
                        "Pass the session executor explicitly for a token without an associated executor");
                    return ex;
                } else return executor;
            }();
            using State = Pending<Value, decltype(h), decltype(session)>;
            auto slot = net::get_associated_cancellation_slot(h);
            auto state = std::make_shared<State>(std::move(h), session);
            if (slot.is_connected())
                slot.assign([weak = std::weak_ptr(state)](net::cancellation_type_t type) {
                    if (type == net::cancellation_type::none) return;
                    auto active = weak.lock();
                    if (!active) return;
                    net::post(active->session_work.get_executor(), [weak] {
                        if (auto state = weak.lock(); state && state->cancel) {
                            auto cancel = std::move(state->cancel);
                            cancel();
                        }
                    });
                });
            // Initiation (including deferred initiation) is on the serialized session loop.
            start(std::move(state));
        }, token);
}

struct ReaderTask {
    struct promise_type {
        ReaderTask get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
};
template<class State, class Rpc, class Operation>
ReaderTask read_into(std::shared_ptr<State> state, Rpc& rpc, Operation op) {
    state->cancel = rpc.canceller();
    auto value = co_await (rpc.*op)();
    state->complete(std::move(value));
}

template<class Executor, class Rpc, class Token, class Op>
auto read(Executor ex, Rpc& rpc, Token&& token, Op op) {
    using Value = decltype((rpc.*op)().await_resume());
    return initiate<Value>(std::move(ex), [&rpc, op](auto state) {
        read_into(std::move(state), rpc, op);
    }, std::forward<Token>(token));
}
} // namespace detail

// With an explicit executor, initiation and all accesses to rpc/signal belong to that loop.
// The shorter form uses the handler's associated executor as the session loop.
template<class Executor, class Rpc, class Token>
auto async_read(Executor executor, Rpc& rpc, Token&& token) {
    return detail::read(std::move(executor), rpc, std::forward<Token>(token), &Rpc::read);
}
template<class Rpc, class Token>
auto async_read(Rpc& rpc, Token&& token) {
    return async_read(detail::FromToken{}, rpc, std::forward<Token>(token));
}
template<class Executor, class Rpc, class Token>
auto async_result(Executor executor, Rpc& rpc, Token&& token) {
    return detail::read(std::move(executor), rpc, std::forward<Token>(token), &Rpc::result);
}
template<class Rpc, class Token>
auto async_result(Rpc& rpc, Token&& token) {
    return async_result(detail::FromToken{}, rpc, std::forward<Token>(token));
}
template<class Executor, class Token>
auto async_wait(Executor executor, Signal signal, Token&& token) {
    return detail::initiate<Result<void>>(std::move(executor), [signal = std::move(signal)](auto state) {
        if (!signal) {
            state->complete(std::unexpected(Status{Code::failed_precondition, "empty signal"}));
            return;
        }
        state->cancel = [weak = std::weak_ptr(state)] {
            if (auto state = weak.lock())
                state->complete(std::unexpected(Status{Code::cancelled, "wait cancelled"}));
        };
        state->subscription = signal.subscribe([state] { state->complete({}); });
    }, std::forward<Token>(token));
}
template<class Token>
auto async_wait(Signal signal, Token&& token) {
    return async_wait(detail::FromToken{}, std::move(signal), std::forward<Token>(token));
}
} // namespace ws_proto::asio
