#include <ws_proto/async_client.hpp>
#include <ws_proto/testing.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <google/protobuf/wrappers.pb.h>
#include <gtest/gtest.h>

namespace net = boost::asio;
namespace wa = ws_proto::asio;
using namespace ws_proto;
using Text = google::protobuf::StringValue;
using Unary = Rpc<Text, Text, false, false>;
using Bidi = Rpc<Text, Text, true, true>;

struct Retry {
    int& attempts;
    int& finishes;
    net::awaitable<wa::RetryDecision> attempt_failed(auto, const wa::AttemptFailure& failure, wa::CallContext) {
        ++attempts;
        EXPECT_EQ(failure.status.code, Code::unavailable);
        if (failure.attempt == 1) co_return Clock::duration::zero();
        co_return std::nullopt;
    }
    net::awaitable<Status> finish(auto, Status status, wa::CallContext) {
        ++finishes;
        co_return status;
    }
};

void pump(net::io_context& io, ws_proto::testing::Loopback& pipe) {
    for (int i = 0; i < 20; ++i) {
        io.restart();
        io.poll();
        pipe.drain();
    }
}

TEST(AsyncClient, RetriesUnaryBeforeHeadersAndFinishesOnce) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0, finishes = 0, served = 0;
    pipe.server.register_method(Method<Unary>{"/unary"}, [&](auto call) {
        if (++served == 1) call.finish({Code::unavailable, "try again"});
        else {
            Text response;
            response.set_value("ok");
            EXPECT_TRUE(call.write(response));
            call.finish();
        }
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Retry{attempts, finishes});
    auto rpc = channel.invoke(Method<Unary>{"/unary"}, {}, Text{});
    auto result = net::co_spawn(io, rpc.result(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(result.get().value().value(), "ok");
    EXPECT_EQ(served, 2);
    EXPECT_EQ(attempts, 1);
    EXPECT_EQ(finishes, 1);
}

TEST(AsyncClient, EmptyHeaderCommitsWithoutRead) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0, finishes = 0;
    pipe.server.register_method(Method<Bidi>{"/bidi"}, [](auto call) {
        EXPECT_TRUE(call.send_headers({}));
        call.finish({Code::unavailable, "ended"});
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Retry{attempts, finishes});
    auto rpc = channel.invoke(Method<Bidi>{"/bidi"}, {});
    auto result = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(result.get().error().code, Code::unavailable);
    EXPECT_EQ(attempts, 0);
    EXPECT_EQ(finishes, 1);
}

struct Trace {
    std::vector<std::string>& steps;
    std::string label;
    net::awaitable<Result<void>> open(auto, Options opts, auto next, wa::CallContext) {
        steps.push_back("open " + label);
        opts.headers[label] = "yes";
        co_return co_await next(std::move(opts));
    }
    net::awaitable<Result<void>> send(auto, Text& text, wa::CallContext) {
        steps.push_back("send " + label);
        text.set_value(text.value() + label);
        co_return Result<void>{};
    }
    net::awaitable<Result<void>> receive(auto, Text& text, wa::CallContext) {
        steps.push_back("receive " + label);
        text.set_value(text.value() + label);
        co_return Result<void>{};
    }
    net::awaitable<Status> finish(auto, Status status, wa::CallContext) {
        steps.push_back("finish " + label);
        co_return status;
    }
};

TEST(AsyncClient, BidiReplaysTransformedMessagesAndHalfCloseOnce) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0, finishes = 0;
    std::vector<std::string> steps;
    std::vector<TypedServerCall<Text, Text>> calls;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto call) { calls.push_back(call); });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; },
        Trace{steps, "A"}, Retry{attempts, finishes}, Trace{steps, "B"});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {});
    Text message;
    message.set_value("request");
    auto sent = net::co_spawn(io, rpc.write(message), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(sent.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_TRUE(sent.get());
    ASSERT_EQ(calls.size(), 1);
    EXPECT_EQ(calls[0].headers().at("A"), "yes");
    EXPECT_EQ(calls[0].headers().at("B"), "yes");
    EXPECT_TRUE(rpc.close_send());
    pump(io, pipe);
    EXPECT_EQ(calls[0].read().await_resume().value()->value(), "requestAB");
    EXPECT_FALSE(calls[0].read().await_resume().value());
    calls[0].finish({Code::unavailable, "retry"});
    pump(io, pipe);
    ASSERT_EQ(calls.size(), 2);
    EXPECT_EQ(calls[1].read().await_resume().value()->value(), "requestAB");
    EXPECT_FALSE(calls[1].read().await_resume().value());
    Text response;
    response.set_value("response");
    ASSERT_TRUE(calls[1].write(response));
    calls[1].finish();
    auto read = net::co_spawn(io, rpc.read(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(read.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(read.get().value()->value(), "responseBA");
    EXPECT_EQ(steps, (std::vector<std::string>{
        "open A", "open B", "send A", "send B", "receive B", "receive A", "finish B", "finish A"}));
    EXPECT_EQ(finishes, 1);
}

struct Backoff {
    bool& asked;
    net::awaitable<wa::RetryDecision> attempt_failed(auto, const wa::AttemptFailure&, wa::CallContext) {
        asked = true;
        co_return std::chrono::hours(1);
    }
};

TEST(AsyncClient, CancellationInterruptsBackoffAndKeepsStatus) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    bool asked = false;
    int served = 0;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto call) {
        ++served;
        call.finish({Code::unavailable, "retry"});
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Backoff{asked});
    SignalSource stop;
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {.cancellation = stop.signal()});
    auto outcome = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    EXPECT_TRUE(asked);
    stop.notify();
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().code, Code::cancelled);
    EXPECT_EQ(served, 1);
}

TEST(AsyncClient, RequestLargerThanReplayBudgetContinuesWithoutRetry) {
    ws_proto::testing::Loopback pipe({.max_retry_buffer=1});
    net::io_context io;
    int attempts = 0, finishes = 0;
    std::optional<TypedServerCall<Text, Text>> server;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto call) { server = call; });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Retry{attempts, finishes});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {});
    Text message;
    message.set_value("larger");
    auto sent = net::co_spawn(io, rpc.write(message), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(sent.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_TRUE(sent.get());
    ASSERT_TRUE(server);
    EXPECT_EQ(server->read().await_resume().value()->value(), "larger");
    server->finish({Code::unavailable, "ended"});
    auto outcome = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().code, Code::unavailable);
    EXPECT_EQ(attempts, 0);
    EXPECT_EQ(finishes, 1);
}

TEST(AsyncClient, FailureAdapterPreservesStatusAndDecodesOnlyFailures) {
    Status status{Code::permission_denied, "denied", {"detail", "other"}};
    auto decode = [](const Status& input) -> std::optional<int> {
        if (input.details.front() == "detail") return 42;
        return {};
    };
    auto refusal = decode_failure(Result<Text>(std::unexpect, status), decode);
    EXPECT_EQ(refusal.error().status.details, status.details);
    EXPECT_EQ(refusal.error().method_error, 42);
    EXPECT_TRUE(decode_failure(Result<void>{}, decode));
    auto plain = decode_failure(Result<void>(std::unexpect, status),
        [](const Status&) -> std::optional<int> { return {}; });
    EXPECT_FALSE(plain.error().method_error);
    EXPECT_EQ(plain.error().status.code, status.code);
}

struct AwaitOpen {
    Signal gate;
    net::awaitable<Result<void>> open(auto, Options opts, auto next, wa::CallContext) {
        auto ready = co_await wa::async_wait(gate, net::use_awaitable);
        if (!ready) co_return std::unexpected(std::move(ready.error()));
        co_return co_await next(std::move(opts));
    }
};

struct RefuseResponse {
    net::awaitable<Result<void>> receive(auto, Text&, wa::CallContext) {
        co_return std::unexpected(Status{Code::permission_denied, "response refused", {"details"}});
    }
    net::awaitable<Status> finish(auto, Status status, wa::CallContext) {
        EXPECT_EQ(status.code, Code::permission_denied);
        status.message = "mapped";
        co_return status;
    }
};

TEST(AsyncClient, OpenWaitsAndReceiveRefusalIsMappedByFinish) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int served = 0;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto call) {
        ++served;
        EXPECT_TRUE(call.write(Text{}));
        call.finish();
    });
    pipe.open();
    SignalSource ready;
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; },
        AwaitOpen{ready.signal()}, RefuseResponse{});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {});
    auto finished = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    EXPECT_EQ(served, 0);
    ready.notify();
    pump(io, pipe);
    EXPECT_EQ(served, 1);
    ASSERT_EQ(finished.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    auto result = finished.get();
    EXPECT_EQ(result.error().message, "mapped");
    EXPECT_EQ(result.error().details.at(0), "details");
}

#ifdef WS_PROTO_TEST_GENERATED
#include "echo_server.hpp"

TEST(AsyncClient, GeneratedClientsSupportBothOneWayStreamingKinds) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    echo_fixture::register_echo(pipe.server);
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; });
    echo::v1::EchoServiceClientT client(channel);
    echo::v1::ServerStreamRequest list;
    list.set_count(2);
    auto down = client.call_ServerStream(list);
    auto up = client.call_ClientStream();
    auto flow = [&]() -> net::awaitable<void> {
        auto first = co_await down.read();
        EXPECT_EQ(first.value()->index(), 0);
        auto second = co_await down.read();
        EXPECT_EQ(second.value()->index(), 1);
        EXPECT_FALSE((co_await down.read()).value());
        auto requests = [step = 0](Signal) mutable
            -> net::awaitable<Result<std::optional<echo::v1::ClientStreamRequest>>> {
            if (++step == 3) co_return std::nullopt;
            echo::v1::ClientStreamRequest request;
            request.set_value(step * 2);
            co_return request;
        };
        EXPECT_TRUE(co_await up.write_from(std::move(requests)));
        auto reply = co_await up.result();
        EXPECT_EQ(reply.value().sum(), 6);
    };
    auto result = net::co_spawn(io, flow(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_NO_THROW(result.get());
}
#endif

TEST(AsyncClient, ResponseMessageCommitsBeforeApplicationReads) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0, finishes = 0;
    pipe.server.register_method(Method<Bidi>{"/live"}, [](auto call) {
        Text text;
        text.set_value("committed");
        EXPECT_TRUE(call.write(text));
        call.finish({Code::unavailable, "ended"});
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Retry{attempts, finishes});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {});
    pump(io, pipe);
    auto outcome = net::co_spawn(io, rpc.finish(), net::use_future);
    auto item = net::co_spawn(io, rpc.read(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    ASSERT_EQ(item.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().code, Code::unavailable);
    EXPECT_EQ(item.get().value()->value(), "committed");
    EXPECT_EQ(attempts, 0);
    EXPECT_EQ(finishes, 1);
}

TEST(AsyncClient, RetryGetsFreshSessionAndKeepsUnaryRequest) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0, finishes = 0, picks = 0;
    std::optional<TypedServerCall<Text, Text>> server;
    pipe.server.register_method(Method<Unary>{"/unary"}, [&](auto call) { server = call; });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&]() -> Session* {
        return ++picks == 1 ? nullptr : &pipe.client;
    }, Retry{attempts, finishes});
    Text message;
    message.set_value("initial");
    auto rpc = channel.invoke(Method<Unary>{"/unary"}, {}, message);
    pump(io, pipe);
    ASSERT_TRUE(server);
    EXPECT_EQ(server->read().await_resume().value()->value(), "initial");
    EXPECT_FALSE(server->read().await_resume().value());
    EXPECT_TRUE(server->write(message));
    server->finish();
    auto outcome = net::co_spawn(io, rpc.result(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().value().value(), "initial");
}

TEST(AsyncClient, CancellationDuringOpenRunsFinishAndSendsNothing) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int served = 0, attempts = 0, finishes = 0;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto) { ++served; });
    pipe.open();
    SignalSource ready;
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; },
        AwaitOpen{ready.signal()}, Retry{attempts, finishes});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {});
    auto outcome = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    rpc.cancel();
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().code, Code::cancelled);
    EXPECT_EQ(served, 0);
    EXPECT_EQ(finishes, 1);
}

struct SelectDecoder {
    auto operator()(Method<Unary>) const {
        return [](const Status& status) -> std::optional<Text> {
            for (const auto& detail : status.details) {
                Text decoded;
                if (decoded.ParseFromString(detail)) return decoded;
            }
            return {};
        };
    }
};

TEST(AsyncClient, DecoderChannelTypesResultsAndFinishByMethod) {
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    pipe.server.register_method(Method<Unary>{"/error"}, [](auto call) {
        Text detail;
        detail.set_value("typed");
        call.finish({Code::permission_denied, "refused", {detail.SerializeAsString()}});
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; });
    auto decoded = wa::with_failure_decoder(channel, SelectDecoder{});
    auto rpc = decoded.invoke(Method<Unary>{"/error"}, {}, Text{});
    auto outcome = net::co_spawn(io, rpc.result(), net::use_future);
    auto finish = net::co_spawn(io, wa::async_finish(rpc), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    ASSERT_EQ(finish.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().method_error->value(), "typed");
    EXPECT_EQ(finish.get().error().status.code, Code::permission_denied);
}

#ifdef WS_PROTO_TEST_GENERATED
TEST(AsyncClient, GeneratedMethodIdentitySelectsDecoderAcrossChannels) {
    using echo::v1::EchoServiceClient;
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    pipe.server.register_method(EchoServiceClient::method_Unary(), [](auto call) {
        call.finish({Code::permission_denied, "refused"});
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; });
    auto decoded = wa::with_failure_decoder(channel, [](decltype(EchoServiceClient::method_Unary())) {
        return [](const Status&) -> std::optional<int> { return 7; };
    });
    echo::v1::EchoServiceClientT client(decoded);
    auto rpc = client.call_Unary(echo::v1::UnaryRequest{});
    auto outcome = wa::async_result(rpc, net::use_future);
    pump(io, pipe);
    ASSERT_EQ(outcome.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().method_error, 7);
}
#endif

TEST(AsyncClient, DeadlineInterruptsBackoffAndPreventsAnotherAttempt) {
    using namespace std::chrono_literals;
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    bool asked = false;
    int served = 0;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto call) {
        ++served;
        call.finish({Code::unavailable, "retry"});
    });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Backoff{asked});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {.deadline = Clock::now() + 200ms});
    auto outcome = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    ASSERT_TRUE(asked);
    io.run_for(1s);
    pipe.drain();
    ASSERT_EQ(outcome.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().code, Code::deadline_exceeded);
    EXPECT_EQ(served, 1);
}

TEST(AsyncClient, BackoffUsesExplicitSettingsAndEntropy) {
    using namespace std::chrono_literals;
    const wa::ExponentialBackoff backoff{
        .initial=100ms, .maximum=250ms, .multiplier=2, .jitter=0.25, .max_attempts=4};
    EXPECT_EQ(backoff.delay(1, 0), 75ms);
    EXPECT_EQ(backoff.delay(2, 0.5), 200ms);
    EXPECT_EQ(backoff.delay(3, 1), 250ms);
    EXPECT_FALSE(backoff.delay(4, 0.5));
}

TEST(AsyncClient, BothOneWayStreamingKindsReplayBeforeCommit) {
    using Upload = Rpc<Text, Text, true, false>;
    using Download = Rpc<Text, Text, false, true>;
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0, finishes = 0;
    std::vector<TypedServerCall<Text, Text>> uploads, downloads;
    pipe.server.register_method(Method<Upload>{"/upload"}, [&](auto call) { uploads.push_back(call); });
    pipe.server.register_method(Method<Download>{"/download"}, [&](auto call) { downloads.push_back(call); });
    pipe.open();
    auto channel = wa::intercept_async(io.get_executor(), [&] { return &pipe.client; }, Retry{attempts, finishes});

    Text message;
    message.set_value("bytes to replay");
    auto up = channel.invoke(Method<Upload>{"/upload"}, {});
    auto down = channel.invoke(Method<Download>{"/download"}, {}, message);
    auto sent = net::co_spawn(io, up.write(message), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(sent.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    ASSERT_TRUE(sent.get());
    ASSERT_TRUE(up.close_send());
    pump(io, pipe);
    ASSERT_EQ(uploads.size(), 1);
    ASSERT_EQ(downloads.size(), 1);
    uploads[0].finish({Code::unavailable, "retry upload"});
    downloads[0].finish({Code::unavailable, "retry download"});
    pump(io, pipe);

    ASSERT_EQ(uploads.size(), 2);
    ASSERT_EQ(downloads.size(), 2);
    for (auto* call : {&uploads.back(), &downloads.back()}) {
        ASSERT_TRUE(call->read().await_ready());
        EXPECT_EQ(call->read().await_resume().value()->value(), message.value());
        ASSERT_TRUE(call->read().await_ready());
        EXPECT_FALSE(call->read().await_resume().value());
        EXPECT_TRUE(call->write(message));
        call->finish();
    }
    auto result = net::co_spawn(io, up.result(), net::use_future);
    auto response = net::co_spawn(io, down.read(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    ASSERT_EQ(response.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(result.get().value().value(), message.value());
    EXPECT_EQ(response.get().value()->value(), message.value());
    EXPECT_EQ(attempts, 2);
    EXPECT_EQ(finishes, 2);
}

namespace {
class RelativeTimer {
public:
    explicit RelativeTimer(net::any_io_executor executor) : timer_(std::move(executor)) {}
    RelativeTimer(const RelativeTimer&) = delete;

    void expires_after(Clock::duration delay) { timer_.expires_after(delay); }
    void cancel() { timer_.cancel(); }

    template<class Token>
    auto async_wait(Token&& token) { return timer_.async_wait(std::forward<Token>(token)); }

private:
    net::steady_timer timer_;
};

struct TimedRetry {
    net::awaitable<wa::RetryDecision> attempt_failed(auto, const wa::AttemptFailure& failure, wa::CallContext) {
        if (failure.attempt == 1) co_return std::chrono::milliseconds(10);
        co_return std::nullopt;
    }
};
} // namespace

TEST(AsyncClient, RelativeTimerTypeDrivesRetryAndDeadline) {
    using namespace std::chrono_literals;
    ws_proto::testing::Loopback pipe;
    net::io_context io;
    int attempts = 0;
    pipe.server.register_method(Method<Bidi>{"/live"}, [&](auto call) {
        if (++attempts == 1) call.finish({Code::unavailable, "retry"});
    });
    pipe.open();
    auto channel = wa::intercept_async<RelativeTimer>(io.get_executor(), [&] { return &pipe.client; }, TimedRetry{});
    auto rpc = channel.invoke(Method<Bidi>{"/live"}, {.deadline = Clock::now() + 2s});
    auto outcome = net::co_spawn(io, rpc.finish(), net::use_future);
    pump(io, pipe);
    ASSERT_EQ(attempts, 1);
    io.run_for(30ms);
    pump(io, pipe);
    ASSERT_EQ(attempts, 2);
    io.run_for(3s);
    ASSERT_EQ(outcome.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(outcome.get().error().code, Code::deadline_exceeded);
}

namespace {
struct RefuseOpen {
    template<class Method, class Next>
    net::awaitable<Result<void>> open(Method, Options, Next, wa::CallContext) {
        co_return std::unexpected(Status{Code::failed_precondition, "gate", {"typed detail"}});
    }
};

template<class Channel>
net::awaitable<void> expect_refused_calls(Channel& channel) {
    for (int i = 0; i < 32; ++i) {
        auto result = co_await channel.invoke(Method<Unary>{"/refused"}, {}, Text{}).result();
        EXPECT_FALSE(result);
        if (result) co_return;
        EXPECT_EQ(result.error().status.code, Code::failed_precondition);
        EXPECT_EQ(result.error().method_error, "typed detail");
        co_await net::post(net::use_awaitable);
    }
}
} // namespace

TEST(AsyncClient, TemporaryRefusedCallCanFinishAcrossExecutorSteps) {
    using namespace std::chrono_literals;
    net::io_context io;
    int opens = 0, retries = 0, finishes = 0;
    auto channel = wa::intercept_async(io.get_executor(),
        [&]() -> Session* { ++opens; return nullptr; }, RefuseOpen{}, Retry{retries, finishes});
    auto decoded = wa::with_failure_decoder(channel, [](Method<Unary>) {
        return [](const Status& status) -> std::optional<std::string> { return status.details.at(0); };
    });
    auto outcome = net::co_spawn(io, expect_refused_calls(decoded), net::use_future);
    for (int i = 0; i < 1024 && !io.stopped(); ++i) {
        if (i % 2 == 0) io.poll_one();
        else io.run_for(1ms);
    }
    ASSERT_EQ(outcome.wait_for(0s), std::future_status::ready);
    EXPECT_NO_THROW(outcome.get());
    EXPECT_EQ(opens, 0);
    EXPECT_EQ(retries, 0);
    EXPECT_EQ(finishes, 32);
    EXPECT_TRUE(io.stopped());
}
