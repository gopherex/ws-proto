#include <ws_proto/asio.hpp>
#include <ws_proto/testing.hpp>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <google/protobuf/wrappers.pb.h>
#include <gtest/gtest.h>

namespace net = boost::asio;
namespace wa = ws_proto::asio;
using namespace ws_proto;
using String = google::protobuf::StringValue;
using Duplex = Rpc<String, String, true, true>;
using Unary = Rpc<String, String, false, false>;

TEST(Asio, CoroutineReadsMessageAndPreservesStatusAndDetails) {
    ws_proto::testing::Loopback pipe;
    pipe.server.register_method("/live", MethodKind::bidi, [](ServerCall call) {
        String s;
        s.set_value("delivered");
        EXPECT_TRUE(call.write(s.SerializeAsString()));
        call.finish({Code::unauthenticated, "token expired", {"details"}});
    });
    pipe.open();
    auto rpc = pipe.client.invoke(Method<Duplex>{"/live"}, {});
    net::io_context io;
    auto future = net::co_spawn(io, [&]() -> net::awaitable<void> {
        auto msg = co_await wa::async_read(rpc, net::use_awaitable);
        EXPECT_EQ(msg.value().value().value(), "delivered");
        auto end = co_await wa::async_read(rpc, net::use_awaitable);
        EXPECT_EQ(end.error().code, Code::unauthenticated);
        EXPECT_EQ(end.error().details.at(0), "details");
    }, net::use_future);
    io.poll();
    pipe.drain();
    io.run();
    EXPECT_NO_THROW(future.get());
}

TEST(Asio, CancellationOfPendingCoroutineResetsTheRpc) {
    ws_proto::testing::Loopback pipe;
    std::optional<ServerCall> server;
    pipe.server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { server = call; });
    pipe.open();
    auto rpc = pipe.client.invoke(Method<Duplex>{"/live"}, {});
    pipe.drain();
    net::io_context io;
    net::cancellation_signal cancel;
    auto future = net::co_spawn(io, [&]() -> net::awaitable<void> {
        auto value = co_await wa::async_read(rpc, net::bind_cancellation_slot(cancel.slot(), net::use_awaitable));
        EXPECT_EQ(value.error().code, Code::cancelled);
    }, net::use_future);
    io.poll();
    cancel.emit(net::cancellation_type::terminal);
    io.run();
    EXPECT_NO_THROW(future.get());
    pipe.drain();
    ASSERT_TRUE(server);
    EXPECT_TRUE(server->cancellation().ready());
}

TEST(Asio, CancelOneWaitAndDispatchReadyOperationsExactlyOnce) {
    net::io_context io;
    net::cancellation_signal cancel;
    SignalSource signal;
    int completed = 0;
    wa::async_wait(io.get_executor(), signal.signal(), net::bind_cancellation_slot(cancel.slot(),
        [&](Result<void> r) { EXPECT_EQ(r.error().code, Code::cancelled); ++completed; }));
    wa::async_wait(io.get_executor(), signal.signal(), [&](Result<void> r) { EXPECT_TRUE(r); ++completed; });
    cancel.emit(net::cancellation_type::all);
    io.poll();
    EXPECT_EQ(completed, 1);
    EXPECT_FALSE(signal.ready());
    signal.notify();
    wa::async_wait(io.get_executor(), signal.signal(), [&](Result<void> r) { EXPECT_TRUE(r); ++completed; });
    EXPECT_EQ(completed, 1);
    io.run();
    EXPECT_EQ(completed, 3);
}

TEST(Asio, ResultWinsAgainstPostedCancellationAndUsesCompletionExecutor) {
    net::io_context session, completion;
    net::cancellation_signal cancel;
    auto rpc = Unary::completed({String{}});
    int calls = 0;
    auto handler = net::bind_executor(completion.get_executor(), [&](Result<String> r) { EXPECT_TRUE(r); ++calls; });
    wa::async_result(session.get_executor(), rpc, net::bind_cancellation_slot(cancel.slot(), std::move(handler)));
    cancel.emit(net::cancellation_type::all);
    session.run();
    EXPECT_EQ(calls, 0);
    completion.run();
    EXPECT_EQ(calls, 1);
}

TEST(Asio, ServerReadCancellationNotifiesPeerAndEndsCoroutine) {
    ws_proto::testing::Loopback pipe;
    std::optional<ServerCall> server;
    pipe.server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { server = call; });
    pipe.open();
    auto rpc = pipe.client.start("/live", {}, MethodKind::bidi);
    pipe.drain();
    ASSERT_TRUE(server);
    net::io_context io;
    net::cancellation_signal cancel;
    int calls = 0;
    wa::async_read(io.get_executor(), *server,
        net::bind_cancellation_slot(cancel.slot(), [&](Result<std::optional<std::string>> result) {
            EXPECT_EQ(result.error().code, Code::cancelled);
            ++calls;
        }));
    cancel.emit(net::cancellation_type::terminal);
    io.run();
    EXPECT_EQ(calls, 1);
    EXPECT_TRUE(server->cancellation().ready());
    pipe.drain();
    EXPECT_EQ(rpc.read().await_resume().error().code, Code::cancelled);
}
