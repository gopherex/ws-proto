#include <ws_proto/testing.hpp>
#include <google/protobuf/wrappers.pb.h>
#include <gtest/gtest.h>

using namespace ws_proto;
using Pipe = ws_proto::testing::Loopback;

TEST(Server, ResponseMetadataAndQueuedDelivery) {
    Pipe pipe;
    bool called = false;
    ASSERT_TRUE(pipe.server.register_method("/echo", MethodKind::bidi, [&](ServerCall call) {
        called = true;
        EXPECT_EQ(call.headers().at("authorization"), "token");
        EXPECT_EQ(call.headers().at("komeet-layer"), "42");
        EXPECT_TRUE(call.send_headers({{"initial", "yes"}}));
        EXPECT_TRUE(call.write("reply"));
        call.finish({Code::permission_denied, "denied", {"details"}}, {{"final", "yes"}});
    }));
    pipe.open();
    auto rpc = pipe.client.start("/echo", {.headers={{"authorization","token"},{"komeet-layer","42"}}}, MethodKind::bidi);
    EXPECT_FALSE(called);
    pipe.drain();
    EXPECT_TRUE(called);
    EXPECT_EQ(rpc.headers().at("initial"), "yes");
    EXPECT_EQ(rpc.read().await_resume().value().value(), "reply");
    EXPECT_EQ(rpc.read().await_resume().error().details, std::vector<std::string>{"details"});
    EXPECT_EQ(rpc.trailers().at("final"), "yes");
}

using Dir = ws_proto::testing::Direction;
using namespace std::chrono_literals;

TEST(Server, DelayedHandshakeUsesRemainingDeadlineAndServerCancels) {
    Pipe pipe;
    std::optional<ServerCall> server_call;
    ASSERT_TRUE(pipe.server.register_method("/slow", MethodKind::bidi, [&](ServerCall call) { server_call = call; }));
    auto deadline = pipe.now() + 100ms;
    auto rpc = pipe.client.start("/slow", {.deadline=deadline}, MethodKind::bidi);
    pipe.advance(30ms);
    pipe.open();
    pipe.drain();
    ASSERT_TRUE(server_call);
    EXPECT_EQ(server_call->deadline(), deadline);
    EXPECT_EQ(pipe.server.next_deadline(), deadline);
    pipe.hold(Dir::to_server);
    pipe.advance(70ms);
    EXPECT_TRUE(server_call->cancellation().ready());
    EXPECT_EQ(server_call->read().await_resume().error().code, Code::deadline_exceeded);
    EXPECT_EQ(rpc.read().await_resume().error().code, Code::deadline_exceeded);
    EXPECT_FALSE(pipe.server.next_deadline());
}

TEST(Server, NeverOpenedAndRejectedProtocol) {
    Pipe pipe;
    auto pending = pipe.client.start("/never", {.deadline=pipe.now()+1s}, MethodKind::bidi);
    pipe.drain();
    EXPECT_FALSE(pending.read().await_ready());
    pipe.advance(1s);
    EXPECT_EQ(pending.read().await_resume().error().code, Code::deadline_exceeded);
    auto waiting = pipe.client.start("/wrong", {}, MethodKind::unary);
    pipe.open("unrelated");
    pipe.drain();
    EXPECT_FALSE(pipe.server.is_open());
    EXPECT_EQ(waiting.read().await_resume().error().code, Code::unavailable);
}

TEST(Server, DelaysRawFramesAndDisconnectDuringStream) {
    Pipe pipe;
    std::optional<ServerCall> handler;
    pipe.server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { handler = call; });
    pipe.open();
    pipe.drain();
    pipe.delay(Dir::to_server, 10ms);
    pipe.inject(Dir::to_server, std::string(1, char(0xff)));
    auto rpc = pipe.client.start("/live", {}, MethodKind::bidi);
    pipe.drain();
    EXPECT_FALSE(handler);
    pipe.advance(10ms);
    pipe.drain();
    ASSERT_TRUE(handler);
    pipe.hold(Dir::to_client);
    EXPECT_TRUE(handler->write("one"));
    pipe.drain();
    EXPECT_FALSE(rpc.read().await_ready());
    pipe.hold(Dir::to_client, false);
    pipe.drain();
    EXPECT_EQ(rpc.read().await_resume().value().value(), "one");
    pipe.disconnect();
    pipe.drain();
    EXPECT_TRUE(handler->cancellation().ready());
    EXPECT_EQ(handler->read().await_resume().error().code, Code::unavailable);
    EXPECT_EQ(rpc.read().await_resume().error().code, Code::unavailable);
    EXPECT_FALSE(pipe.inject(Dir::to_server, "closed"));
}

TEST(Server, ResponseFlowControlOrdersEndAfterBlockedMessages) {
    Pipe pipe({.initial_window=2});
    std::optional<ServerCall> handler;
    pipe.server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { handler = call; });
    pipe.open();
    auto rpc = pipe.client.start("/live", {}, MethodKind::bidi);
    pipe.drain();
    ASSERT_TRUE(handler);
    EXPECT_TRUE(handler->write("abc"));
    EXPECT_TRUE(handler->write("d"));
    handler->finish({}, {{"done","yes"}});
    pipe.drain();
    EXPECT_FALSE(rpc.finished().ready());
    EXPECT_EQ(rpc.read().await_resume().value().value(), "abc");
    pipe.drain();
    EXPECT_TRUE(rpc.finished().ready());
    EXPECT_EQ(rpc.read().await_resume().value().value(), "d");
    EXPECT_FALSE(rpc.read().await_resume().value());
    EXPECT_EQ(rpc.trailers().at("done"), "yes");
}

TEST(Server, ReturnsRequestCreditOnReadAndCancelsOnlyOneStream) {
    Pipe pipe({.initial_window=2});
    std::vector<ServerCall> handlers;
    pipe.server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { handlers.push_back(call); });
    pipe.open();
    auto rpc = pipe.client.start("/live", {}, MethodKind::bidi);
    auto sibling = pipe.client.start("/live", {}, MethodKind::bidi);
    EXPECT_TRUE(rpc.send("abc"));
    EXPECT_TRUE(rpc.send("d"));
    rpc.close_send();
    pipe.drain();
    ASSERT_EQ(handlers.size(), 2);
    EXPECT_EQ(handlers[0].read().await_resume().value().value(), "abc");
    EXPECT_FALSE(handlers[0].read().await_ready());
    pipe.drain();
    EXPECT_EQ(handlers[0].read().await_resume().value().value(), "d");
    EXPECT_FALSE(handlers[0].read().await_resume().value());
    rpc.cancel();
    pipe.drain();
    EXPECT_TRUE(handlers[0].cancellation().ready());
    EXPECT_FALSE(handlers[1].cancellation().ready());
    EXPECT_TRUE(handlers[1].write("sibling"));
}

TEST(Server, UnknownMethodAndInvalidCardinalityTerminateOnlyThatCall) {
    Pipe pipe;
    auto ignore = [](ServerCall) {};
    EXPECT_TRUE(pipe.server.register_method("/unary", MethodKind::unary, ignore));
    EXPECT_FALSE(pipe.server.register_method("/unary", MethodKind::bidi, ignore));
    pipe.open();
    auto unknown = pipe.client.start("/unknown", {}, MethodKind::bidi);
    auto empty = pipe.client.start("/unary", {}, MethodKind::unary);
    empty.close_send();
    auto extra = pipe.client.start("/unary", {}, MethodKind::unary);
    ASSERT_TRUE(extra.send("1"));
    ASSERT_TRUE(extra.send("2"));
    pipe.drain();
    EXPECT_EQ(unknown.read().await_resume().error().code, Code::unimplemented);
    EXPECT_EQ(empty.read().await_resume().error().code, Code::invalid_argument);
    EXPECT_EQ(extra.read().await_resume().error().code, Code::invalid_argument);
    EXPECT_TRUE(pipe.server.is_open());
}

TEST(Server, DeadlineInterruptsAnEndWaitingForResponseCredit) {
    Pipe pipe({.initial_window=1});
    std::optional<ServerCall> handler;
    pipe.server.register_method("/stream", MethodKind::bidi, [&](ServerCall call) { handler = call; });
    pipe.open();
    auto rpc = pipe.client.start("/stream", {.deadline=pipe.now()+10ms}, MethodKind::bidi);
    pipe.drain();
    ASSERT_TRUE(handler);
    ASSERT_TRUE(handler->write("first"));
    ASSERT_TRUE(handler->write("blocked"));
    handler->finish();
    pipe.server.expire(*handler->deadline());
    pipe.drain();
    EXPECT_TRUE(handler->cancellation().ready());
    EXPECT_EQ(rpc.read().await_resume().value().value(), "first");
    EXPECT_EQ(rpc.read().await_resume().error().code, Code::deadline_exceeded);
}

TEST(Server, RejectsSecondHeadersAndExtraUnaryResponse) {
    Pipe pipe;
    std::optional<ServerCall> handler;
    pipe.server.register_method("/one", MethodKind::unary, [&](ServerCall call) { handler = call; });
    pipe.open();
    auto rpc = pipe.client.start("/one", {}, MethodKind::unary);
    rpc.send("request").value();
    pipe.drain();
    ASSERT_TRUE(handler);
    ASSERT_TRUE(handler->read().await_ready());
    EXPECT_EQ(handler->read().await_resume().value().value(), "request");
    EXPECT_TRUE(handler->send_headers({}));
    EXPECT_FALSE(handler->send_headers({}));
    ASSERT_TRUE(handler->write("first"));
    EXPECT_FALSE(handler->write("extra"));
    handler->finish();
    pipe.drain();
    EXPECT_EQ(rpc.read().await_resume().value().value(), "first");
}

TEST(Server, FailedHostWriteClosesOnceAndCancelsOtherHandlers) {
    std::vector<std::string> frames;
    std::vector<ServerCall> calls;
    int closes = 0;
    ServerSession server([](std::string) { return false; }, [&] { ++closes; });
    Session client([&](std::string frame) { frames.push_back(std::move(frame)); return true; }, [] {});
    server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { calls.push_back(call); });
    server.opened("wsrpc.v1");
    client.opened("wsrpc.v1");
    auto first = client.start("/live", {}, MethodKind::bidi);
    auto second = client.start("/live", {}, MethodKind::bidi);
    for (const auto& frame : frames) server.receive(frame);
    ASSERT_EQ(calls.size(), 2);
    EXPECT_EQ(calls[0].write("no socket").error().code, Code::unavailable);
    EXPECT_TRUE(calls[1].cancellation().ready());
    EXPECT_EQ(closes, 1);
    server.disconnected();
    EXPECT_EQ(closes, 1);
}

TEST(Server, TypedMethodRejectsInvalidRequestWithoutClosingSession) {
    using Text = google::protobuf::StringValue;
    using Echo = Rpc<Text, Text, true, true>;
    Pipe pipe;
    std::optional<TypedServerCall<Text, Text>> handler;
    pipe.server.register_method(Method<Echo>{"/typed"}, [&](auto call) { handler = call; });
    pipe.open();
    auto rpc = pipe.client.start("/typed", {}, MethodKind::bidi);
    ASSERT_TRUE(rpc.send(std::string(1, char(0xff))));
    pipe.drain();
    ASSERT_TRUE(handler);
    auto read = handler->read();
    ASSERT_TRUE(read.await_ready());
    EXPECT_EQ(read.await_resume().error().code, Code::invalid_argument);
    pipe.drain();
    EXPECT_EQ(rpc.read().await_resume().error().code, Code::invalid_argument);
    EXPECT_TRUE(pipe.server.is_open());
}

TEST(Server, BoundsUnreadRequestsAndCreditBlockedResponses) {
    Pipe pipe({.initial_window = 1, .max_send_buffer = 1, .max_queued_messages = 1});
    std::vector<ServerCall> handlers;
    pipe.server.register_method("/live", MethodKind::bidi, [&](ServerCall call) { handlers.push_back(call); });
    pipe.open();
    auto unread = pipe.client.start("/live", {}, MethodKind::bidi);
    // Empty messages consume no credit but still occupy the receive queue.
    ASSERT_TRUE(unread.send(""));
    ASSERT_TRUE(unread.send(""));
    auto blocked = pipe.client.start("/live", {}, MethodKind::bidi);
    pipe.drain();
    ASSERT_EQ(handlers.size(), 2);
    EXPECT_EQ(unread.read().await_resume().error().code, Code::resource_exhausted);
    EXPECT_TRUE(handlers[1].write("a"));
    EXPECT_EQ(handlers[1].write("too large").error().code, Code::resource_exhausted);
    pipe.drain();
    EXPECT_EQ(blocked.read().await_resume().value().value(), "a");
    EXPECT_EQ(blocked.read().await_resume().error().code, Code::resource_exhausted);
}
