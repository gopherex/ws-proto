#include "ws_proto/client.hpp"
#include "transport/transport.pb.h"

#include <gtest/gtest.h>

namespace wire = wsproto::transport::v1;
using namespace ws_proto;

class SessionTest : public testing::Test {
protected:
    std::vector<wire::Frame> sent;
    int closes = 0;
    Session session{[this](std::string bytes) { return sent.emplace_back().ParseFromString(bytes); },
                    [this] { ++closes; }, Limits{.initial_window = 2}};

    void SetUp() override { session.opened("wsrpc.v1"); }

    void deliver(wire::Kind kind, std::uint32_t id, std::string data = {}) {
        wire::Frame frame;
        frame.set_stream_id(id);
        frame.set_kind(kind);
        frame.set_window(2);
        frame.set_payload(std::move(data));
        session.receive(frame.SerializeAsString());
    }
};

TEST_F(SessionTest, HalfCloseWaitsForCreditAndRunsOnce) {
    auto call = session.start("/test.Service/Bidi", {}, false);
    ASSERT_TRUE(call.send("abc"));
    ASSERT_TRUE(call.send("d"));
    call.close_send();
    call.close_send();
    ASSERT_EQ(sent.size(), 2);
    EXPECT_EQ(sent.back().payload(), "abc");
    deliver(wire::KIND_WINDOW_UPDATE, 1);
    ASSERT_EQ(sent.size(), 4);
    EXPECT_EQ(sent[2].payload(), "d");
    EXPECT_EQ(sent[3].kind(), wire::KIND_HALF_CLOSE);
    EXPECT_FALSE(call.send("e"));
}

TEST_F(SessionTest, CancelDropsBlockedWriteAndLeavesSiblingActive) {
    auto call = session.start("/a", {}, false);
    ASSERT_TRUE(call.send("abc"));
    ASSERT_TRUE(call.send("d"));
    call.close_send();
    auto sibling = session.start("/b", {}, true);
    call.cancel();
    EXPECT_EQ(sent.back().kind(), wire::KIND_RST);
    EXPECT_EQ(sent.back().stream_id(), 1);
    auto count = sent.size();
    deliver(wire::KIND_WINDOW_UPDATE, 1);
    EXPECT_EQ(sent.size(), count);
    EXPECT_FALSE(sibling.read().await_ready());
    session.disconnected();
    EXPECT_EQ(sibling.read().await_resume().error().code, Code::unavailable);
}

TEST_F(SessionTest, StreamingCreditFollowsConsumptionAndEndDrainsMessages) {
    auto call = session.start("/a", {}, false);
    deliver(wire::KIND_MSG, 1, "abc");
    ASSERT_EQ(sent.size(), 1);
    EXPECT_EQ(call.read().await_resume().value().value(), "abc");
    ASSERT_EQ(sent.size(), 2);
    EXPECT_EQ(sent.back().kind(), wire::KIND_WINDOW_UPDATE);
    EXPECT_EQ(sent.back().window(), 3);
    deliver(wire::KIND_MSG, 1, "last");
    deliver(wire::KIND_END, 1);
    EXPECT_EQ(call.read().await_resume().value().value(), "last");
    EXPECT_FALSE(call.read().await_resume().value());
}

TEST_F(SessionTest, SingleResponseWaitsForEndAndDetectsSecondMessage) {
    auto call = session.start("/a", {}, true);
    deliver(wire::KIND_MSG, 1, "oversized");
    EXPECT_FALSE(call.read().await_ready());
    EXPECT_EQ(sent.back().kind(), wire::KIND_WINDOW_UPDATE);
    deliver(wire::KIND_MSG, 1, "extra");
    EXPECT_EQ(call.read().await_resume().error().code, Code::internal);
    EXPECT_EQ(sent.back().kind(), wire::KIND_RST);
}

TEST_F(SessionTest, DeadlinesExpireOnlyTheCorrespondingCalls) {
    Options opts;
    opts.deadline = Clock::now() + std::chrono::seconds(5);
    auto call = session.start("/a", opts, true);
    auto sibling = session.start("/b", {}, true);
    EXPECT_EQ(session.next_deadline(), opts.deadline);
    EXPECT_TRUE(sent.front().headers().contains("ws-timeout-ms"));
    session.expire(*opts.deadline);
    EXPECT_EQ(call.read().await_resume().error().code, Code::deadline_exceeded);
    EXPECT_FALSE(session.next_deadline());
    EXPECT_FALSE(sibling.read().await_ready());
}

TEST_F(SessionTest, ConnectionEndPreservesStatusForFutureCallsAndClosesOnce) {
    auto call = session.start("/a", {}, false);
    wire::Frame end;
    end.set_kind(wire::KIND_CONN_END);
    end.mutable_status()->set_code(7);
    end.mutable_status()->add_details("detail");
    session.receive(end.SerializeAsString());
    session.disconnected();
    EXPECT_EQ(closes, 1);
    EXPECT_EQ(call.read().await_resume().error().details.at(0), "detail");
    EXPECT_EQ(session.start("/b", {}, true).read().await_resume().error().code, Code::permission_denied);
}

TEST_F(SessionTest, BadSubprotocolAndRejectedWriteTerminateSession) {
    session.opened("invalid");
    EXPECT_EQ(closes, 1);
    EXPECT_EQ(session.start("/a", {}, true).read().await_resume().error().code, Code::failed_precondition);
    Session refused([](std::string) { return false; }, [this] { ++closes; });
    refused.opened("wsrpc.v1");
    EXPECT_EQ(refused.start("/a", {}, true).read().await_resume().error().code, Code::unavailable);
    EXPECT_EQ(closes, 2);
}

TEST_F(SessionTest, OverflowResetsOnlyOffendingStream) {
    auto call = session.start("/a", {}, false);
    auto sibling = session.start("/b", {}, false);
    deliver(wire::KIND_MSG, 1, "oversized");
    deliver(wire::KIND_MSG, 1, "without credit");
    EXPECT_EQ(call.read().await_resume().error().code, Code::resource_exhausted);
    EXPECT_FALSE(sibling.read().await_ready());
}

TEST_F(SessionTest, SingleResponseCanBeAnEmptyProtobufMessage) {
    auto call = session.start("/a", {}, true);
    deliver(wire::KIND_MSG, 1);
    EXPECT_FALSE(call.read().await_ready());
    deliver(wire::KIND_END, 1);
    EXPECT_EQ(call.read().await_resume().value().value(), "");
    auto missing = session.start("/b", {}, true);
    deliver(wire::KIND_END, 3);
    EXPECT_EQ(missing.read().await_resume().error().code, Code::internal);
}
