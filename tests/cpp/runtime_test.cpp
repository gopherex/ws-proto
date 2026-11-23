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
    EXPECT_EQ(session.start("/a", {}, true).read().await_resume().error().code, Code::unavailable);
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

TEST_F(SessionTest, DrainBeforeReportingRemoteTerminalStatus) {
    for (const auto kind : {wire::KIND_RST, wire::KIND_END, wire::KIND_CONN_END, wire::KIND_UNSPECIFIED}) {
        SCOPED_TRACE(static_cast<int>(kind));
        Session peer([](std::string) { return true; }, [] {});
        peer.opened("wsrpc.v1");
        auto call = peer.start("/download", {}, false);
        wire::Frame frame;
        frame.set_kind(wire::KIND_MSG);
        frame.set_stream_id(1);
        for (auto payload : {"first", "second"}) {
            frame.set_payload(payload);
            peer.receive(frame.SerializeAsString());
        }
        frame.set_kind(kind);
        frame.mutable_status()->set_code(static_cast<int>(Code::permission_denied));
        if (kind == wire::KIND_CONN_END) frame.set_stream_id(0);
        if (kind == wire::KIND_UNSPECIFIED) peer.disconnected();
        else peer.receive(frame.SerializeAsString());
        for (auto expected : {"first", "second"}) {
            auto item = call.read().await_resume();
            ASSERT_TRUE(item && *item);
            EXPECT_EQ(**item, expected);
        }
        auto end = call.read().await_resume();
        ASSERT_FALSE(end);
        EXPECT_EQ(end.error().code, kind == wire::KIND_UNSPECIFIED ? Code::unavailable : Code::permission_denied);
    }
}

TEST_F(SessionTest, DefaultFrameLimitAllowsSixteenMiBPayloadAndSibling) {
    auto download = session.start("/download", {}, false);
    auto sibling = session.start("/sibling", {}, true);
    deliver(wire::KIND_MSG, 1, std::string(16 * 1024 * 1024, 0));
    auto data = download.read().await_resume();
    ASSERT_TRUE(data && *data);
    EXPECT_EQ((*data)->size(), 16 * 1024 * 1024);
    EXPECT_FALSE(sibling.read().await_ready());
}


namespace {
struct Task {
    struct promise_type {
        Task get_return_object() { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
    ~Task() { handle.destroy(); }
};
Task observe(Signal signal, int& hits) { co_await signal; ++hits; }
Task consume(Call& call, int& hits) { co_await call.read(); ++hits; }
}

TEST_F(SessionTest, DestroyingSuspendedReaderResetsTheCallExactlyOnce) {
    auto call = session.start("/download", {}, false);
    auto sibling = session.start("/sibling", {}, false);
    int resumed = 0;
    { auto reader = consume(call, resumed); }
    ASSERT_EQ(sent.size(), 3);
    EXPECT_EQ(sent.back().kind(), wire::KIND_RST);
    EXPECT_EQ(sent.back().stream_id(), 1);
    EXPECT_EQ(call.read().await_resume().error().code, Code::cancelled);
    deliver(wire::KIND_MSG, 1);
    EXPECT_EQ(resumed, 0);
    EXPECT_FALSE(sibling.read().await_ready());
}

TEST_F(SessionTest, CancellationSignalSupportsPrecancelAndSharedInFlightCalls) {
    SignalSource source;
    Options options{.cancellation = source.signal()};
    auto a = session.start("/a", options, true);
    auto b = session.start("/b", options, false);
    auto other = session.start("/other", {}, false);
    source.notify();
    source.notify();
    EXPECT_EQ(sent.size(), 5);
    EXPECT_TRUE(a.finished().ready());
    EXPECT_EQ(b.read().await_resume().error().code, Code::cancelled);
    EXPECT_FALSE(other.finished().ready());
    auto rejected = session.start("/c", options, true);
    EXPECT_EQ(rejected.read().await_resume().error().code, Code::cancelled);
    EXPECT_EQ(sent.size(), 5);
}

TEST_F(SessionTest, HeadersBeforeDataAndTrailersOnFailureNotifyOnce) {
    std::vector<Metadata> observed;
    Options options;
    options.on_header = options.on_trailer = [&](const Metadata& m) { observed.push_back(m); };
    auto call = session.start("/download", options, false);
    int leading = 0, trailing = 0;
    auto early = observe(call.headers_ready(), leading);
    { auto abandoned = observe(call.headers_ready(), leading); }
    auto end = observe(call.finished(), trailing);
    EXPECT_FALSE(call.headers_ready().ready());
    wire::Frame header;
    header.set_kind(wire::KIND_HEADER);
    header.set_stream_id(1);
    session.receive(header.SerializeAsString()); // explicitly empty
    EXPECT_TRUE(call.headers_ready().ready());
    EXPECT_FALSE(call.finished().ready());
    EXPECT_FALSE(call.read().await_ready());
    auto late = observe(call.headers_ready(), leading);
    (*header.mutable_headers())["ignored"] = "duplicate";
    session.receive(header.SerializeAsString());
    EXPECT_EQ(leading, 2);
    EXPECT_EQ(trailing, 0);
    deliver(wire::KIND_MSG, 1, "x");
    wire::Frame reset;
    reset.set_kind(wire::KIND_RST);
    reset.set_stream_id(1);
    reset.mutable_status()->set_message("reason without a status code");
    (*reset.mutable_headers())["why"] = "refused";
    session.receive(reset.SerializeAsString());
    session.disconnected();
    EXPECT_EQ(trailing, 1);
    EXPECT_EQ(call.trailers().at("why"), "refused");
    EXPECT_EQ(observed, (std::vector<Metadata>{{}, {{"why", "refused"}}}));
    EXPECT_EQ(**call.read().await_resume(), "x");
    auto status = call.read().await_resume().error();
    EXPECT_EQ(status.code, Code::cancelled);
    EXPECT_EQ(status.message, "reason without a status code");
}

TEST_F(SessionTest, MissingMetadataSettlesOnFirstMessageOrTermination) {
    for (bool send_data : {false, true}) {
        std::vector<std::string> events;
        Options options;
        options.on_header = [&](const Metadata& m) { EXPECT_TRUE(m.empty()); events.push_back("header"); };
        options.on_trailer = [&](const Metadata& m) { EXPECT_TRUE(m.empty()); events.push_back("trailer"); };
        auto call = session.start("/download", options, false);
        auto id = sent.back().stream_id();
        if (send_data) {
            deliver(wire::KIND_MSG, id);
            EXPECT_EQ(events, std::vector<std::string>{"header"});
        }
        deliver(wire::KIND_END, id);
        EXPECT_EQ(events, (std::vector<std::string>{"header", "trailer"}));
        EXPECT_TRUE(call.headers_ready().ready());
        EXPECT_TRUE(call.finished().ready());
    }
}

TEST_F(SessionTest, HandshakeFlushesCallsInOrderAndSkipsCancelledOrExpiredCalls) {
    std::vector<wire::Frame> frames;
    Session peer([&](std::string bytes) { return frames.emplace_back().ParseFromString(bytes); }, [] {});
    auto active = peer.start("/active", {}, false);
    ASSERT_TRUE(active.send("a"));
    auto cancelled = peer.start("/cancelled", {}, false);
    auto until = Clock::now() + std::chrono::seconds(2);
    auto expired = peer.start("/expired", {.deadline = until}, false);
    auto second = peer.start("/second", {}, true);
    cancelled.cancel();
    active.close_send();
    peer.expire(until);
    EXPECT_TRUE(frames.empty());
    peer.opened("wsrpc.v1");
    peer.opened("wsrpc.v1");
    ASSERT_EQ(frames.size(), 4);
    EXPECT_EQ(frames[0].method(), "/active");
    EXPECT_EQ(frames[1].payload(), "a");
    EXPECT_EQ(frames[2].method(), "/second");
    EXPECT_EQ(frames[3].kind(), wire::KIND_HALF_CLOSE);
    EXPECT_EQ(expired.read().await_resume().error().code, Code::deadline_exceeded);
    EXPECT_EQ(cancelled.read().await_resume().error().code, Code::cancelled);
    EXPECT_TRUE(peer.is_open());
    EXPECT_FALSE(second.finished().ready());
}

TEST_F(SessionTest, HostSeesPermanentHandshakeAndPeerErrorsAndTransientDisconnect) {
    for (int scenario = 0; scenario < 3; ++scenario) {
        SCOPED_TRACE(scenario);
        int ended = 0, closed = 0;
        Session peer([](std::string) { return true; }, [&] { ++closed; }, {}, [&](const SessionEnd& end) {
            ++ended;
            EXPECT_EQ(end.retryable, scenario == 2);
            EXPECT_EQ(end.status.code, scenario == 1 ? Code::permission_denied :
                                       Code::unavailable);
            if (scenario == 1) {
                EXPECT_EQ(end.status.message, "expired");
                EXPECT_EQ(end.status.details, std::vector<std::string>{"why"});
            }
        });
        if (scenario == 0) peer.opened("wrong");
        else if (scenario == 1) {
            peer.opened("wsrpc.v1");
            wire::Frame end;
            end.set_kind(wire::KIND_CONN_END);
            end.mutable_status()->set_code(7);
            end.mutable_status()->set_message("expired");
            end.mutable_status()->add_details("why");
            peer.receive(end.SerializeAsString());
        } else if (scenario == 2) peer.disconnected();
        peer.disconnected();
        EXPECT_FALSE(peer.is_open());
        EXPECT_TRUE(peer.end().has_value());
        EXPECT_EQ(ended, 1);
        EXPECT_EQ(closed, scenario == 2 ? 0 : 1);
    }
}

TEST_F(SessionTest, ExplicitFrameAndPreopenLimitsEndTheSession) {
    Limits limits{.max_frame_bytes = 20, .max_preopen_bytes = 50};
    for (bool open : {false, true}) {
        Session peer([](std::string) { return true; }, [] {}, limits);
        if (open) peer.opened("wsrpc.v1");
        auto a = peer.start("/a", {}, false);
        auto b = peer.start("/b", {}, false);
        if (open) peer.receive(std::string(21, 'a'));
        else EXPECT_FALSE(a.send(std::string(51, 'a')));
        ASSERT_TRUE(peer.end().has_value());
        EXPECT_FALSE(peer.end()->retryable);
        EXPECT_EQ(b.read().await_resume().error().code, open ? Code::resource_exhausted : Code::unavailable);
    }
}

TEST_F(SessionTest, ClientStreamingDrainsToLastAndPreservesStatusPrecedence) {
    for (int count : {0, 1, 3}) {
        for (auto code : {Code::ok, Code::not_found}) {
            auto call = session.start("/upload", {}, MethodKind::client_streaming);
            auto id = sent.back().stream_id();
            for (int i = 0; i < count; ++i) {
                deliver(wire::KIND_MSG, id, std::to_string(i));
                EXPECT_FALSE(call.read().await_ready());
                EXPECT_EQ(sent.back().kind(), wire::KIND_WINDOW_UPDATE);
            }
            wire::Frame end;
            end.set_kind(wire::KIND_END);
            end.set_stream_id(id);
            end.mutable_status()->set_code(static_cast<int>(code));
            session.receive(end.SerializeAsString());
            auto result = call.read().await_resume();
            if (code == Code::ok && count) {
                ASSERT_TRUE(result && *result);
                EXPECT_EQ(**result, std::to_string(count - 1));
            } else {
                ASSERT_FALSE(result);
                EXPECT_EQ(result.error().code, code == Code::ok ? Code::internal : code);
            }
        }
    }
}

TEST_F(SessionTest, DestroyingSessionNotifiesHostAndActiveCalls) {
    std::optional<SessionEnd> ended;
    std::optional<Call> call;
    {
        Session peer([](std::string) { return true; }, [] {}, {}, [&](const SessionEnd& reason) { ended = reason; });
        call.emplace(peer.start("/waiting", {}, false));
    }
    ASSERT_TRUE(ended);
    EXPECT_FALSE(ended->retryable);
    EXPECT_EQ(ended->status.code, Code::cancelled);
    EXPECT_EQ(call->read().await_resume().error().code, Code::cancelled);
}

TEST_F(SessionTest, ImmediateServerEndDuringHandshakeDropsUnsentRequests) {
    Session* target = nullptr;
    std::vector<wire::Kind> writes;
    Session peer([&](std::string bytes) {
        wire::Frame frame;
        if (!frame.ParseFromString(bytes)) return false;
        writes.push_back(frame.kind());
        if (frame.kind() == wire::KIND_OPEN) {
            frame.set_kind(wire::KIND_END);
            target->receive(frame.SerializeAsString());
        }
        return true;
    }, [] {});
    target = &peer;
    auto call = peer.start("/a", {}, false);
    ASSERT_TRUE(call.send("buffered"));
    call.close_send();
    peer.opened("wsrpc.v1");
    EXPECT_EQ(writes, std::vector<wire::Kind>{wire::KIND_OPEN});
    EXPECT_TRUE(call.finished().ready());
}

TEST_F(SessionTest, ConfiguredMessageCountLimitsEmptyFrames) {
    Session peer([](std::string) { return true; }, [] {}, Limits{.initial_window = 1, .max_queued_messages = 1});
    peer.opened("wsrpc.v1");
    auto a = peer.start("/send", {}, false);
    EXPECT_TRUE(a.send("x"));
    EXPECT_TRUE(a.send(""));
    EXPECT_EQ(a.send("").error().code, Code::resource_exhausted);
    auto b = peer.start("/receive", {}, false);
    wire::Frame frame;
    frame.set_kind(wire::KIND_MSG);
    frame.set_stream_id(3);
    for (int i = 0; i < 2; ++i) peer.receive(frame.SerializeAsString());
    EXPECT_EQ(b.read().await_resume().error().code, Code::resource_exhausted);
    EXPECT_TRUE(peer.is_open());
}

TEST(SignalTest, MovingSubscriptionsTransfersDetachmentAndNotifyAllowsReentry) {
    SignalSource source;
    auto signal = source.signal();
    int delivered = 0;
    auto original = signal.subscribe([&] { ++delivered; });
    Subscription transferred(std::move(original));
    Subscription last;
    last = std::move(transferred);
    original.reset();
    transferred.reset();
    Subscription removed;
    auto reentrant = signal.subscribe([&] { source.notify(); removed.reset(); });
    removed = signal.subscribe([&] { ADD_FAILURE() << "detached callback fired"; });
    source.notify();
    EXPECT_EQ(delivered, 1);
    EXPECT_TRUE(signal.ready());
    auto late = signal.subscribe([&] { ++delivered; });
    source.notify();
    EXPECT_EQ(delivered, 2);
}

TEST_F(SessionTest, ReentrantHostCannotWriteAfterReset) {
    std::optional<Call> call;
    std::vector<wire::Kind> writes;
    Session peer([&](std::string bytes) {
        wire::Frame frame;
        if (!frame.ParseFromString(bytes)) return false;
        writes.push_back(frame.kind());
        if (frame.kind() == wire::KIND_RST) {
            auto sent = call->send("late");
            EXPECT_FALSE(sent);
            if (!sent) EXPECT_EQ(sent.error().code, Code::cancelled);
            call->close_send();
        }
        return true;
    }, [] {});
    peer.opened("wsrpc.v1");
    call.emplace(peer.start("/bidi", {}, false));
    call->cancel();
    EXPECT_EQ(writes, (std::vector<wire::Kind>{wire::KIND_OPEN, wire::KIND_RST}));
}

TEST_F(SessionTest, HandshakeBudgetIncludesCreditBlockedMessagesAcrossCalls) {
    Limits limits{.initial_window = 1, .max_preopen_bytes = 100};
    Session peer([](std::string) { return true; }, [] {}, limits);
    auto a = peer.start("/a", {}, false);
    ASSERT_TRUE(a.send("x"));  // spends its only byte of credit
    ASSERT_TRUE(a.send(std::string(50, 'a')));
    a.cancel();                // releases both queues from the handshake budget
    auto b = peer.start("/b", {}, false);
    auto c = peer.start("/c", {}, false);
    ASSERT_TRUE(b.send("x"));
    ASSERT_TRUE(c.send("x"));
    ASSERT_TRUE(b.send(std::string(45, 'b')));
    EXPECT_FALSE(c.send(std::string(45, 'c')));
    ASSERT_TRUE(peer.end());
    EXPECT_EQ(peer.end()->status.code, Code::unavailable);
    EXPECT_TRUE(b.finished().ready());
}

TEST_F(SessionTest, HandshakeReleasesBudgetWhenBufferedMessagesBecomeSendable) {
    std::vector<wire::Frame> frames;
    Session peer([&](std::string bytes) { return frames.emplace_back().ParseFromString(bytes); }, [] {},
                 Limits{.initial_window = 1, .max_preopen_bytes = 90});
    auto a = peer.start("/a", {}, false);
    ASSERT_TRUE(a.send("x"));
    ASSERT_TRUE(a.send(std::string(40, 'a')));
    a.close_send();
    peer.opened("wsrpc.v1");
    wire::Frame credit;
    credit.set_stream_id(1);
    credit.set_kind(wire::KIND_WINDOW_UPDATE);
    credit.set_window(1);
    peer.receive(credit.SerializeAsString());
    ASSERT_EQ(frames.size(), 4);
    EXPECT_EQ(frames[2].payload(), std::string(40, 'a'));
    EXPECT_EQ(frames[3].kind(), wire::KIND_HALF_CLOSE);
    EXPECT_TRUE(peer.is_open());
}
