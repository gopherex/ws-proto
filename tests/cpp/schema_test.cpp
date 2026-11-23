#include "service.ws.pb.h"
#include "well_known.ws.pb.h"

#include "transport/transport.pb.h"
#include <gtest/gtest.h>

#include <concepts>
#include <utility>

using Service = naming::api::KeywordServiceClient;
using Request = naming::v1::Outer::Type;
using Response = naming::v1::Outer::WhenMessage;
static_assert(std::same_as<decltype(std::declval<Service>().call_Class(std::declval<Request>())),
                           ws_proto::Rpc<Request, Response, false, false>>);
static_assert(std::same_as<decltype(std::declval<wiretest::WellKnownServiceClient>().call_Empty({})),
                           ws_proto::Rpc<google::protobuf::Empty, google::protobuf::Empty, false, false>>);

using namespace ws_proto;
namespace wire = wsproto::transport::v1;

class GeneratedClientTest : public testing::Test {
protected:
    std::vector<wire::Frame> frames;
    Session session{[&](std::string data) { return frames.emplace_back().ParseFromString(data); }, [] {}};
    void SetUp() override { session.opened("wsrpc.v1"); }
    void deliver(wire::Kind kind, std::string data = {}) {
        wire::Frame frame;
        frame.set_stream_id(1);
        frame.set_kind(kind);
        frame.set_payload(std::move(data));
        session.receive(frame.SerializeAsString());
    }
};

TEST_F(GeneratedClientTest, UnaryMiddlewareSeesTypedRequestsAndCanReplaceOrShortCircuitResponses) {
    auto inner = intercept(session, [](auto method, auto next, Options options, const Request& request) {
        EXPECT_EQ(method.path, "/naming.api.KeywordService/Class");
        EXPECT_EQ(method.kind, MethodKind::unary);
        options.headers["auth"] = "token";
        auto copy = request;
        copy.set_value(request.value() + "/inner");
        auto rpc = next(std::move(options), copy);
        rpc.map_responses([](Response& response) { response.set_value(response.value() + "/inner"); });
        return rpc;
    });
    auto outer = intercept(inner, [](auto method, auto next, Options options, const Request& request) {
        using Rpc = typename decltype(method)::CallType;
        if (request.value() == "cached") {
            Response cached;
            cached.set_value("hit");
            return Rpc::completed({cached}, std::move(options), {}, {{"source", "cache"}}, {{"done", "yes"}});
        }
        if (request.value() == "reject")
            return Rpc::completed({}, std::move(options), {Code::permission_denied, "denied"});
        auto rpc = next(std::move(options), request);
        rpc.map_responses([](Response& response) { response.set_value(response.value() + "/outer"); });
        return rpc;
    });
    naming::api::KeywordServiceClientT client(outer);
    Request request;
    request.set_value("cached");
    std::vector<Metadata> metadata;
    Options options;
    options.on_header = options.on_trailer = [&](const Metadata& m) { metadata.push_back(m); };
    auto cached = client.call_Class(request, options);
    EXPECT_EQ(cached.result().await_resume()->value(), "hit");
    EXPECT_EQ(metadata, (std::vector<Metadata>{{{"source", "cache"}}, {{"done", "yes"}}}));
    request.set_value("reject");
    EXPECT_EQ(client.call_Class(request).result().await_resume().error().code, Code::permission_denied);
    EXPECT_TRUE(frames.empty());
    request.set_value("wire");
    auto rpc = client.call_Class(request);
    ASSERT_EQ(frames.size(), 3);
    EXPECT_EQ(frames[0].headers().at("auth"), "token");
    Request sent;
    ASSERT_TRUE(sent.ParseFromString(frames[1].payload()));
    EXPECT_EQ(sent.value(), "wire/inner");
    EXPECT_EQ(request.value(), "wire");
    Response response;
    response.set_value("result");
    deliver(wire::KIND_MSG, response.SerializeAsString());
    deliver(wire::KIND_END);
    EXPECT_EQ(rpc.result().await_resume()->value(), "result/inner/outer");
}

TEST_F(GeneratedClientTest, StreamingMiddlewareTransformsEveryMessageInOrder) {
    auto add = [](std::string suffix) {
        return [suffix](auto method, auto next, Options options) {
            EXPECT_EQ(method.kind, MethodKind::bidi);
            auto rpc = next(std::move(options));
            rpc.map_requests([suffix](naming::api::Local& m) { m.set_value(m.value() + suffix); });
            rpc.map_responses([suffix](naming::v1::String& m) { m.set_value(m.value() + suffix); });
            return rpc;
        };
    };
    auto inner = intercept(session, add("I"));
    auto outer = intercept(inner, add("O"));
    naming::api::KeywordServiceClientT client(outer);
    auto rpc = client.call_Bidi();
    for (std::string value : {"1", "2"}) {
        naming::api::Local request;
        request.set_value(value);
        ASSERT_TRUE(rpc.write(request));
        naming::api::Local wire_request;
        ASSERT_TRUE(wire_request.ParseFromString(frames.back().payload()));
        EXPECT_EQ(wire_request.value(), value + "OI");
        naming::v1::String reply;
        reply.set_value(value);
        deliver(wire::KIND_MSG, reply.SerializeAsString());
        auto item = rpc.read().await_resume();
        ASSERT_TRUE(item && *item);
        EXPECT_EQ((**item).value(), value + "IO");
    }
}

using UploadItem = Result<std::optional<naming::v1::StringMessage>>;
struct ReadyItem {
    UploadItem value;
    bool await_ready() const { return true; }
    void await_suspend(std::coroutine_handle<>) const {}
    UploadItem await_resume() { return std::move(value); }
};
struct IdleItem : SignalWait {
    int& released;
    IdleItem(Signal stop, int& released) : SignalWait(std::move(stop)), released(released) {}
    ~IdleItem() { ++released; }
    Result<std::optional<naming::api::Local>> await_resume() { return std::nullopt; }
};

TEST_F(GeneratedClientTest, RequestSourceEofHalfClosesAndGeneratedUploadReturnsLastResponse) {
    naming::api::KeywordServiceClient client(session);
    auto upload = client.call_Upload();
    upload.write_from([i = 0](Signal) mutable -> ReadyItem {
        if (++i == 3) return {std::nullopt};
        naming::v1::StringMessage item;
        item.set_value(std::to_string(i));
        return {std::move(item)};
    });
    ASSERT_EQ(frames.size(), 4);
    EXPECT_EQ(frames.back().kind(), wire::KIND_HALF_CLOSE);
    for (std::string value : {"first", "last"}) {
        naming::api::Local response;
        response.set_value(value);
        deliver(wire::KIND_MSG, response.SerializeAsString());
    }
    EXPECT_FALSE(upload.result().await_ready());
    deliver(wire::KIND_END);
    EXPECT_EQ(upload.result().await_resume()->value(), "last");
}

TEST_F(GeneratedClientTest, EarlyEndResetOrCancellationWakesAnIdleSource) {
    for (auto kind : {wire::KIND_END, wire::KIND_RST, wire::KIND_UNSPECIFIED}) {
        auto rpc = naming::api::KeywordServiceClient(session).call_Bidi();
        auto id = frames.back().stream_id();
        int released = 0;
        rpc.write_from([&](Signal stop) { return IdleItem(stop, released); });
        EXPECT_EQ(released, 0);
        auto sent = frames.size();
        wire::Frame end;
        end.set_stream_id(id);
        end.set_kind(kind);
        if (kind == wire::KIND_UNSPECIFIED) rpc.cancel();
        else session.receive(end.SerializeAsString());
        EXPECT_EQ(released, 1);
        EXPECT_EQ(frames.size(), sent + (kind == wire::KIND_UNSPECIFIED));
    }
}

TEST_F(GeneratedClientTest, RequestSourceFailureResetsAndSurfacesOriginalStatus) {
    auto rpc = naming::api::KeywordServiceClient(session).call_Upload();
    rpc.write_from([](Signal) -> ReadyItem { return {std::unexpected(Status{Code::aborted, "source failed"})}; });
    EXPECT_EQ(frames.back().kind(), wire::KIND_RST);
    EXPECT_EQ(rpc.result().await_resume().error().message, "source failed");
}

TEST_F(GeneratedClientTest, HeaderOnlyConsumerHandlesInitialWriteFailure) {
    Session refusing([](std::string data) {
        wire::Frame frame;
        return frame.ParseFromString(data) && frame.kind() != wire::KIND_MSG;
    }, [] {});
    refusing.opened("wsrpc.v1");
    auto rpc = naming::api::KeywordServiceClient(refusing).call_Class({});
    EXPECT_EQ(rpc.result().await_resume().error().code, Code::unavailable);
}


TEST_F(GeneratedClientTest, LocalHalfCloseStopsIdleProducerWhileReaderRemainsActive) {
    auto rpc = naming::api::KeywordServiceClient(session).call_Bidi();
    int released = 0;
    rpc.write_from([&](Signal stop) { return IdleItem(stop, released); });
    rpc.close_send();
    EXPECT_EQ(released, 1);
    EXPECT_FALSE(rpc.finished().ready());
    EXPECT_EQ(frames.back().kind(), wire::KIND_HALF_CLOSE);
}

TEST_F(GeneratedClientTest, StatefulResponseInterceptorSurvivesSuccessiveReads) {
    auto channel = intercept(session, [](auto method, auto, Options options, const auto&) {
        using Rpc = typename decltype(method)::CallType;
        auto rpc = Rpc::completed({{}, {}}, std::move(options));
        rpc.map_responses([i = 0](naming::v1::StringMessage& item) mutable { item.set_value(std::to_string(++i)); });
        return rpc;
    });
    naming::api::KeywordServiceClientT client(channel);
    auto rpc = client.call_Init({});
    for (auto expected : {"1", "2"}) {
        auto item = rpc.read().await_resume();
        ASSERT_TRUE(item && *item);
        EXPECT_EQ((*item)->value(), expected);
    }
    EXPECT_FALSE(*rpc.read().await_resume());
    EXPECT_TRUE(frames.empty());
}
