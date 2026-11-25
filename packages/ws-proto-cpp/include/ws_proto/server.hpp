#pragma once

#include "ws_proto/client.hpp"

namespace ws_proto {
namespace detail {
struct ServerConnection;
struct InboundStream;
}

// One pending read per stream. An empty optional is the peer's HALF_CLOSE.
class ServerRead {
public:
    explicit ServerRead(std::shared_ptr<detail::InboundStream> stream) : stream_(std::move(stream)) {}
    ServerRead(const ServerRead&) = delete;
    ~ServerRead();
    bool await_ready() const;
    void await_suspend(std::coroutine_handle<> continuation);
    Result<std::optional<std::string>> await_resume();
protected:
    std::shared_ptr<detail::InboundStream> stream_;
};

class ServerCall {
public:
    const Metadata& headers() const;
    std::optional<Clock::time_point> deadline() const;
    Signal cancellation() const;
    std::function<void()> canceller() const;
    ServerRead read() const { return ServerRead(stream_); }
    Result<void> send_headers(const Metadata& headers);
    Result<void> write(std::string message);
    void finish(Status status = {}, Metadata trailers = {});
private:
    friend class ServerSession;
    friend struct detail::ServerConnection;
    explicit ServerCall(std::shared_ptr<detail::InboundStream> stream) : stream_(std::move(stream)) {}
    std::shared_ptr<detail::InboundStream> stream_;
};

template<class Request, class Response>
class TypedServerCall : public ServerCall {
    class Read {
    public:
        explicit Read(ServerCall call) : raw_(call.read()), call_(std::move(call)) {}
        bool await_ready() const { return raw_.await_ready(); }
        void await_suspend(std::coroutine_handle<> continuation) { raw_.await_suspend(continuation); }
        Result<std::optional<Request>> await_resume() {
            auto bytes = raw_.await_resume();
            if (!bytes) return std::unexpected(std::move(bytes.error()));
            if (!*bytes) return std::nullopt;
            Request request;
            if (!request.ParseFromString(**bytes)) {
                Status status{Code::invalid_argument, "invalid protobuf request"};
                call_.finish(status);
                return std::unexpected(std::move(status));
            }
            return std::optional(std::move(request));
        }
    private:
        ServerRead raw_;
        ServerCall call_;
    };
public:
    explicit TypedServerCall(ServerCall call) : ServerCall(std::move(call)) {}
    auto read() const { return Read(*this); }
    Result<void> write(const Response& response) {
        std::string message;
        if (response.IsInitialized() && response.SerializeToString(&message))
            return ServerCall::write(std::move(message));
        Status status{Code::internal, "uninitialized protobuf response"};
        finish(status);
        return std::unexpected(std::move(status));
    }
};

class ServerSession {
public:
    using Handler = std::function<void(ServerCall)>;
    ServerSession(std::function<bool(std::string)> send, std::function<void()> close,
                  Limits limits = {}, Now now = Clock::now);
    ServerSession(const ServerSession&) = delete;
    ServerSession& operator=(const ServerSession&) = delete;
    ~ServerSession();
    // Returns false for a duplicate path, an empty path or an empty handler.
    bool register_method(std::string path, MethodKind kind, Handler handler);
    template<class Rpc, class HandlerFn>
    bool register_method(Method<Rpc> method, HandlerFn handler) {
        using Call = TypedServerCall<typename Rpc::RequestType, typename Rpc::ResponseType>;
        return register_method(std::string(method.path), Rpc::kind,
            [handler = std::move(handler)](ServerCall call) mutable { handler(Call(std::move(call))); });
    }
    void opened(std::string_view negotiated_protocol);
    void receive(std::string_view message);
    void disconnected();
    void expire();
    void expire(Clock::time_point now);
    std::optional<Clock::time_point> next_deadline() const;
    bool is_open() const;
private:
    std::shared_ptr<detail::ServerConnection> connection_;
};
} // namespace ws_proto
