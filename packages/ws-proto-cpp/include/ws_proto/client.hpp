#pragma once

#include "ws_proto/signal.hpp"

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace ws_proto {

enum class Code : std::int32_t {
    ok, cancelled, unknown, invalid_argument, deadline_exceeded, not_found, already_exists,
    permission_denied, resource_exhausted, failed_precondition, aborted, out_of_range,
    unimplemented, internal, unavailable, data_loss, unauthenticated
};

struct Status {
    Code code = Code::ok;
    std::string message;
    std::vector<std::string> details;
};

using Metadata = std::map<std::string, std::string>;
using Clock = std::chrono::steady_clock;
template<class T> using Result = std::expected<T, Status>;

struct Options {
    Metadata headers;
    std::optional<Clock::time_point> deadline;
    Signal cancellation;
    std::function<void(const Metadata&)> on_header, on_trailer;
};

struct Limits {
    std::uint32_t initial_window = 256 * 1024;
    std::size_t max_send_buffer = 16 * 1024 * 1024;
    std::size_t max_frame_bytes = 0;
    std::size_t max_queued_messages = 0;
    std::size_t max_preopen_bytes = 4 * 1024 * 1024;
};

enum class MethodKind { unary, server_streaming, client_streaming, bidi };
struct SessionEnd {
    Status status;
    bool retryable;
};

namespace detail {
struct Connection;
struct Stream;

// Sending and half-closing do not own the lifetime of the RPC. In particular,
// a coroutine feeding requests can finish while the caller still waits for END.
class RequestSide {
public:
    explicit RequestSide(std::shared_ptr<Stream> stream) : stream_(std::move(stream)) {}
    Result<void> send(std::string payload);
    void close_send();
    Signal requests_closed() const;
    void fail(Status status);
protected:
    std::shared_ptr<Stream> stream_;
};
}

class ReadOperation {
public:
    explicit ReadOperation(std::shared_ptr<detail::Stream> stream);
    ReadOperation(const ReadOperation&) = delete;
    ReadOperation(ReadOperation&&) = delete;
    ~ReadOperation();
    bool await_ready() const;
    void await_suspend(std::coroutine_handle<> continuation);
    Result<std::optional<std::string>> await_resume();
    void decode_failed();

private:
    std::shared_ptr<detail::Stream> stream_;
};

class Call : private detail::RequestSide {
public:
    using RequestSide::send;
    using RequestSide::close_send;
    Call(Call&&) noexcept = default;
    Call& operator=(Call&&) noexcept;
    ~Call();
    void cancel();
    ReadOperation read() const;
    const Metadata& headers() const;
    const Metadata& trailers() const;
    Signal headers_ready() const;
    Signal finished() const;
    static Call completed(MethodKind kind, std::vector<std::string> responses, Status status, Options options,
                          Metadata headers, Metadata trailers);
private:
    friend class Session;
    template<class, class, bool, bool> friend class Rpc;
    using RequestSide::RequestSide;
};

// All calls, input events and coroutine resumptions use one host event loop.
class Session {
public:
    Session(std::function<bool(std::string)> send, std::function<void()> close, Limits limits = {},
            std::function<void(const SessionEnd&)> on_end = {});
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    ~Session();
    void opened(std::string_view negotiated_protocol);
    void receive(std::string_view message);
    void disconnected();
    void expire(Clock::time_point now = Clock::now());
    std::optional<Clock::time_point> next_deadline() const;
    bool is_open() const;
    const std::optional<SessionEnd>& end() const;
    Call start(std::string_view method, Options options, MethodKind kind);
    Call start(std::string_view method, Options options, bool single_response) {
        return start(method, std::move(options), single_response ? MethodKind::unary : MethodKind::bidi);
    }
    template<class Method, class... Args>
    auto invoke(Method method, Options options, const Args&... args) {
        return typename Method::CallType(start(method.path, std::move(options), Method::kind), args...);
    }

private:
    std::shared_ptr<detail::Connection> connection_;
};

template<class Message, bool Single>
class MessageRead : public ReadOperation {
public:
    using Transform = std::shared_ptr<const std::function<void(Message&)>>;
    MessageRead(std::shared_ptr<detail::Stream> stream, Transform transform)
        : ReadOperation(std::move(stream)), transform_(std::move(transform)) {}
    using Value = std::conditional_t<Single, Message, std::optional<Message>>;

    Result<Value> await_resume() {
        auto bytes = ReadOperation::await_resume();
        if (!bytes) return std::unexpected(std::move(bytes.error()));
        if (!*bytes) {
            if constexpr (Single) return std::unexpected(Status{Code::failed_precondition, "no response"});
            else return std::nullopt;
        }
        Message message;
        if (!message.ParseFromString(**bytes)) {
            decode_failed();
            return std::unexpected(Status{Code::data_loss, "invalid protobuf response"});
        }
        if (transform_) (*transform_)(message);
        return Value(std::move(message));
    }
private:
    Transform transform_;
};

template<class Request, class Response, bool ClientStreaming, bool ServerStreaming>
class Rpc {
public:
    using RequestType = Request;
    using ResponseType = Response;
    static constexpr auto kind = ClientStreaming
        ? (ServerStreaming ? MethodKind::bidi : MethodKind::client_streaming)
        : (ServerStreaming ? MethodKind::server_streaming : MethodKind::unary);
    explicit Rpc(Call call) : call_(std::move(call)) {}
    Rpc(Call call, const Request& request) requires (!ClientStreaming) : Rpc(std::move(call)) {
        if (write_message(request)) call_.close_send();
    }
    Result<void> write(const Request& request) requires ClientStreaming { return write_message(request); }
    void close_send() requires ClientStreaming { call_.close_send(); }
    auto read() const requires ServerStreaming { return MessageRead<Response, false>(call_.stream_, response_map_); }
    auto result() const requires (!ServerStreaming) { return MessageRead<Response, true>(call_.stream_, response_map_); }
    void cancel() { call_.cancel(); }
    const Metadata& headers() const { return call_.headers(); }
    const Metadata& trailers() const { return call_.trailers(); }
    Signal headers_ready() const { return call_.headers_ready(); }
    Signal finished() const { return call_.finished(); }

    // The outermost interceptor sees requests first and responses last.
    void map_requests(std::function<void(Request&)> transform) requires ClientStreaming {
        request_map_ = compose(std::move(transform), std::move(request_map_));
    }
    void map_responses(std::function<void(Response&)> transform) {
        if (response_map_) {
            auto previous = response_map_;
            transform = [previous, next = std::move(transform)](Response& message) {
                (*previous)(message);
                next(message);
            };
        }
        response_map_ = std::make_shared<const std::function<void(Response&)>>(std::move(transform));
    }
    static Rpc completed(std::vector<Response> responses, Options options = {}, Status status = {},
                         Metadata headers = {}, Metadata trailers = {}) {
        std::vector<std::string> bytes;
        for (const auto& response : responses) {
            std::string payload;
            if (!response.IsInitialized() || !response.SerializeToString(&payload)) {
                status = {Code::invalid_argument, "uninitialized protobuf response"};
                break;
            }
            bytes.push_back(std::move(payload));
        }
        return Rpc(Call::completed(kind, std::move(bytes), std::move(status), std::move(options),
                                   std::move(headers), std::move(trailers)));
    }

    // next(stop) asynchronously returns Result<std::optional<Request>>. An idle
    // source must wake on stop so its coroutine and resources can be released.
    template<class Next>
    void write_from(Next next) requires ClientStreaming {
        pump(call_.stream_, request_map_, std::move(next));
    }

private:
    template<class T>
    static std::function<void(T&)> compose(std::function<void(T&)> first, std::function<void(T&)> second) {
        if (!first) return second;
        if (!second) return first;
        return [first = std::move(first), second = std::move(second)](T& value) { first(value); second(value); };
    }
    struct Producer {
        struct promise_type {
            Producer get_return_object() { return {}; }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void return_void() {}
            void unhandled_exception() { std::terminate(); }
        };
    };
    template<class Next>
    static Producer pump(std::shared_ptr<detail::Stream> stream, std::function<void(Request&)> transform, Next next) {
        detail::RequestSide writer(std::move(stream));
        auto stop = writer.requests_closed();
#if defined(__cpp_exceptions)
        try {
#endif
            while (!stop.ready()) {
                auto item = co_await next(stop);
                if (stop.ready()) break;
                if (!item) { writer.fail(std::move(item.error())); break; }
                if (!*item) { writer.close_send(); break; }
                if (transform) transform(**item);
                if (!send_message(writer, **item)) break;
            }
#if defined(__cpp_exceptions)
        } catch (...) {
            writer.fail({Code::internal, "request source threw"});
        }
#endif
    }
    static Result<void> send_message(detail::RequestSide& call, const Request& message) {
        std::string payload;
        if (!message.IsInitialized() || !message.SerializeToString(&payload)) {
            Status error{Code::invalid_argument, "uninitialized protobuf request"};
            call.fail(error);
            return std::unexpected(std::move(error));
        }
        return call.send(std::move(payload));
    }
    Result<void> write_message(const Request& message) {
        if (!request_map_) return send_message(call_, message);
        Request copy(message);
        request_map_(copy);
        return send_message(call_, copy);
    }
    Call call_;
    std::function<void(Request&)> request_map_;
    typename MessageRead<Response, !ServerStreaming>::Transform response_map_;
};

template<class Rpc>
struct Method {
    using CallType = Rpc;
    static constexpr auto kind = Rpc::kind;
    std::string_view path;
};

template<class Next, class Handler>
struct InterceptedChannel {
    Next& channel;
    Handler handler;

    template<class M, class... Args>
    auto invoke(M method, Options options, const Args&... args) {
        auto next = [this, method](Options opts, const auto&... request) {
            return channel.invoke(method, std::move(opts), request...);
        };
        return handler(method, next, std::move(options), args...);
    }
};

template<class Next, class Handler>
auto intercept(Next& channel, Handler handler) {
    return InterceptedChannel<Next, Handler>{channel, std::move(handler)};
}

} // namespace ws_proto
