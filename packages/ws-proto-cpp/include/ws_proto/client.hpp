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
using Now = std::function<Clock::time_point()>;
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
    std::size_t max_retry_buffer = 4 * 1024 * 1024;
};

enum class MethodKind { unary, server_streaming, client_streaming, bidi };
struct SessionEnd {
    Status status;
    bool retryable;
};

namespace detail {
struct Connection;
struct Stream;
template<class M> using MessageTransform = std::shared_ptr<const std::function<void(M&)>>;

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
    explicit ReadOperation(std::shared_ptr<detail::Stream> stream, bool return_credit = true);
    ReadOperation(const ReadOperation&) = delete;
    ReadOperation(ReadOperation&&) = delete;
    ~ReadOperation();
    bool await_ready() const;
    void await_suspend(std::coroutine_handle<> continuation);
    Result<std::optional<std::string>> await_resume();
    void decode_failed();

private:
    std::shared_ptr<detail::Stream> stream_;
    bool return_credit_;
};

class Call : private detail::RequestSide {
public:
    using RequestSide::send;
    using RequestSide::close_send;
    Call(Call&&) noexcept = default;
    Call& operator=(Call&&) noexcept;
    ~Call();
    void cancel();
    std::function<void()> canceller() const;
    ReadOperation read() const;
    ReadOperation read_without_credit() const;
    void return_credit(std::size_t bytes);
    const Metadata& headers() const;
    const Metadata& trailers() const;
    Signal headers_ready() const;
    Signal finished() const;
    Signal response_started() const;
    const std::optional<Status>& status() const;
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
    Session(std::function<bool(std::string)> send, std::function<void()> close, Limits limits,
            std::function<void(const SessionEnd&)> on_end, Now now);
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    ~Session();
    void opened(std::string_view negotiated_protocol);
    void receive(std::string_view message);
    void disconnected();
    void expire();
    void expire(Clock::time_point now);
    std::optional<Clock::time_point> next_deadline() const;
    bool is_open() const;
    const std::optional<SessionEnd>& end() const;
    const Limits& limits() const;
    Call start(std::string_view method, Options options, MethodKind kind);
    Call start(std::string_view method, Options options, bool single_response) {
        return start(method, std::move(options), single_response ? MethodKind::unary : MethodKind::bidi);
    }
    template<class Method, class... Args>
    auto invoke(Method method, Options options, const Args&... args) {
        using Rpc = typename Method::CallType;
        if constexpr (Method::kind == MethodKind::client_streaming) {
            auto last = std::make_shared<typename Rpc::ResponseType>();
            auto call = start_call(method.path, std::move(options), Method::kind,
                                   [last](const std::string& bytes) { return last->ParseFromString(bytes); });
            return Rpc(std::move(call), std::move(last));
        } else {
            return Rpc(start(method.path, std::move(options), Method::kind), args...);
        }
    }

private:
    Call start_call(std::string_view method, Options options, MethodKind kind,
                    std::function<bool(const std::string&)> decode_upload);
    std::shared_ptr<detail::Connection> connection_;
};

template<class Message, bool Single>
class MessageRead : public ReadOperation {
public:
    using Transform = detail::MessageTransform<Message>;
    MessageRead(std::shared_ptr<detail::Stream> stream, Transform transform, std::shared_ptr<Message> decoded = {})
        : ReadOperation(std::move(stream)), transform_(std::move(transform)), decoded_(std::move(decoded)) {}
    using Value = std::conditional_t<Single, Message, std::optional<Message>>;

    Result<Value> await_resume() {
        auto bytes = ReadOperation::await_resume();
        if (!bytes) return std::unexpected(std::move(bytes.error()));
        if (!*bytes) {
            if constexpr (Single) return std::unexpected(Status{Code::failed_precondition, "no response"});
            else return std::nullopt;
        }
        Message message;
        if (decoded_) message = std::move(*decoded_);
        else if (!message.ParseFromString(**bytes)) {
            decode_failed();
            return std::unexpected(Status{Code::data_loss, "invalid protobuf response"});
        }
        if (transform_) (*transform_)(message);
        return Value(std::move(message));
    }
private:
    Transform transform_;
    std::shared_ptr<Message> decoded_;
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
    auto result() const requires (!ServerStreaming) { return MessageRead<Response, true>(call_.stream_, response_map_, last_response_); }
    void cancel() { call_.cancel(); }
    auto canceller() const { return call_.canceller(); }
    const Metadata& headers() const { return call_.headers(); }
    const Metadata& trailers() const { return call_.trailers(); }
    Signal headers_ready() const { return call_.headers_ready(); }
    Signal finished() const { return call_.finished(); }

    // The outermost interceptor sees requests first and responses last.
    void map_requests(std::function<void(Request&)> transform) requires ClientStreaming {
        if (!transform) return;
        if (request_map_) {
            auto inner = request_map_;
            transform = [outer = std::move(transform), inner](Request& message) {
                outer(message);
                (*inner)(message);
            };
        }
        request_map_ = std::make_shared<const std::function<void(Request&)>>(std::move(transform));
    }
    void map_responses(std::function<void(Response&)> transform) {
        if (!transform) return;
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
    friend class Session;
    Rpc(Call call, std::shared_ptr<Response> last) requires (ClientStreaming && !ServerStreaming)
        : call_(std::move(call)), last_response_(std::move(last)) {}
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
    static Producer pump(std::shared_ptr<detail::Stream> stream, detail::MessageTransform<Request> transform, Next next) {
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
                if (transform) (*transform)(**item);
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
        (*request_map_)(copy);
        return send_message(call_, copy);
    }
    Call call_;
    std::shared_ptr<Response> last_response_;
    detail::MessageTransform<Request> request_map_;
    detail::MessageTransform<Response> response_map_;
};

template<class Rpc>
struct Method {
    using CallType = Rpc;
    static constexpr auto kind = Rpc::kind;
    std::string_view path;
};

namespace detail {
template<std::size_t Size>
struct MethodPath {
    char text[Size];
    constexpr MethodPath(const char (&path)[Size]) : text{} {
        for (std::size_t i = 0; i < Size; ++i) text[i] = path[i];
    }
};
} // namespace detail

// Generated method identities are independent of the configured channel.
template<detail::MethodPath Path, class Rpc>
struct MethodDescriptor : Method<Rpc> {
    constexpr MethodDescriptor() : Method<Rpc>{std::string_view(Path.text)} {}
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
