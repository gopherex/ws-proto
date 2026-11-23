#pragma once

#include <chrono>
#include <coroutine>
#include <cstdint>
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
};

struct Limits {
    std::uint32_t initial_window = 256 * 1024;
    std::size_t max_send_buffer = 16 * 1024 * 1024;
    std::size_t max_frame_bytes = 16 * 1024 * 1024;
    std::size_t max_queued_messages = 1024 * 1024;
};

namespace detail {
struct Connection;
struct Stream;
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

class Call {
public:
    Call(Call&&) noexcept = default;
    Call& operator=(Call&&) noexcept;
    ~Call();
    Result<void> send(std::string payload);
    void close_send();
    void cancel();
    ReadOperation read() const;
    const Metadata& headers() const;
    const Metadata& trailers() const;
private:
    friend class Session;
    template<class, class, bool, bool> friend class Rpc;
    void encoding_failed();
    explicit Call(std::shared_ptr<detail::Stream> stream);
    std::shared_ptr<detail::Stream> stream_;
};

// All calls, input events and coroutine resumptions use one host event loop.
class Session {
public:
    Session(std::function<bool(std::string)> send, std::function<void()> close, Limits limits = {});
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    ~Session();
    void opened(std::string_view negotiated_protocol);
    void receive(std::string_view message);
    void disconnected();
    void expire(Clock::time_point now = Clock::now());
    std::optional<Clock::time_point> next_deadline() const;
    Call start(std::string_view method, Options options, bool single_response);

private:
    std::shared_ptr<detail::Connection> connection_;
};

template<class Message, bool Single>
class MessageRead : public ReadOperation {
public:
    using ReadOperation::ReadOperation;
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
        return Value(std::move(message));
    }
};

template<class Request, class Response, bool ClientStreaming, bool ServerStreaming>
class Rpc {
public:
    explicit Rpc(Call call) : call_(std::move(call)) {}
    Rpc(Call call, const Request& request) requires (!ClientStreaming) : Rpc(std::move(call)) {
        write_message(request);
        call_.close_send();
    }
    Result<void> write(const Request& request) requires ClientStreaming { return write_message(request); }
    void close_send() requires ClientStreaming { call_.close_send(); }
    auto read() const requires ServerStreaming { return MessageRead<Response, false>(call_.stream_); }
    auto result() const requires (!ServerStreaming) { return MessageRead<Response, true>(call_.stream_); }
    void cancel() { call_.cancel(); }
    const Metadata& headers() const { return call_.headers(); }
    const Metadata& trailers() const { return call_.trailers(); }

private:
    Result<void> write_message(const Request& message) {
        std::string payload;
        if (!message.IsInitialized() || !message.SerializeToString(&payload)) {
            call_.encoding_failed();
            return std::unexpected(Status{Code::invalid_argument, "uninitialized protobuf request"});
        }
        return call_.send(std::move(payload));
    }
    Call call_;
};

} // namespace ws_proto
