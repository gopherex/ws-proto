#include "ws_proto/client.hpp"
#include "transport/transport.pb.h"

#include <algorithm>
#include <climits>
#include <deque>
#include <unordered_map>

namespace ws_proto {
namespace wire = ::wsproto::transport::v1;

namespace detail {
struct Stream {
    std::weak_ptr<Connection> connection;
    std::uint32_t id = 0;
    bool single = false;
    bool closing = false;
    bool half_closed = false;
    bool headers_seen = false;
    std::optional<Status> terminal;
    std::optional<Clock::time_point> deadline;
    Metadata headers, trailers;
    std::deque<std::string> incoming, outgoing;
    std::size_t incoming_bytes = 0, outgoing_bytes = 0;
    std::int64_t credit = 0;
    std::uint32_t consumed = 0;
    std::coroutine_handle<> reader;
    ReadOperation* waiting = nullptr;

    void wake() {
        if (reader && (terminal || (!single && !incoming.empty()))) {
            waiting = nullptr;
            std::exchange(reader, {}).resume();
        }
    }

    void finish(Status status, bool discard) {
        if (terminal) return;
        terminal = std::move(status);
        outgoing.clear();
        outgoing_bytes = 0;
        if (discard || (single && terminal->code != Code::ok)) {
            incoming.clear();
            incoming_bytes = 0;
        }
        wake();
    }
};

wire::Frame frame(std::uint32_t id, wire::Kind kind) {
    wire::Frame value;
    value.set_stream_id(id);
    value.set_kind(kind);
    return value;
}

Status peer_status(const wire::Status& value) {
    return {static_cast<Code>(value.code()), value.message(), {value.details().begin(), value.details().end()}};
}

struct Connection {
    std::function<bool(std::string)> send;
    std::function<void()> close;
    Limits limits;
    bool open = false;
    std::optional<Status> end;
    std::uint64_t next_id = 1;
    std::unordered_map<std::uint32_t, std::shared_ptr<Stream>> streams;

    void stop(Status reason, bool close_host) {
        if (end) return;
        end = reason;
        open = false;
        auto active = std::exchange(streams, {});
        if (close_host) close();
        for (auto& [id, stream] : active) stream->finish(reason, true);
    }

    bool transmit(const wire::Frame& envelope) {
        if (end) return false;
        if (send(envelope.SerializeAsString())) return true;
        stop({Code::unavailable, "host refused WebSocket message"}, true);
        return false;
    }

    void reset(std::shared_ptr<Stream> stream, Status status) {
        if (stream->terminal || !streams.erase(stream->id)) return;
        auto envelope = frame(stream->id, wire::KIND_RST);
        envelope.mutable_status()->set_code(static_cast<int>(status.code));
        envelope.mutable_status()->set_message(status.message);
        transmit(envelope);
        stream->finish(std::move(status), true);
    }

    void consumed(Stream& stream, std::size_t count) {
        stream.consumed += static_cast<std::uint32_t>(count);
        if (stream.consumed < std::max(1u, limits.initial_window / 2)) return;
        auto update = frame(stream.id, wire::KIND_WINDOW_UPDATE);
        update.set_window(std::exchange(stream.consumed, 0));
        transmit(update);
    }

    void pump(std::shared_ptr<Stream> stream) {
        while (!stream->terminal && stream->credit > 0 && !stream->outgoing.empty()) {
            auto envelope = frame(stream->id, wire::KIND_MSG);
            auto payload = std::move(stream->outgoing.front());
            stream->outgoing.pop_front();
            stream->outgoing_bytes -= payload.size();
            stream->credit -= static_cast<std::int64_t>(payload.size());
            envelope.set_payload(std::move(payload));
            if (!transmit(envelope)) return;
        }
        if (!stream->terminal && stream->closing && !stream->half_closed && stream->outgoing.empty()) {
            stream->half_closed = true;
            transmit(frame(stream->id, wire::KIND_HALF_CLOSE));
        }
    }
};
} // namespace detail

using detail::frame;

Session::Session(std::function<bool(std::string)> send, std::function<void()> close, Limits limits)
    : connection_(std::make_shared<detail::Connection>()) {
    connection_->send = std::move(send);
    connection_->close = std::move(close);
    if (!limits.initial_window) limits.initial_window = Limits{}.initial_window;
    connection_->limits = limits;
}

Session::~Session() { connection_->stop({Code::cancelled, "session destroyed"}, true); }

void Session::opened(std::string_view protocol) {
    auto connection = connection_;
    if (connection->end) return;
    if (protocol != "wsrpc.v1") connection->stop({Code::failed_precondition, "expected wsrpc.v1"}, true);
    else connection->open = true;
}

void Session::disconnected() {
    auto connection = connection_;
    connection->stop({Code::unavailable, "connection lost"}, false);
}

Call Session::start(std::string_view method, Options options, bool single_response) {
    auto connection = connection_;
    auto stream = std::make_shared<detail::Stream>();
    stream->connection = connection;
    stream->single = single_response;
    stream->deadline = options.deadline;
    if (!connection->open) {
        stream->finish(connection->end.value_or(Status{Code::unavailable, "connection is not open"}), true);
        return Call(stream);
    }
    if (connection->next_id > UINT32_MAX) {
        stream->finish({Code::resource_exhausted, "stream ids exhausted"}, true);
        return Call(stream);
    }
    if (stream->deadline) {
        auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*stream->deadline - Clock::now());
        if (remaining.count() <= 0) {
            stream->finish({Code::deadline_exceeded, "deadline exceeded"}, true);
            return Call(stream);
        }
        options.headers["ws-timeout-ms"] = std::to_string(remaining.count());
    }
    stream->id = static_cast<std::uint32_t>(connection->next_id);
    connection->next_id += 2;
    stream->credit = connection->limits.initial_window;
    connection->streams.emplace(stream->id, stream);
    auto envelope = frame(stream->id, wire::KIND_OPEN);
    envelope.set_method(std::string(method));
    envelope.mutable_headers()->insert(options.headers.begin(), options.headers.end());
    connection->transmit(envelope);
    return Call(stream);
}

void Session::receive(std::string_view message) {
    auto connection = connection_;
    if (!connection->open) return;
    if ((connection->limits.max_frame_bytes && message.size() > connection->limits.max_frame_bytes)
        || message.size() > INT_MAX) {
        connection->stop({Code::resource_exhausted, "incoming frame too large"}, true);
        return;
    }
    wire::Frame envelope;
    if (!envelope.ParseFromArray(message.data(), static_cast<int>(message.size()))) return;
    if (envelope.kind() == wire::KIND_CONN_END && envelope.stream_id() == 0) {
        auto status = detail::peer_status(envelope.status());
        if (status.code == Code::ok) status = {Code::unavailable, "server ended connection"};
        connection->stop(std::move(status), true);
        return;
    }
    const auto it = connection->streams.find(envelope.stream_id());
    if (it == connection->streams.end()) return;
    auto stream = it->second;
    switch (envelope.kind()) {
    case wire::KIND_HEADER:
        if (!stream->headers_seen) stream->headers.insert(envelope.headers().begin(), envelope.headers().end());
        stream->headers_seen = true;
        return;
    case wire::KIND_WINDOW_UPDATE:
        stream->credit = std::min<std::int64_t>(UINT32_MAX, stream->credit + envelope.window());
        connection->pump(stream);
        return;
    case wire::KIND_MSG:
        stream->headers_seen = true;
        if (stream->single && !stream->incoming.empty()) {
            connection->reset(stream, {Code::internal, "multiple unary responses"});
            return;
        }
        if (stream->incoming_bytes >= connection->limits.initial_window
            || stream->incoming.size() >= connection->limits.max_queued_messages) {
            connection->reset(stream, {Code::resource_exhausted, "receive backlog full"});
            return;
        }
        stream->incoming_bytes += envelope.payload().size();
        stream->incoming.push_back(std::move(*envelope.mutable_payload()));
        // A single-response RPC consumes its one result immediately but cannot
        // publish it until END. A second response must be able to reach us.
        if (stream->single) connection->consumed(*stream, stream->incoming_bytes);
        stream->wake();
        return;
    case wire::KIND_RST:
    case wire::KIND_END: {
        connection->streams.erase(it);
        stream->trailers.insert(envelope.headers().begin(), envelope.headers().end());
        auto status = detail::peer_status(envelope.status());
        const bool reset = envelope.kind() == wire::KIND_RST;
        if (reset && status.code == Code::ok) status = {Code::cancelled, "peer reset stream"};
        if (stream->single && status.code == Code::ok && stream->incoming.empty())
            status = {Code::internal, "missing unary response"};
        stream->finish(std::move(status), reset);
        return;
    }
    default: return;
    }
}

void Session::expire(Clock::time_point now) {
    auto connection = connection_;
    std::vector<std::shared_ptr<detail::Stream>> expired;
    for (const auto& [id, stream] : connection->streams)
        if (stream->deadline && *stream->deadline <= now) expired.push_back(stream);
    for (auto& stream : expired) connection->reset(stream, {Code::deadline_exceeded, "deadline exceeded"});
}

std::optional<Clock::time_point> Session::next_deadline() const {
    std::optional<Clock::time_point> next;
    for (const auto& [id, stream] : connection_->streams)
        if (stream->deadline && (!next || *stream->deadline < *next)) next = stream->deadline;
    return next;
}

Call::Call(std::shared_ptr<detail::Stream> stream) : stream_(std::move(stream)) {}
Call::~Call() { cancel(); }
Call& Call::operator=(Call&& other) noexcept {
    if (this != &other) {
        cancel();
        stream_ = std::move(other.stream_);
    }
    return *this;
}

void Call::cancel() {
    if (stream_ && !stream_->terminal)
        if (auto connection = stream_->connection.lock()) connection->reset(stream_, {Code::cancelled, "cancelled"});
}

void Call::encoding_failed() {
    if (auto connection = stream_->connection.lock())
        connection->reset(stream_, {Code::invalid_argument, "uninitialized protobuf request"});
}

Result<void> Call::send(std::string payload) {
    auto stream = stream_;
    auto connection = stream->connection.lock();
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    if (connection && connection->end && !stream->terminal) return std::unexpected(*connection->end);
    if (!connection || stream->closing || stream->terminal)
        return std::unexpected(Status{Code::failed_precondition, "request side closed"});
    if ((stream->credit <= 0 && payload.size() > connection->limits.max_send_buffer -
           std::min(stream->outgoing_bytes, connection->limits.max_send_buffer))
        || stream->outgoing.size() >= connection->limits.max_queued_messages || payload.size() > INT_MAX) {
        Status status{Code::resource_exhausted, "send backlog full"};
        connection->reset(stream, status);
        return std::unexpected(status);
    }
    stream->outgoing_bytes += payload.size();
    stream->outgoing.push_back(std::move(payload));
    connection->pump(stream);
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    return {};
}

void Call::close_send() {
    if (stream_->closing || stream_->terminal) return;
    stream_->closing = true;
    if (auto connection = stream_->connection.lock()) connection->pump(stream_);
}
const Metadata& Call::headers() const { return stream_->headers; }
const Metadata& Call::trailers() const { return stream_->trailers; }
ReadOperation Call::read() const { return ReadOperation(stream_); }

ReadOperation::ReadOperation(std::shared_ptr<detail::Stream> stream) : stream_(std::move(stream)) {}
ReadOperation::~ReadOperation() {
    if (stream_->waiting == this) {
        stream_->waiting = nullptr;
        stream_->reader = {};
    }
}
bool ReadOperation::await_ready() const {
    return stream_->reader || stream_->terminal || (!stream_->single && !stream_->incoming.empty());
}
void ReadOperation::await_suspend(std::coroutine_handle<> continuation) {
    stream_->waiting = this;
    stream_->reader = continuation;
}
Result<std::optional<std::string>> ReadOperation::await_resume() {
    if (stream_->reader) return std::unexpected(Status{Code::failed_precondition, "read already pending"});
    if (!stream_->incoming.empty()) {
        auto bytes = std::move(stream_->incoming.front());
        stream_->incoming.pop_front();
        stream_->incoming_bytes -= bytes.size();
        if (!stream_->terminal && !stream_->single)
            if (auto connection = stream_->connection.lock()) connection->consumed(*stream_, bytes.size());
        return std::optional(std::move(bytes));
    }
    if (stream_->terminal && stream_->terminal->code != Code::ok) return std::unexpected(*stream_->terminal);
    return std::nullopt;
}

void ReadOperation::decode_failed() {
    if (auto connection = stream_->connection.lock()) connection->reset(stream_, {Code::data_loss, "invalid protobuf response"});
}

} // namespace ws_proto
