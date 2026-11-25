#include "ws_proto/server.hpp"
#include "wire.hpp"

#include <algorithm>
#include <charconv>
#include <climits>
#include <deque>
#include <unordered_map>

namespace ws_proto {
namespace wire = ::wsproto::transport::v1;
namespace detail {
struct InboundStream {
    std::weak_ptr<ServerConnection> owner;
    std::uint32_t id, consumed = 0;
    MethodKind kind;
    Metadata headers;
    std::optional<Clock::time_point> deadline;
    std::shared_ptr<ServerSession::Handler> handler;
    SignalSource cancelled;
    std::optional<Status> terminal;
    std::optional<wire::Frame> ending;
    std::deque<std::string> requests, responses;
    std::size_t queued_in = 0, queued_out = 0;
    std::int64_t credit;
    bool received = false, eof = false, sent_header = false, sent_message = false, flushing = false;
    ServerRead* waiting = nullptr;
    std::coroutine_handle<> reader;

    bool single_request() const { return kind == MethodKind::unary || kind == MethodKind::server_streaming; }
    bool single_response() const { return kind == MethodKind::unary || kind == MethodKind::client_streaming; }
    bool readable() const { return terminal || ending || eof || !requests.empty(); }
    void wake() {
        if (!reader || !readable()) return;
        waiting = nullptr;
        std::exchange(reader, {}).resume();
    }
};

static wire::Frame end_frame(std::uint32_t id, const Status& status, const Metadata& trailers = {}) {
    auto envelope = frame(id, wire::KIND_END);
    write_status(envelope, status);
    envelope.mutable_headers()->insert(trailers.begin(), trailers.end());
    return envelope;
}

struct ServerConnection : std::enable_shared_from_this<ServerConnection> {
    struct MethodEntry { MethodKind kind; std::shared_ptr<ServerSession::Handler> handler; };
    std::map<std::string, MethodEntry, std::less<>> methods;
    std::unordered_map<std::uint32_t, std::shared_ptr<InboundStream>> streams;
    std::function<bool(std::string)> send;
    std::function<void()> close;
    Now now;
    Limits limits;
    bool open = false, stopped = false;

    void terminate(std::shared_ptr<InboundStream> stream, Status status, bool cancelled,
                   std::optional<wire::Frame> final = {}) {
        if (stream->terminal) return;
        streams.erase(stream->id);
        stream->terminal = std::move(status);
        stream->ending.reset();
        stream->responses.clear();
        stream->requests.clear();
        stream->queued_in = stream->queued_out = 0;
        if (final && !transmit(*final)) {
            stream->terminal = Status{Code::unavailable, "connection closed"};
            cancelled = true;
        }
        if (cancelled) stream->cancelled.notify();
        stream->wake();
    }

    void stop(Status status, bool close_host) {
        if (stopped) return;
        stopped = true;
        open = false;
        if (close_host) close();
        while (!streams.empty()) terminate(streams.begin()->second, status, true);
    }

    bool transmit(const wire::Frame& envelope) {
        if (!open) return false;
        if (send(envelope.SerializeAsString())) return true;
        stop({Code::unavailable, "host refused WebSocket message"}, true);
        return false;
    }

    void abort(std::shared_ptr<InboundStream> stream, Status status) {
        auto final = end_frame(stream->id, status);
        terminate(stream, std::move(status), true, std::move(final));
    }

    void pump(std::shared_ptr<InboundStream> stream) {
        if (stream->flushing) return;
        stream->flushing = true;
        while (!stream->terminal && stream->credit > 0 && !stream->responses.empty()) {
            auto envelope = frame(stream->id, wire::KIND_MSG);
            auto& bytes = stream->responses.front();
            stream->credit -= static_cast<std::int64_t>(bytes.size());
            stream->queued_out -= bytes.size();
            envelope.set_payload(std::move(bytes));
            stream->responses.pop_front();
            if (!transmit(envelope)) break;
        }
        stream->flushing = false;
        if (stream->ending && stream->responses.empty()) {
            auto status = peer_status(stream->ending->status());
            terminate(stream, std::move(status), false, std::move(stream->ending));
        }
    }

    void accept(const wire::Frame& envelope) {
        const auto id = envelope.stream_id();
        if (!id) return;
        if (auto duplicate = streams.find(id); duplicate != streams.end()) {
            abort(duplicate->second, {Code::invalid_argument, "duplicate OPEN"});
            return;
        }
        auto it = methods.find(envelope.method());
        if (it == methods.end()) {
            transmit(end_frame(id, {Code::unimplemented, "unknown method " + envelope.method()}));
            return;
        }
        auto stream = std::make_shared<InboundStream>();
        stream->id = id;
        stream->kind = it->second.kind;
        stream->handler = it->second.handler;
        stream->credit = limits.initial_window;
        stream->headers.insert(envelope.headers().begin(), envelope.headers().end());
        if (auto timeout = stream->headers.find("ws-timeout-ms"); timeout != stream->headers.end()) {
            const auto& text = timeout->second;
            std::int64_t ms = 0;
            auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), ms);
            if (ec == std::errc{} && end == text.data() + text.size() && ms > 0) {
                using namespace std::chrono;
                auto max_ms = duration_cast<milliseconds>(Clock::duration::max()).count();
                auto duration = milliseconds(std::min<std::int64_t>(ms, max_ms));
                auto start = now();
                stream->deadline = start > Clock::time_point::max() - duration ? Clock::time_point::max() : start + duration;
            }
        }
        stream->owner = weak_from_this();
        streams.emplace(id, stream);
#if defined(__cpp_exceptions)
        try {
#endif
            (*stream->handler)(ServerCall(stream));
#if defined(__cpp_exceptions)
        } catch (...) {
            abort(stream, {Code::internal, "server handler threw"});
        }
#endif
    }

    void receive(wire::Frame envelope) {
        if (envelope.kind() == wire::KIND_OPEN) { accept(envelope); return; }
        auto it = streams.find(envelope.stream_id());
        if (it == streams.end()) return;
        auto stream = it->second;
        switch (envelope.kind()) {
        case wire::KIND_MSG:
            if (stream->ending) return;
            if (stream->eof || (stream->single_request() && stream->received)) {
                abort(stream, {Code::invalid_argument, "unexpected request message"});
                return;
            }
            if (stream->queued_in >= limits.initial_window ||
                (limits.max_queued_messages && stream->requests.size() >= limits.max_queued_messages)) {
                abort(stream, {Code::resource_exhausted, "request backlog full"});
                return;
            }
            stream->received = true;
            stream->queued_in += envelope.payload().size();
            stream->requests.push_back(std::move(*envelope.mutable_payload()));
            stream->wake();
            break;
        case wire::KIND_HALF_CLOSE:
            if (stream->single_request() && !stream->received) {
                abort(stream, {Code::invalid_argument, "missing request"});
                return;
            }
            stream->eof = true;
            stream->wake();
            break;
        case wire::KIND_WINDOW_UPDATE:
            stream->credit = std::min<std::int64_t>(UINT32_MAX, stream->credit + envelope.window());
            pump(stream);
            break;
        case wire::KIND_RST: {
            auto status = peer_status(envelope.status());
            if (status.code == Code::ok) status.code = Code::cancelled;
            terminate(stream, std::move(status), true);
            break;
        }
        default: break;
        }
    }
};
} // namespace detail

ServerSession::ServerSession(std::function<bool(std::string)> send, std::function<void()> close, Limits limits, Now now)
    : connection_(std::make_shared<detail::ServerConnection>()) {
    connection_->send = std::move(send);
    connection_->close = std::move(close);
    connection_->now = std::move(now);
    if (!limits.initial_window) limits.initial_window = Limits{}.initial_window;
    connection_->limits = limits;
}

ServerSession::~ServerSession() { connection_->stop({Code::cancelled, "session destroyed"}, true); }
bool ServerSession::register_method(std::string path, MethodKind kind, Handler handler) {
    if (path.empty() || !handler) return false;
    return connection_->methods.try_emplace(std::move(path), kind, std::make_shared<Handler>(std::move(handler))).second;
}
void ServerSession::opened(std::string_view protocol) {
    auto owner = connection_;
    if (owner->stopped) return;
    if (protocol == "wsrpc.v1") owner->open = true;
    else owner->stop({Code::unavailable, "expected wsrpc.v1"}, true);
}
void ServerSession::disconnected() {
    auto owner = connection_;
    owner->stop({Code::unavailable, "connection lost"}, false);
}
bool ServerSession::is_open() const { return connection_->open; }
void ServerSession::receive(std::string_view message) {
    auto owner = connection_;
    if (!owner->open) return;
    if ((owner->limits.max_frame_bytes && message.size() > owner->limits.max_frame_bytes) || message.size() > INT_MAX) {
        owner->stop({Code::resource_exhausted, "incoming frame too large"}, true);
        return;
    }
    wire::Frame envelope;
    if (envelope.ParseFromArray(message.data(), static_cast<int>(message.size()))) owner->receive(std::move(envelope));
}
void ServerSession::expire() { expire(connection_->now()); }
void ServerSession::expire(Clock::time_point now) {
    auto owner = connection_;
    std::vector<std::shared_ptr<detail::InboundStream>> expired;
    for (const auto& [id, stream] : owner->streams)
        if (stream->deadline && *stream->deadline <= now) expired.push_back(stream);
    for (const auto& stream : expired) owner->abort(stream, {Code::deadline_exceeded, "deadline exceeded"});
}
std::optional<Clock::time_point> ServerSession::next_deadline() const {
    std::optional<Clock::time_point> next;
    for (const auto& [id, stream] : connection_->streams)
        if (stream->deadline && (!next || *stream->deadline < *next)) next = stream->deadline;
    return next;
}

const Metadata& ServerCall::headers() const { return stream_->headers; }
std::optional<Clock::time_point> ServerCall::deadline() const { return stream_->deadline; }
Signal ServerCall::cancellation() const { return stream_->cancelled.signal(); }

std::function<void()> ServerCall::canceller() const {
    return [weak = std::weak_ptr(stream_)] {
        if (auto stream = weak.lock())
            if (auto owner = stream->owner.lock())
                owner->abort(stream, {Code::cancelled, "server cancelled call"});
    };
}

Result<void> ServerCall::send_headers(const Metadata& headers) {
    auto stream = stream_;
    auto owner = stream->owner.lock();
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    if (!owner || owner->stopped || stream->terminal || stream->ending || stream->sent_header || stream->sent_message)
        return std::unexpected(Status{Code::failed_precondition, "response headers already closed"});
    stream->sent_header = true;
    auto envelope = detail::frame(stream->id, wire::KIND_HEADER);
    envelope.mutable_headers()->insert(headers.begin(), headers.end());
    if (!owner->transmit(envelope)) return std::unexpected(Status{Code::unavailable, "connection closed"});
    return {};
}

Result<void> ServerCall::write(std::string message) {
    auto stream = stream_;
    auto owner = stream->owner.lock();
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    if (!owner || owner->stopped || stream->terminal || stream->ending || (stream->sent_message && stream->single_response()))
        return std::unexpected(Status{Code::failed_precondition, "response side closed"});
    const auto& limits = owner->limits;
    if (message.size() > INT_MAX || ((stream->credit <= 0 || stream->flushing) &&
            message.size() > limits.max_send_buffer - std::min(stream->queued_out, limits.max_send_buffer)) ||
            (limits.max_queued_messages && stream->responses.size() >= limits.max_queued_messages)) {
        Status status{Code::resource_exhausted, "response backlog full"};
        owner->abort(stream, status);
        return std::unexpected(std::move(status));
    }
    stream->sent_message = true;
    stream->queued_out += message.size();
    stream->responses.push_back(std::move(message));
    owner->pump(stream);
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    return {};
}

void ServerCall::finish(Status status, Metadata trailers) {
    auto stream = stream_;
    if (stream->terminal || stream->ending) return;
    if (auto owner = stream->owner.lock()) {
        if (stream->single_response() && !stream->sent_message && status.code == Code::ok)
            status = {Code::internal, "missing response"};
        stream->ending = detail::end_frame(stream->id, status, trailers);
        owner->pump(stream);
        stream->wake();
    }
}

ServerRead::~ServerRead() {
    if (stream_->waiting == this) {
        stream_->reader = {};
        stream_->waiting = nullptr;
        if (auto owner = stream_->owner.lock()) owner->abort(stream_, {Code::cancelled, "handler read cancelled"});
    }
}
bool ServerRead::await_ready() const { return stream_->reader || stream_->readable(); }
void ServerRead::await_suspend(std::coroutine_handle<> continuation) {
    stream_->waiting = this;
    stream_->reader = continuation;
}
Result<std::optional<std::string>> ServerRead::await_resume() {
    auto stream = stream_;
    if (stream->reader) return std::unexpected(Status{Code::failed_precondition, "read already pending"});
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    if (stream->requests.empty() || stream->ending) return std::nullopt;
    auto bytes = std::move(stream->requests.front());
    stream->requests.pop_front();
    stream->queued_in -= bytes.size();
    if (auto owner = stream->owner.lock()) {
        stream->consumed += static_cast<std::uint32_t>(bytes.size());
        if (stream->consumed >= std::max(1u, owner->limits.initial_window / 2)) {
            auto update = detail::frame(stream->id, wire::KIND_WINDOW_UPDATE);
            update.set_window(std::exchange(stream->consumed, 0));
            owner->transmit(update);
        }
    }
    return std::optional(std::move(bytes));
}
} // namespace ws_proto
