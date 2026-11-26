#include "ws_proto/client.hpp"
#include "wire.hpp"

#include <algorithm>
#include <climits>
#include <deque>
#include <unordered_map>

namespace ws_proto {
namespace wire = ::wsproto::transport::v1;

struct Signal::State {
    bool ready = false;
    std::size_t next = 0;
    std::map<std::size_t, std::function<void()>> callbacks;
};

bool Signal::ready() const { return raised_ || (state_ && state_->ready); }

Subscription Signal::subscribe(std::function<void()> callback) const {
    if (ready()) { callback(); return {}; }
    if (!state_) return {};
    auto id = ++state_->next;
    state_->callbacks.emplace(id, std::move(callback));
    return Subscription([weak = std::weak_ptr(state_), id] {
        if (auto state = weak.lock()) state->callbacks.erase(id);
    });
}

Signal SignalSource::signal() const {
    if (!signal_.raised_ && !signal_.state_) signal_.state_ = std::make_shared<Signal::State>();
    return signal_;
}

void SignalSource::notify() {
    if (signal_.raised_) return;
    signal_.raised_ = true;
    auto state = std::move(signal_.state_);
    if (!state) return;
    state->ready = true;
    while (!state->callbacks.empty())
        state->callbacks.extract(state->callbacks.begin()).mapped()();
}

namespace detail {
struct Stream {
    std::weak_ptr<Connection> connection;
    std::uint32_t id = 0;
    MethodKind kind;
    bool single() const { return kind == MethodKind::unary || kind == MethodKind::client_streaming; }
    bool opened = false;
    bool closing = false;
    bool half_closed = false;
    SignalSource leading, requests_closed, finished;
    bool received_response = false;
    std::optional<SignalSource> response_started;

    void mark_response() {
        if (std::exchange(received_response, true)) return;
        if (response_started) response_started->notify();
    }
    Subscription cancellation, header_hook, trailer_hook;
    std::optional<Status> terminal;
    std::optional<Clock::time_point> deadline;
    Metadata headers, trailers;
    std::function<bool(const std::string&)> decode_upload;
    std::deque<std::string> incoming;
    std::deque<wire::Frame> outgoing;
    std::size_t incoming_bytes = 0, outgoing_bytes = 0;
    std::int64_t credit = 0;
    std::uint32_t consumed = 0;
    std::coroutine_handle<> reader;
    ReadOperation* waiting = nullptr;

    explicit Stream(MethodKind kind, Options& options) : kind(kind), deadline(options.deadline) {
        if (options.on_header) header_hook = leading.signal().subscribe(
            [this, hook = std::move(options.on_header)] { hook(headers); });
        if (options.on_trailer) trailer_hook = finished.signal().subscribe(
            [this, hook = std::move(options.on_trailer)] { hook(trailers); });
    }

    void wake() {
        if (reader && (terminal || (!single() && !incoming.empty()))) {
            waiting = nullptr;
            std::exchange(reader, {}).resume();
        }
    }

    void set_terminal(Status status, bool discard) {
        terminal = std::move(status);
        outgoing.clear();
        outgoing_bytes = 0;
        if (discard || (single() && terminal->code != Code::ok)) {
            incoming.clear();
            incoming_bytes = 0;
        }
        cancellation.reset();
    }

    void notify_finished() {
        leading.notify();
        requests_closed.notify();
        finished.notify();
        wake();
    }

    void finish(Status status, bool discard) {
        if (terminal) return;
        set_terminal(std::move(status), discard);
        notify_finished();
    }
};

struct Connection {
    std::function<bool(std::string)> send;
    std::function<void()> close;
    Limits limits;
    Now now;
    bool open = false;
    std::optional<SessionEnd> end;
    std::function<void(const SessionEnd&)> on_end;
    std::deque<wire::Frame> pending;
    std::size_t pending_bytes = 0, credit_blocked_before_open = 0;
    bool flushing = false;
    std::uint64_t next_id = 1;
    std::unordered_map<std::uint32_t, std::shared_ptr<Stream>> streams;

    void stop(Status reason, bool close_host, bool retryable = false) {
        if (end) return;
        end = SessionEnd{reason, retryable};
        pending.clear();
        pending_bytes = credit_blocked_before_open = 0;
        open = false;
        auto active = std::exchange(streams, {});
        if (close_host) close();
        for (auto& [id, stream] : active) stream->finish(reason, false);
        if (on_end) on_end(*end);
    }

    bool write(const wire::Frame& envelope) {
        if (send(envelope.SerializeAsString())) return true;
        stop({Code::unavailable, "host refused WebSocket message"}, true, true);
        return false;
    }

    bool fits_before_open(std::size_t bytes) {
        if (bytes <= limits.max_preopen_bytes - pending_bytes - credit_blocked_before_open) return true;
        stop({Code::unavailable, "send buffer overflow before connect"}, true);
        return false;
    }

    bool transmit(wire::Frame&& envelope) {
        if (end) return false;
        if (open && !flushing) return write(envelope);
        auto size = envelope.ByteSizeLong();
        if (!fits_before_open(size)) return false;
        pending_bytes += size;
        pending.push_back(std::move(envelope));
        return true;
    }

    void flush() {
        credit_blocked_before_open = 0;
        open = true;
        flushing = true;
        while (!pending.empty()) {
            auto frame = std::move(pending.front());
            pending.pop_front();
            pending_bytes -= frame.ByteSizeLong();
            auto active = streams.find(frame.stream_id());
            if (active == streams.end() && frame.kind() != wire::KIND_RST) continue;
            if (frame.kind() == wire::KIND_OPEN) {
                auto stream = active->second;
                if (stream->deadline) {
                    auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*stream->deadline - now());
                    if (remaining.count() <= 0) {
                        reset(stream, {Code::deadline_exceeded, "deadline exceeded"});
                        continue;
                    }
                    (*frame.mutable_headers())["ws-timeout-ms"] = std::to_string(remaining.count());
                }
                stream->opened = true;
            }
            if (!write(frame)) break;
        }
        flushing = false;
    }

    void reset(std::shared_ptr<Stream> stream, Status status) {
        if (stream->terminal || !streams.erase(stream->id)) return;
        if (!pending.empty()) {
            std::erase_if(pending, [&](const wire::Frame& frame) {
                if (frame.stream_id() != stream->id) return false;
                pending_bytes -= frame.ByteSizeLong();
                return true;
            });
        }
        if (!open) {
            for (const auto& queued : stream->outgoing) credit_blocked_before_open -= queued.ByteSizeLong();
        }
        // A synchronous host can call back into the RPC while accepting RST.
        stream->set_terminal(std::move(status), true);
        if (stream->opened && !end) {
            auto envelope = frame(stream->id, wire::KIND_RST);
            write_status(envelope, *stream->terminal);
            transmit(std::move(envelope));
        }
        stream->notify_finished();
    }

    void consumed(Stream& stream, std::size_t count) {
        stream.consumed += static_cast<std::uint32_t>(count);
        if (stream.consumed < std::max(1u, limits.initial_window / 2)) return;
        auto update = frame(stream.id, wire::KIND_WINDOW_UPDATE);
        update.set_window(std::exchange(stream.consumed, 0));
        transmit(std::move(update));
    }

    void pump(std::shared_ptr<Stream> stream) {
        while (!stream->terminal && stream->credit > 0 && !stream->outgoing.empty()) {
            auto envelope = std::move(stream->outgoing.front());
            stream->outgoing.pop_front();
            stream->outgoing_bytes -= envelope.payload().size();
            stream->credit -= static_cast<std::int64_t>(envelope.payload().size());
            if (!transmit(std::move(envelope))) return;
        }
        if (!stream->terminal && stream->closing && !stream->half_closed && stream->outgoing.empty()) {
            stream->half_closed = true;
            transmit(frame(stream->id, wire::KIND_HALF_CLOSE));
        }
    }
};
} // namespace detail

using detail::frame;

Session::Session(std::function<bool(std::string)> send, std::function<void()> close, Limits limits,
                 std::function<void(const SessionEnd&)> on_end)
    : Session(std::move(send), std::move(close), limits, std::move(on_end), Clock::now) {}

Session::Session(std::function<bool(std::string)> send, std::function<void()> close, Limits limits,
                 std::function<void(const SessionEnd&)> on_end, Now now)
    : connection_(std::make_shared<detail::Connection>()) {
    connection_->now = std::move(now);
    connection_->send = std::move(send);
    connection_->close = std::move(close);
    connection_->on_end = std::move(on_end);
    if (!limits.initial_window) limits.initial_window = Limits{}.initial_window;
    connection_->limits = limits;
}

Session::~Session() { connection_->stop({Code::cancelled, "session destroyed"}, true); }

void Session::opened(std::string_view protocol) {
    auto connection = connection_;
    if (connection->end) return;
    if (protocol != "wsrpc.v1") connection->stop({Code::unavailable, "expected wsrpc.v1"}, true);
    else if (!connection->open) {
        expire();
        if (!connection->end) connection->flush();
    }
}

void Session::disconnected() {
    auto connection = connection_;
    connection->stop({Code::unavailable, "connection lost"}, false, true);
}

bool Session::is_open() const { return connection_->open; }
const std::optional<SessionEnd>& Session::end() const { return connection_->end; }
const Limits& Session::limits() const { return connection_->limits; }

Call Session::start(std::string_view method, Options options, MethodKind kind) {
    return start_call(method, std::move(options), kind, {});
}

Call Session::start_call(std::string_view method, Options options, MethodKind kind,
                         std::function<bool(const std::string&)> decode_upload) {
    auto connection = connection_;
    auto stream = std::make_shared<detail::Stream>(kind, options);
    stream->connection = connection;
    stream->decode_upload = std::move(decode_upload);
    if (connection->end || options.cancellation.ready()) {
        stream->finish(connection->end ? connection->end->status : Status{Code::cancelled, "cancelled"}, true);
        return Call(stream);
    }
    if (connection->next_id > UINT32_MAX) {
        stream->finish({Code::resource_exhausted, "stream ids exhausted"}, true);
        return Call(stream);
    }
    if (stream->deadline) {
        auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*stream->deadline - connection->now());
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
    stream->opened = connection->open && !connection->flushing;
    if (options.cancellation) {
        stream->cancellation = options.cancellation.subscribe([weak = std::weak_ptr(stream)] {
            if (auto active = weak.lock())
                if (auto owner = active->connection.lock()) owner->reset(active, {Code::cancelled, "cancelled"});
        });
    }
    connection->transmit(std::move(envelope));
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
        if (!stream->leading.ready()) stream->headers.insert(envelope.headers().begin(), envelope.headers().end());
        stream->mark_response();
        stream->leading.notify();
        return;
    case wire::KIND_WINDOW_UPDATE:
        stream->credit = std::min<std::int64_t>(UINT32_MAX, stream->credit + envelope.window());
        connection->pump(stream);
        return;
    case wire::KIND_MSG: {
        stream->mark_response();
        if (stream->terminal) return;
        auto size = envelope.payload().size();
        if (stream->kind == MethodKind::client_streaming) {
            stream->incoming.clear();
            stream->incoming_bytes = 0;
        }
        if (stream->kind == MethodKind::unary && !stream->incoming.empty()) {
            connection->reset(stream, {Code::internal, "multiple unary responses"});
            return;
        }
        if (stream->incoming_bytes >= connection->limits.initial_window
            || (connection->limits.max_queued_messages && stream->incoming.size() >= connection->limits.max_queued_messages)) {
            connection->reset(stream, {Code::resource_exhausted, "receive backlog full"});
            return;
        }
        if (stream->decode_upload) {
            if (!stream->decode_upload(envelope.payload())) {
                connection->reset(stream, {Code::data_loss, "invalid protobuf response"});
                return;
            }
            // The typed result owns the decoded message. Keep only its presence until END.
            stream->incoming.emplace_back();
        } else {
            stream->incoming_bytes += size;
            stream->incoming.push_back(std::move(*envelope.mutable_payload()));
        }
        stream->leading.notify();
        // Single-result calls consume each response immediately but publish only after END.
        if (stream->single() && !stream->terminal) connection->consumed(*stream, size);
        stream->wake();
        return;
    }
    case wire::KIND_RST:
    case wire::KIND_END: {
        connection->streams.erase(it);
        stream->trailers.insert(envelope.headers().begin(), envelope.headers().end());
        auto status = detail::peer_status(envelope.status());
        const bool reset = envelope.kind() == wire::KIND_RST;
        if (reset && status.code == Code::ok) {
            status.code = Code::cancelled;
            if (status.message.empty()) status.message = "peer reset stream";
        }
        if (stream->single() && status.code == Code::ok && stream->incoming.empty())
            status = {Code::internal, "missing unary response"};
        stream->finish(std::move(status), false);
        return;
    }
    default: return;
    }
}

void Session::expire() { expire(connection_->now()); }

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

Call::~Call() { cancel(); }
Call& Call::operator=(Call&& other) noexcept {
    if (this != &other) {
        cancel();
        stream_ = std::move(other.stream_);
    }
    return *this;
}

static void cancel_call(const std::shared_ptr<detail::Stream>& stream) {
    if (stream && !stream->terminal)
        if (auto owner = stream->connection.lock()) owner->reset(stream, {Code::cancelled, "cancelled"});
}

void Call::cancel() { cancel_call(stream_); }

std::function<void()> Call::canceller() const {
    return [weak = std::weak_ptr(stream_)] { cancel_call(weak.lock()); };
}

void detail::RequestSide::fail(Status status) {
    if (auto connection = stream_->connection.lock())
        connection->reset(stream_, std::move(status));
}

Result<void> detail::RequestSide::send(std::string payload) {
    auto stream = stream_;
    auto connection = stream->connection.lock();
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    if (connection && connection->end && !stream->terminal) return std::unexpected(connection->end->status);
    if (!connection || stream->closing || stream->terminal)
        return std::unexpected(Status{Code::failed_precondition, "request side closed"});
    if ((stream->credit <= 0 && payload.size() > connection->limits.max_send_buffer -
           std::min(stream->outgoing_bytes, connection->limits.max_send_buffer))
        || (connection->limits.max_queued_messages && stream->outgoing.size() >= connection->limits.max_queued_messages)
        || payload.size() > INT_MAX) {
        Status status{Code::resource_exhausted, "send backlog full"};
        connection->reset(stream, status);
        return std::unexpected(status);
    }
    auto message = frame(stream->id, wire::KIND_MSG);
    message.set_payload(std::move(payload));
    if (!connection->open && stream->credit <= 0) {
        auto size = message.ByteSizeLong();
        if (!connection->fits_before_open(size)) return std::unexpected(connection->end->status);
        connection->credit_blocked_before_open += size;
    }
    stream->outgoing_bytes += message.payload().size();
    stream->outgoing.push_back(std::move(message));
    connection->pump(stream);
    if (stream->terminal && stream->terminal->code != Code::ok) return std::unexpected(*stream->terminal);
    return {};
}

Signal detail::RequestSide::requests_closed() const { return stream_->requests_closed.signal(); }
void detail::RequestSide::close_send() {
    auto stream = stream_;
    if (stream->closing || stream->terminal) return;
    stream->closing = true;
    stream->requests_closed.notify();
    if (auto connection = stream->connection.lock()) connection->pump(stream);
}
const Metadata& Call::headers() const { return stream_->headers; }
const Metadata& Call::trailers() const { return stream_->trailers; }
Signal Call::headers_ready() const { return stream_->leading.signal(); }
Signal Call::finished() const { return stream_->finished.signal(); }
ReadOperation Call::read_without_credit() const { return ReadOperation(stream_, false); }
void Call::return_credit(std::size_t bytes) {
    auto stream = stream_;
    if (!stream->terminal && !stream->single())
        if (auto owner = stream->connection.lock()) owner->consumed(*stream, bytes);
}
Signal Call::response_started() const {
    if (!stream_->response_started) stream_->response_started.emplace();
    if (stream_->received_response) stream_->response_started->notify();
    return stream_->response_started->signal();
}
const std::optional<Status>& Call::status() const { return stream_->terminal; }

Call Call::completed(MethodKind kind, std::vector<std::string> responses, Status status, Options options,
                     Metadata headers, Metadata trailers) {
    auto stream = std::make_shared<detail::Stream>(kind, options);
    stream->headers = std::move(headers);
    stream->trailers = std::move(trailers);
    bool cancelled = options.cancellation.ready();
    bool expired = options.deadline && *options.deadline <= Clock::now();
    if (cancelled) status = {Code::cancelled, "cancelled"};
    else if (expired) status = {Code::deadline_exceeded, "deadline exceeded"};
    else if (status.code == Code::ok && stream->single() &&
             (responses.empty() || (kind == MethodKind::unary && responses.size() != 1)))
        status = {Code::internal, "invalid number of responses"};
    if (kind == MethodKind::client_streaming && !responses.empty()) {
        stream->incoming.push_back(std::move(responses.back()));
    } else {
        for (auto& bytes : responses) stream->incoming.push_back(std::move(bytes));
    }
    for (const auto& bytes : stream->incoming) stream->incoming_bytes += bytes.size();
    stream->finish(std::move(status), cancelled || expired);
    return Call(stream);
}
ReadOperation Call::read() const { return ReadOperation(stream_); }

ReadOperation::ReadOperation(std::shared_ptr<detail::Stream> stream, bool return_credit)
    : stream_(std::move(stream)), return_credit_(return_credit) {}
ReadOperation::~ReadOperation() {
    if (stream_->waiting == this) {
        stream_->waiting = nullptr;
        stream_->reader = {};
        if (auto connection = stream_->connection.lock())
            connection->reset(stream_, {Code::cancelled, "reader cancelled"});
    }
}
bool ReadOperation::await_ready() const {
    return stream_->reader || stream_->terminal || (!stream_->single() && !stream_->incoming.empty());
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
        if (return_credit_ && !stream_->terminal && !stream_->single())
            if (auto connection = stream_->connection.lock()) connection->consumed(*stream_, bytes.size());
        return std::optional(std::move(bytes));
    }
    if (stream_->terminal && stream_->terminal->code != Code::ok) return std::unexpected(*stream_->terminal);
    return std::nullopt;
}

void ReadOperation::decode_failed() {
    Status error{Code::data_loss, "invalid protobuf response"};
    if (stream_->terminal) stream_->set_terminal(std::move(error), true);
    else if (auto connection = stream_->connection.lock()) connection->reset(stream_, std::move(error));
}

} // namespace ws_proto
