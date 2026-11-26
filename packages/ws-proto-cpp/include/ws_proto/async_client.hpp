#pragma once

#include "ws_proto/asio.hpp"
#include "ws_proto/failure.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <deque>
#include <tuple>

namespace ws_proto::asio {

struct CallContext {
    Signal stop;
    std::optional<Clock::time_point> deadline;
};

struct AttemptFailure {
    const Status& status;
    const Metadata& trailers;
    std::size_t attempt;
};

// nullopt stops retries. A duration requests another attempt after that delay.
using RetryDecision = std::optional<Clock::duration>;

namespace detail {
template<class T> using Awaitable = net::awaitable<T>;
inline constexpr auto use = net::use_awaitable;

struct Event {
    Signal signal() {
        if (!source) source.emplace();
        return source->signal();
    }
    void notify() {
        if (source) {
            // Completions using async_wait are posted to the consuming executor.
            source->notify();
            source.reset();
        }
    }
    std::optional<SignalSource> source;
};

template<class Method, class Provider, class... Hooks>
struct AsyncState : std::enable_shared_from_this<AsyncState<Method, Provider, Hooks...>> {
    using Rpc = typename Method::CallType;
    using Request = typename Rpc::RequestType;
    using Response = typename Rpc::ResponseType;
    static constexpr bool uploads = Method::kind == MethodKind::bidi || Method::kind == MethodKind::client_streaming;
    static constexpr bool downloads = Method::kind == MethodKind::bidi || Method::kind == MethodKind::server_streaming;
    template<class Hook>
    static constexpr bool can_retry = requires(Hook& hook, Method method, const AttemptFailure& failure, CallContext ctx) {
        hook.attempt_failed(method, failure, ctx);
    };
    static constexpr bool retries = (can_retry<Hooks> || ...);

    net::any_io_executor executor;
    net::steady_timer deadline_timer, retry_timer;
    net::cancellation_signal run_cancel;
    Method method;
    std::shared_ptr<std::tuple<Provider, Hooks...>> chain;
    Options options;
    std::optional<Call> call;
    SignalSource stop, finished, leading;
    Subscription cancel_subscription, hook_cancel_subscription, response_subscription;
    Event changed;
    std::deque<std::pair<Response, std::size_t>> responses;
    std::size_t max_responses = 0;
    Metadata headers, trailers;
    std::optional<Status> stopping, terminal;
    std::vector<std::string> replay;
    std::size_t retry_bytes = 0, retry_limit = 0;
    bool committed = !retries, half_closed = false, active = false, closing = false;
    bool reading = false, writing = false, passed_open = false, commit_after_replay = false;

    AsyncState(net::any_io_executor ex, Method method, std::shared_ptr<std::tuple<Provider, Hooks...>> chain,
               Options options)
        : executor(std::move(ex)), deadline_timer(executor), retry_timer(executor), method(method),
          chain(std::move(chain)), options(std::move(options)) {}

    CallContext context() { return {stop.signal(), options.deadline}; }

    void commit() {
        if (std::exchange(committed, true)) return;
        std::vector<std::string>{}.swap(replay);
        retry_bytes = 0;
    }

    void accept_response() {
        commit();
        headers = call->headers();
        leading.notify();
        if (options.on_header) {
            try { options.on_header(headers); }
            catch (...) { cancel({Code::internal, "header callback threw"}); }
        }
        changed.notify();
    }

    void cancel(Status status) {
        if (stopping || terminal || closing) return;
        auto keep_alive = this->shared_from_this();
        stopping = std::move(status);
        stop.notify();
        retry_timer.cancel();
        if (call) call->cancel();
        run_cancel.emit(net::cancellation_type::terminal);
        changed.notify();
    }

    void arm_deadline() {
        deadline_timer.cancel();
        if (!options.deadline) return;
        if (*options.deadline <= Clock::now()) {
            cancel({Code::deadline_exceeded, "deadline exceeded"});
            return;
        }
        deadline_timer.expires_at(*options.deadline);
        deadline_timer.async_wait([weak = this->weak_from_this()](boost::system::error_code ec) {
            if (!ec)
                if (auto self = weak.lock()) self->cancel({Code::deadline_exceeded, "deadline exceeded"});
        });
    }

    template<std::size_t I = 1>
    Awaitable<Result<void>> open(Options opts) {
        if constexpr (I <= sizeof...(Hooks)) {
            auto& hook = std::get<I>(*chain);
            auto next = [this](Options next_options) { return open<I + 1>(std::move(next_options)); };
            if constexpr (requires { hook.open(method, opts, next, context()); }) {
                co_return co_await hook.open(method, std::move(opts), next, context());
            } else {
                co_return co_await next(std::move(opts));
            }
        } else {
            if (std::exchange(passed_open, true))
                co_return std::unexpected(Status{Code::failed_precondition, "open continuation already called"});
            if (stopping) co_return std::unexpected(*stopping);
            // The logical deadline can be shortened, including by an open hook.
            if (options.deadline && (!opts.deadline || *opts.deadline > *options.deadline))
                opts.deadline = options.deadline;
            hook_cancel_subscription = subscribe_cancellation(opts.cancellation);
            options = std::move(opts);
            arm_deadline();
            if (stopping) co_return std::unexpected(*stopping);
            co_return Result<void>{};
        }
    }

    template<class Hook>
    static constexpr bool sends = requires(Hook& hook, Method method, Request& message, CallContext ctx) {
        hook.send(method, message, ctx);
    };
    template<class Hook>
    static constexpr bool receives = requires(Hook& hook, Method method, Response& message, CallContext ctx) {
        hook.receive(method, message, ctx);
    };

    static constexpr bool intercept_sends = (sends<Hooks> || ...);
    static constexpr bool intercept_receives = (receives<Hooks> || ...);
    static constexpr bool process_responses = !downloads || intercept_receives;

    template<std::size_t I = 1>
    Awaitable<Result<void>> send_hooks(Request& message) {
        if constexpr (I <= sizeof...(Hooks)) {
            auto& hook = std::get<I>(*chain);
            if constexpr (sends<std::remove_reference_t<decltype(hook)>>) {
                auto result = co_await hook.send(method, message, context());
                if (!result || stopping) co_return result;
            }
            co_return co_await send_hooks<I + 1>(message);
        } else co_return Result<void>{};
    }

    template<std::size_t I = sizeof...(Hooks)>
    Awaitable<Result<void>> receive_hooks(Response& message) {
        if constexpr (I > 0) {
            auto& hook = std::get<I>(*chain);
            if constexpr (receives<std::remove_reference_t<decltype(hook)>>) {
                auto result = co_await hook.receive(method, message, context());
                if (!result || stopping) co_return result;
            }
            co_return co_await receive_hooks<I - 1>(message);
        } else co_return Result<void>{};
    }

    template<std::size_t I = sizeof...(Hooks)>
    Awaitable<Status> finish_hooks(Status status) {
        if constexpr (I > 0) {
            auto& hook = std::get<I>(*chain);
            if constexpr (requires { hook.finish(method, status, context()); }) {
                try { status = co_await hook.finish(method, status, context()); }
                catch (...) { status = {Code::internal, "finish interceptor threw"}; }
            }
            co_return co_await finish_hooks<I - 1>(std::move(status));
        } else co_return status;
    }

    template<std::size_t I = sizeof...(Hooks)>
    Awaitable<RetryDecision> retry_hooks(const AttemptFailure& failure) {
        if constexpr (I > 0) {
            auto& hook = std::get<I>(*chain);
            if constexpr (requires { hook.attempt_failed(method, failure, context()); }) {
                co_return co_await hook.attempt_failed(method, failure, context());
            } else co_return co_await retry_hooks<I - 1>(failure);
        } else co_return std::nullopt;
    }

    static Result<std::string> encode(const Request& request) {
        std::string bytes;
        if (!request.IsInitialized() || !request.SerializeToString(&bytes))
            return std::unexpected(Status{Code::invalid_argument, "uninitialized protobuf request"});
        return bytes;
    }

    void remember(const std::string& bytes) {
        if constexpr (retries) {
            if (committed) return;
            // Charge each entry as well as its payload, including empty messages.
            constexpr auto overhead = sizeof(std::string);
            if (retry_limit - retry_bytes < overhead || bytes.size() > retry_limit - retry_bytes - overhead) {
                commit();
                return;
            }
            replay.push_back(bytes);
            retry_bytes += bytes.size() + overhead;
        }
    }

    void make_attempt() {
        auto* session = std::invoke(std::get<0>(*chain));
        if (!session) return;
        if constexpr (retries) {
            retry_limit = session->limits().max_retry_buffer;
            if (retry_bytes > retry_limit) commit_after_replay = true;
        }

        max_responses = session->limits().max_queued_messages;
        auto opts = options;
        opts.on_header = {};
        opts.on_trailer = {};
        opts.cancellation = {};
        // The logical upload observes every response before retaining its final value.
        constexpr auto wire_kind = Method::kind == MethodKind::client_streaming ? MethodKind::bidi : Method::kind;
        call.emplace(session->start(method.path, std::move(opts), wire_kind));
        auto response = call->response_started();
        response_subscription = response.subscribe([weak = this->weak_from_this()] {
            if (auto self = weak.lock()) {
                net::post(self->executor, [weak] {
                    if (auto state = weak.lock(); state && state->call && state->call->response_started().ready() && !state->leading.ready())
                        state->accept_response();
                });
            }
        });
    }

    Awaitable<Status> receive_responses() {
        for (;;) {
            auto bytes = co_await detail::read(executor, *call, use, &Call::read_without_credit);
            if (!bytes || !*bytes) {
                auto status = call->status().value_or(Status{Code::internal, "missing terminal status"});
                if constexpr (!downloads)
                    if (status.code == Code::ok && responses.empty())
                        status = {Code::internal, "missing response"};
                co_return status;
            }
            Response response;
            if (!response.ParseFromString(**bytes))
                co_return Status{Code::data_loss, "invalid protobuf response"};
            if constexpr (intercept_receives) {
                auto accepted = co_await receive_hooks(response);
                if (!accepted) co_return std::move(accepted.error());
            }
            if (stopping) co_return *stopping;
            if constexpr (Method::kind == MethodKind::client_streaming) {
                call->return_credit((**bytes).size());
                responses.clear();
            }
            if (max_responses && responses.size() >= max_responses)
                co_return Status{Code::resource_exhausted, "intercepted response backlog full"};
            responses.emplace_back(std::move(response), (**bytes).size());
            changed.notify();
            if constexpr (Method::kind == MethodKind::unary)
                co_return call->status().value_or(Status{Code::internal, "missing terminal status"});
        }
    }

    Awaitable<Status> run(std::optional<Request> first) {
        if (stopping) co_return *stopping;
        auto opened = co_await open(options);
        if (!opened) co_return std::move(opened.error());
        if (!passed_open) co_return Status{Code::failed_precondition, "open continuation was not called"};
        std::optional<std::string> initial;
        if (first) {
            if constexpr (intercept_sends) {
                auto sent = co_await send_hooks(*first);
                if (!sent) co_return std::move(sent.error());
            }
            auto bytes = encode(*first);
            if (!bytes) co_return std::move(bytes.error());
            initial = std::move(*bytes);
        }
        for (std::size_t attempt = 1; !stopping; ++attempt) {
            make_attempt();
            Status status{Code::unavailable, "no session available"};
            Metadata attempt_trailers;
            if (call) {
                if (initial) {
                    remember(*initial);
                    static_cast<void>(call->send(std::move(*initial)));
                    initial.reset();
                    half_closed = true;
                } else {
                    // 'remember' is only called for messages provided by the application.
                    for (const auto& bytes : replay) {
                        if (!call->send(bytes)) break;
                    }
                }
                if (commit_after_replay) commit();
                if (half_closed) call->close_send();
                active = true;
                changed.notify();
                if constexpr (process_responses) {
                    auto done = call->finished().subscribe([this] { changed.notify(); });
                    while (!call->status() && !call->response_started().ready())
                        static_cast<void>(co_await async_wait(changed.signal(), use));
                    if (call->response_started().ready()) status = co_await receive_responses();
                    else {
                        status = *call->status();
                        if constexpr (!downloads)
                            if (status.code == Code::ok) status = {Code::internal, "missing response"};
                    }
                } else {
                    static_cast<void>(co_await async_wait(call->finished(), use));
                    status = call->status().value_or(Status{Code::internal, "attempt did not complete"});
                }
                active = false;
                if (call->response_started().ready() && !leading.ready()) accept_response();
                attempt_trailers = call->trailers();
            }
            if (stopping) co_return *stopping;
            if constexpr (retries) {
                if (!committed && status.code != Code::ok) {
                    response_subscription.reset();
                    call.reset();
                    auto retry = co_await retry_hooks({status, attempt_trailers, attempt});
                    if (stopping) co_return *stopping;
                    if (retry) {
                        const auto delay = std::max(Clock::duration::zero(), *retry);
                        const auto now = Clock::now();
                        retry_timer.expires_at(now + std::min(delay, Clock::time_point::max() - now));
                        co_await retry_timer.async_wait(use);
                        continue;
                    }
                }
            }
            trailers = std::move(attempt_trailers);
            co_return status;
        }
        co_return *stopping;
    }

    static Awaitable<void> supervise(std::shared_ptr<AsyncState> self, std::optional<Request> first) {
        Status status;
        try { status = co_await self->run(std::move(first)); }
        catch (...) { status = {Code::internal, "call interceptor threw"}; }
        self->active = false;
        self->closing = true;
        self->commit();
        self->response_subscription.reset();
        self->cancel_subscription.reset();
        self->hook_cancel_subscription.reset();
        self->deadline_timer.cancel();
        if (self->stopping) status = *self->stopping;
        if (self->call && !self->call->status()) self->call->cancel();
        co_await net::this_coro::reset_cancellation_state(net::disable_cancellation());
        self->stop.notify();
        self->terminal = co_await self->finish_hooks(std::move(status));
        if (!self->leading.ready()) {
            self->leading.notify();
            if (self->options.on_header) {
                try { self->options.on_header(self->headers); }
                catch (...) {}
            }
        }
        if (self->options.on_trailer) {
            try { self->options.on_trailer(self->trailers); }
            catch (...) {}
        }
        self->finished.notify();
        self->changed.notify();
    }

    Subscription subscribe_cancellation(const Signal& signal) {
        if (!signal) return {};
        return signal.subscribe([weak = this->weak_from_this()] {
            if (auto self = weak.lock()) self->cancel({Code::cancelled, "cancelled"});
        });
    }

    void start(std::optional<Request> first) {
        cancel_subscription = subscribe_cancellation(options.cancellation);
        arm_deadline();
        net::co_spawn(executor, supervise(this->shared_from_this(), std::move(first)),
            net::bind_cancellation_slot(run_cancel.slot(), [](std::exception_ptr error) {
                if (error) std::terminate();
            }));
    }

    Awaitable<Result<void>> write(Request request) {
        if (writing || half_closed || closing || stopping)
            co_return std::unexpected(Status{Code::failed_precondition, "write side closed or write pending"});
        writing = true;
        struct Reset { bool& flag; ~Reset() { flag = false; } } reset{writing};
        auto submit = [this](std::string bytes) -> Awaitable<Result<void>> {
            while (!closing && !stopping) {
                if (active && !call->status()) {
                    if (call->response_started().ready() && !committed) commit();
                    remember(bytes);
                    auto sent = call->send(std::move(bytes));
                    if constexpr (retries) {
                        if (!committed && call->status() && !stopping) co_return Result<void>{};
                    }
                    co_return sent;
                }
                static_cast<void>(co_await async_wait(changed.signal(), use));
            }
            co_return std::unexpected(stopping.value_or(Status{Code::failed_precondition, "write side closed"}));
        };
        try {
            if constexpr (intercept_sends) {
                auto sent = co_await send_hooks(request);
                if (!sent) {
                    cancel(sent.error());
                    co_return sent;
                }
            }
            auto bytes = encode(request);
            if (bytes) co_return co_await submit(std::move(*bytes));
            cancel(bytes.error());
            co_return std::unexpected(std::move(bytes.error()));
        } catch (...) {
            Status status{Code::internal, "send interceptor interrupted"};
            cancel(status);
            co_return std::unexpected(stopping.value_or(status));
        }
    }

    Result<void> close_send() {
        if (writing || stopping || closing)
            return std::unexpected(Status{Code::failed_precondition, "write side closed or write pending"});
        half_closed = true;
        if (active) call->close_send();
        return {};
    }

    Awaitable<Result<std::optional<Response>>> read() {
        if (reading) co_return std::unexpected(Status{Code::failed_precondition, "read already pending"});
        reading = true;
        struct Reset { bool& flag; ~Reset() { flag = false; } } reset{reading};
        for (;;) {
            if constexpr (process_responses) {
                if (!responses.empty() && (downloads || (terminal && terminal->code == Code::ok))) {
                    auto [response, size] = std::move(responses.front());
                    responses.pop_front();
                    if constexpr (downloads) call->return_credit(size);
                    co_return std::optional(std::move(response));
                }
            } else if (call && (terminal || (committed && active))) {
                auto bytes = co_await async_read(*call, use);
                if (bytes && *bytes) {
                    Response response;
                    if (!response.ParseFromString(**bytes)) {
                        Status status{Code::data_loss, "invalid protobuf response"};
                        cancel(status);
                        co_return std::unexpected(status);
                    }
                    co_return std::optional(std::move(response));
                }
                if (!terminal) static_cast<void>(co_await async_wait(finished.signal(), use));
            }
            if (terminal) {
                if (terminal->code != Code::ok) co_return std::unexpected(*terminal);
                co_return std::nullopt;
            }
            auto woke = co_await async_wait(changed.signal(), use);
            if (!woke) cancel({Code::cancelled, "read cancelled"});
        }
    }
};

template<class State>
class AsyncRpc {
    std::shared_ptr<State> state_;

    static Awaitable<Result<void>> finish_call(std::shared_ptr<State> state) {
        auto waited = co_await async_wait(state->finished.signal(), use);
        if (!waited) {
            state->cancel(waited.error());
            co_return waited;
        }
        if (state->terminal->code != Code::ok) co_return std::unexpected(*state->terminal);
        co_return Result<void>{};
    }
    static Awaitable<Result<typename State::Response>> result_call(std::shared_ptr<State> state) {
        auto response = co_await read_call(state);
        if (!response) co_return std::unexpected(std::move(response.error()));
        if (!*response) co_return std::unexpected(Status{Code::internal, "missing response"});
        co_return std::move(**response);
    }
    static Awaitable<Result<std::optional<typename State::Response>>> read_call(std::shared_ptr<State> state) {
        try { co_return co_await state->read(); }
        catch (const boost::system::system_error& error) {
            if (error.code() != net::error::operation_aborted) throw;
            Status status{Code::cancelled, "read cancelled"};
            state->cancel(status);
            co_return std::unexpected(std::move(status));
        }
    }
    template<class Next>
    static Awaitable<Result<void>> write_source(std::shared_ptr<State> state, Next next) {
        const auto stop = state->stop.signal();
        try {
            while (!stop.ready()) {
                auto item = co_await next(stop);
                if (stop.ready()) break;
                if (!item) {
                    state->cancel(item.error());
                    co_return std::unexpected(std::move(item.error()));
                }
                if (!*item) co_return state->close_send();
                auto written = co_await state->write(std::move(**item));
                if (!written) co_return written;
            }
            if (state->stopping) co_return std::unexpected(*state->stopping);
            co_return Result<void>{};
        } catch (...) {
            Status status{Code::internal, "request source interrupted"};
            state->cancel(status);
            co_return std::unexpected(state->stopping.value_or(status));
        }
    }
    static Awaitable<Result<void>> write_call(std::shared_ptr<State> state, typename State::Request request) {
        co_return co_await state->write(std::move(request));
    }
public:
    explicit AsyncRpc(std::shared_ptr<State> state) : state_(std::move(state)) {}
    AsyncRpc(AsyncRpc&&) = default;
    AsyncRpc& operator=(AsyncRpc&& other) noexcept {
        if (this != &other) {
            cancel();
            state_ = std::move(other.state_);
        }
        return *this;
    }
    ~AsyncRpc() { cancel(); }
    void cancel() { if (state_) state_->cancel({Code::cancelled, "cancelled"}); }
    auto write(typename State::Request request) requires State::uploads { return write_call(state_, std::move(request)); }
    template<class Next>
    auto write_from(Next next) requires State::uploads { return write_source(state_, std::move(next)); }
    auto close_send() requires State::uploads { return state_->close_send(); }
    auto read() const requires State::downloads { return read_call(state_); }
    auto result() const requires (!State::downloads) { return result_call(state_); }
    auto finish() const { return finish_call(state_); }
    const auto& get_executor() const { return state_->executor; }
    auto headers_ready() const { return state_->leading.signal(); }
    auto finished() const { return state_->finished.signal(); }
    const Metadata& headers() const { return state_->headers; }
    const Metadata& trailers() const { return state_->trailers; }
};

template<class Provider, class... Hooks>
class AsyncChannel {
    net::any_io_executor executor_;
    std::shared_ptr<std::tuple<Provider, Hooks...>> chain_;
public:
    AsyncChannel(net::any_io_executor executor, Provider provider, Hooks... hooks)
        : executor_(std::move(executor)),
          chain_(std::make_shared<std::tuple<Provider, Hooks...>>(std::move(provider), std::move(hooks)...)) {}

    template<class Method, class... Args>
    auto invoke(Method method, Options options, const Args&... args) {
        using State = AsyncState<Method, Provider, Hooks...>;
        auto state = std::make_shared<State>(executor_, method, chain_, std::move(options));
        if constexpr (sizeof...(Args)) state->start(std::optional<typename State::Request>(args...));
        else state->start(std::nullopt);
        return AsyncRpc<State>(std::move(state));
    }
};
} // namespace detail

// Provider returns the session for this attempt, or nullptr if unavailable.
template<class Provider, class... Hooks>
auto intercept_async(boost::asio::any_io_executor executor, Provider provider, Hooks... hooks) {
    return detail::AsyncChannel<Provider, Hooks...>(std::move(executor), std::move(provider), std::move(hooks)...);
}

template<class Value, class Decoder>
auto with_failure(boost::asio::awaitable<Result<Value>> operation, Decoder decoder)
    -> boost::asio::awaitable<decltype(decode_failure(std::declval<Result<Value>>(), decoder))> {
    co_return decode_failure(co_await std::move(operation), decoder);
}

namespace detail {
template<class Rpc, class Decoder>
class DecodedRpc {
    Rpc rpc_;
    Decoder decode_;
public:
    DecodedRpc(Rpc rpc, Decoder decode) : rpc_(std::move(rpc)), decode_(std::move(decode)) {}

    auto result() const requires requires(const Rpc& rpc) { rpc.result(); } {
        return with_failure(rpc_.result(), decode_);
    }
    auto read() const requires requires(const Rpc& rpc) { rpc.read(); } {
        return with_failure(rpc_.read(), decode_);
    }
    auto finish() const { return with_failure(rpc_.finish(), decode_); }

    template<class Request>
    auto write(Request&& request) requires requires(Rpc& rpc) { rpc.write(std::forward<Request>(request)); } {
        return rpc_.write(std::forward<Request>(request));
    }
    template<class Next>
    auto write_from(Next next) requires requires(Rpc& rpc) { rpc.write_from(std::move(next)); } {
        return rpc_.write_from(std::move(next));
    }
    auto close_send() requires requires(Rpc& rpc) { rpc.close_send(); } { return rpc_.close_send(); }
    void cancel() { rpc_.cancel(); }
    const auto& get_executor() const { return rpc_.get_executor(); }
    auto headers_ready() const { return rpc_.headers_ready(); }
    auto finished() const { return rpc_.finished(); }
    const Metadata& headers() const { return rpc_.headers(); }
    const Metadata& trailers() const { return rpc_.trailers(); }
};

template<class Channel, class Select>
struct FailureChannel {
    Channel& next;
    Select select;

    template<class Method, class... Args>
    auto invoke(Method method, Options options, const Args&... args) {
        if constexpr (requires { select(method); }) {
            auto decode = select(method);
            return DecodedRpc(next.invoke(method, std::move(options), args...), std::move(decode));
        } else {
            return next.invoke(method, std::move(options), args...);
        }
    }
};
} // namespace detail

// Select is only callable for methods that have an application error decoder.
template<class Channel, class Select>
auto with_failure_decoder(Channel& channel, Select select) {
    return detail::FailureChannel<Channel, Select>{channel, std::move(select)};
}

template<class Rpc>
auto async_finish(const Rpc& rpc) { return rpc.finish(); }

template<class Rpc, class Token>
    requires requires(Rpc& rpc) { rpc.get_executor(); rpc.read(); }
auto async_read(Rpc& rpc, Token&& token) {
    return boost::asio::co_spawn(rpc.get_executor(), rpc.read(), std::forward<Token>(token));
}

template<class Rpc, class Token>
    requires requires(Rpc& rpc) { rpc.get_executor(); rpc.result(); }
auto async_result(Rpc& rpc, Token&& token) {
    return boost::asio::co_spawn(rpc.get_executor(), rpc.result(), std::forward<Token>(token));
}

template<class Rpc, class Request, class Token>
auto async_write(Rpc& rpc, Request request, Token&& token) {
    return boost::asio::co_spawn(rpc.get_executor(), rpc.write(std::move(request)), std::forward<Token>(token));
}

template<class Rpc, class Token>
auto async_finish(Rpc& rpc, Token&& token) {
    return boost::asio::co_spawn(rpc.get_executor(), rpc.finish(), std::forward<Token>(token));
}

} // namespace ws_proto::asio
