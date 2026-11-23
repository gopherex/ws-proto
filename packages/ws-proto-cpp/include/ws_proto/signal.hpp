#pragma once

#include <coroutine>
#include <functional>
#include <memory>
#include <utility>

namespace ws_proto {

// One-shot notification on the same serialized event loop as the session.
class Subscription {
public:
    Subscription() = default;
    explicit Subscription(std::function<void()> detach) : detach_(std::move(detach)) {}
    Subscription(Subscription&& other) noexcept : detach_(std::exchange(other.detach_, {})) {}
    Subscription& operator=(Subscription&& other) noexcept {
        if (this != &other) {
            reset();
            detach_ = std::exchange(other.detach_, {});
        }
        return *this;
    }
    ~Subscription() { reset(); }
    void reset() { if (detach_) std::exchange(detach_, {})(); }
private:
    std::function<void()> detach_;
};

class SignalSource;
class SignalWait;
class Signal {
public:
    Signal() = default;
    explicit operator bool() const { return raised_ || state_; }
    bool ready() const;
    [[nodiscard]] Subscription subscribe(std::function<void()> callback) const;
    SignalWait operator co_await() const;
private:
    friend class SignalSource;
    struct State;
    std::shared_ptr<State> state_;
    bool raised_ = false;
};

class SignalSource {
public:
    SignalSource() = default;
    SignalSource(const SignalSource&) = delete;
    SignalSource& operator=(const SignalSource&) = delete;
    bool ready() const { return signal_.ready(); }
    Signal signal() const;
    void notify();
private:
    mutable Signal signal_;
};

class SignalWait {
public:
    explicit SignalWait(Signal signal) : signal_(std::move(signal)) {}
    bool await_ready() const { return signal_.ready(); }
    void await_suspend(std::coroutine_handle<> continuation) {
        subscription_ = signal_.subscribe([continuation] { continuation.resume(); });
    }
    void await_resume() const {}
private:
    Signal signal_;
    Subscription subscription_;
};
inline SignalWait Signal::operator co_await() const { return SignalWait(*this); }

} // namespace ws_proto
