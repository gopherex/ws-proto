#pragma once

#include "ws_proto/server.hpp"

#include <array>
#include <deque>

namespace ws_proto::testing {

enum class Direction { to_server, to_client };

// Both sessions live on the calling thread. Only step()/drain() deliver events.
class Loopback {
    struct Event { std::uint64_t sequence; Clock::time_point due; std::string frame; };
    struct Lane {
        bool held = false;
        Clock::duration delay{};
        std::deque<Event> queue;
    };
    Clock::time_point now_{};
    std::array<Lane, 2> lanes_;
    std::uint64_t sequence_ = 0;
    std::optional<std::string> opening_;
    bool closed_ = false, closing_ = false, delivering_ = false;
    Lane& lane(Direction direction) { return lanes_[static_cast<std::size_t>(direction)]; }

public:
    explicit Loopback(Limits limits = {})
        : client([this](std::string frame) { return inject(Direction::to_server, std::move(frame)); },
                 [this] { disconnect(); }, limits, {}, [this] { return now_; }),
          server([this](std::string frame) { return inject(Direction::to_client, std::move(frame)); },
                 [this] { disconnect(); }, limits, [this] { return now_; }) {}

    Session client;
    ServerSession server;
    Clock::time_point now() const { return now_; }
    void open(std::string protocol = "wsrpc.v1") {
        if (!closed_ && !server.is_open()) opening_ = std::move(protocol);
    }
    void disconnect() {
        if (closed_) return;
        closed_ = closing_ = true;
        opening_.reset();
        for (auto& lane : lanes_) lane.queue.clear();
    }
    void hold(Direction direction, bool held = true) { lane(direction).held = held; }
    void delay(Direction direction, Clock::duration delay) { lane(direction).delay = delay; }
    bool inject(Direction direction, std::string frame) {
        if (closed_) return false;
        auto& target = lane(direction);
        target.queue.push_back({sequence_++, now_ + target.delay, std::move(frame)});
        return true;
    }
    void advance(Clock::duration elapsed) {
        if (elapsed < Clock::duration::zero()) return;
        now_ += elapsed;
        server.expire(now_);
        client.expire(now_);
    }
    bool step() {
        if (delivering_) return false;
        struct Guard {
            bool& active;
            explicit Guard(bool& flag) : active(flag) { active = true; }
            ~Guard() { active = false; }
        } guard(delivering_);
        if (closing_) {
            closing_ = false;
            client.disconnected();
            server.disconnected();
            return true;
        }
        if (opening_) {
            auto protocol = *std::exchange(opening_, {});
            server.opened(protocol);
            client.opened(protocol);
            return true;
        }
        Lane* next = nullptr;
        for (auto& lane : lanes_) {
            if (lane.held || lane.queue.empty() || lane.queue.front().due > now_) continue;
            if (!next || lane.queue.front().sequence < next->queue.front().sequence) next = &lane;
        }
        if (!next) return false;
        auto frame = std::move(next->queue.front().frame);
        next->queue.pop_front();
        if (next == &lane(Direction::to_server)) server.receive(frame);
        else client.receive(frame);
        return true;
    }
    void drain() { while (step()) {} }
};
} // namespace ws_proto::testing
