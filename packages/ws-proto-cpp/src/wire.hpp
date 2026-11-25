#pragma once

#include "ws_proto/client.hpp"
#include "transport/transport.pb.h"

namespace ws_proto::detail {
namespace wire = ::wsproto::transport::v1;

inline wire::Frame frame(std::uint32_t id, wire::Kind kind) {
    wire::Frame value;
    value.set_stream_id(id);
    value.set_kind(kind);
    return value;
}

inline Status peer_status(const wire::Status& value) {
    return {static_cast<Code>(value.code()), value.message(), {value.details().begin(), value.details().end()}};
}

inline void write_status(wire::Frame& envelope, const Status& status) {
    auto* value = envelope.mutable_status();
    value->set_code(static_cast<int>(status.code));
    value->set_message(status.message);
    for (const auto& item : status.details) value->add_details(item);
}
} // namespace ws_proto::detail
