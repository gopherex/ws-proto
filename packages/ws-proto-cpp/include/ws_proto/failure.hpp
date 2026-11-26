#pragma once

#include "ws_proto/client.hpp"

namespace ws_proto {

// The decoder belongs to the application and takes the complete terminal status.
template<class Error>
struct Failure {
    Status status;
    std::optional<Error> method_error;
};

template<class Value, class Decoder>
auto decode_failure(Result<Value> result, Decoder&& decode) {
    using Error = typename std::invoke_result_t<Decoder, const Status&>::value_type;
    using Outcome = std::expected<Value, Failure<Error>>;
    if (!result) {
        auto error = std::invoke(std::forward<Decoder>(decode), std::as_const(result.error()));
        return Outcome(std::unexpect, Failure<Error>{std::move(result.error()), std::move(error)});
    }
    if constexpr (std::is_void_v<Value>) return Outcome{};
    else return Outcome(std::in_place, std::move(*result));
}
} // namespace ws_proto
