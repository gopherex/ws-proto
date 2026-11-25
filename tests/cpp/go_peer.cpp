#include "echo_server.hpp"

#include <array>
#include <iostream>

// The Go FrameConn exchanges complete WebSocket messages via length-prefixed stdio.
int main() {
    bool closed = false;
    ws_proto::ServerSession session([](std::string bytes) {
        auto size = static_cast<std::uint32_t>(bytes.size());
        std::array<char, 4> header{char(size >> 24), char(size >> 16), char(size >> 8), char(size)};
        return bool(std::cout.write(header.data(), 4).write(bytes.data(), bytes.size()).flush());
    }, [&] { closed = true; });
    echo_fixture::register_echo(session);
    session.opened("wsrpc.v1");
    std::array<unsigned char, 4> header;
    while (!closed && std::cin.read(reinterpret_cast<char*>(header.data()), 4)) {
        std::uint32_t size = 0;
        for (auto octet : header) size = (size << 8) | octet;
        if (size > 4 * 1024 * 1024) return 1;
        std::string bytes(size, '\0');
        if (!std::cin.read(bytes.data(), size)) return 1;
        session.receive(bytes);
    }
    session.disconnected();
}
