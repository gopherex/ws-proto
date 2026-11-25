#include <ws_proto/asio.hpp>
#include <ws_proto/testing.hpp>
#include <boost/asio/io_context.hpp>

int main() {
    ws_proto::testing::Loopback pipe;
    pipe.server.register_method("/probe", ws_proto::MethodKind::bidi,
        [](ws_proto::ServerCall call) { call.finish(); });
    pipe.open();
    auto rpc = pipe.client.start("/probe", {}, ws_proto::MethodKind::bidi);
    boost::asio::io_context loop;
    bool completed = false;
    ws_proto::asio::async_wait(loop.get_executor(), rpc.finished(),
        [&](ws_proto::Result<void> r) { completed = r.has_value(); });
    pipe.drain();
    loop.run();
    return completed && rpc.finished().ready() ? 0 : 1;
}
