#include <ws_proto/async_client.hpp>
#include <ws_proto/testing.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <google/protobuf/empty.pb.h>

int main() {
    using namespace ws_proto;
    using Message = google::protobuf::Empty;
    using Probe = Rpc<Message, Message, true, true>;
    testing::Loopback pipe;
    pipe.server.register_method("/probe", MethodKind::bidi, [](ServerCall call) { call.finish(); });
    pipe.open();

    boost::asio::io_context loop;
    auto channel = asio::intercept_async(loop.get_executor(), [&] { return &pipe.client; });
    auto rpc = channel.invoke(Method<Probe>{"/probe"}, {});
    auto finished = asio::async_finish(rpc, boost::asio::use_future);
    loop.poll();
    pipe.drain();
    loop.run();
    return finished.get().has_value() ? 0 : 1;
}
