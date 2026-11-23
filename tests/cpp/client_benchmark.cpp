#include "ws_proto/client.hpp"
#include <benchmark/benchmark.h>

static void OpenAndCancel(benchmark::State& state) {
    ws_proto::Session session([](std::string data) { benchmark::DoNotOptimize(data); return true; }, [] {});
    session.opened("wsrpc.v1");
    for (auto _ : state) {
        auto call = session.start("/bench.Service/Method", {}, false);
        call.cancel();
    }
}
BENCHMARK(OpenAndCancel);
BENCHMARK_MAIN();
