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

static void CancelFromSignal(benchmark::State& state) {
    ws_proto::Session session([](std::string data) { benchmark::DoNotOptimize(data); return true; }, [] {});
    session.opened("wsrpc.v1");
    for (auto _ : state) {
        ws_proto::SignalSource stop;
        auto call = session.start("/bench.Service/Method", {.cancellation = stop.signal()}, ws_proto::MethodKind::bidi);
        auto observed = call.finished().subscribe([&] { benchmark::DoNotOptimize(call.trailers().size()); });
        stop.notify();
    }
}
BENCHMARK(CancelFromSignal);

static void FlushBeforeOpen(benchmark::State& state) {
    for (auto _ : state) {
        ws_proto::Session session([](std::string data) { benchmark::DoNotOptimize(data); return true; }, [] {});
        auto call = session.start("/bench.Service/Method", {}, ws_proto::MethodKind::client_streaming);
        if (!call.send("payload")) state.SkipWithError("request refused");
        call.close_send();
        session.opened("wsrpc.v1");
    }
}
BENCHMARK(FlushBeforeOpen);
BENCHMARK_MAIN();
