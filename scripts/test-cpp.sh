#!/usr/bin/env bash
# Build the plugins on the host and execute the generated client in Firefox.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
: "${VCPKG_ROOT:?set VCPKG_ROOT to a bootstrapped vcpkg checkout}"
: "${EMSDK:?source emsdk_env.sh first}"
host=${VCPKG_HOST_TRIPLET:-x64-linux}
toolchain="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cd "$root"
vcpkg="$VCPKG_ROOT/vcpkg"
"$vcpkg" install --classic --host-triplet "$host" \
    "protobuf[libprotoc]:$host" "gtest:$host" "benchmark:$host" protobuf:wasm32-emscripten
common=(-G Ninja -DCMAKE_BUILD_TYPE=Release "-DCMAKE_TOOLCHAIN_FILE=$toolchain" "-DVCPKG_HOST_TRIPLET=$host")
cmake -S packages/ws-proto-cpp -B "$scratch/host" "${common[@]}" "-DVCPKG_TARGET_TRIPLET=$host" \
    -DWS_PROTO_BUILD_TOOLS=ON -DWS_PROTO_BUILD_TESTS=ON
cmake --build "$scratch/host"
ctest --test-dir "$scratch/host" --output-on-failure
"$scratch/host/client_benchmark" --benchmark_min_time=0.1s

mkdir "$scratch/schema"
cp -r example/proto example/protobuf.{mod,lock} tests/cpp/easyp.gen.yaml "$scratch/schema/"
(cd "$scratch/schema" && PATH="$scratch/host:$PATH" easyp generate --frozen)

wasm=("${common[@]}" -DVCPKG_TARGET_TRIPLET=wasm32-emscripten
    "-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$EMSDK/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake")
cmake -S packages/ws-proto-cpp -B "$scratch/runtime" "${wasm[@]}" "-DCMAKE_INSTALL_PREFIX=$scratch/prefix"
cmake --build "$scratch/runtime"
cmake --install "$scratch/runtime"
cmake -S tests/cpp/browser -B "$scratch/browser" "${wasm[@]}" \
    "-Dws-proto_DIR=$scratch/prefix/lib/cmake/ws-proto" "-DECHO_GEN=$scratch/schema/gen"
cmake --build "$scratch/browser"
scripts/with-mobile-server.sh node tests/cpp/browser/run.mjs "$scratch/browser"
