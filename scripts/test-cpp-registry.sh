#!/usr/bin/env bash
# Consume the versioned ports from Git, without source overrides or overlay ports.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
: "${VCPKG_ROOT:?set VCPKG_ROOT to a bootstrapped vcpkg checkout}"
: "${EMSDK:?source emsdk_env.sh first}"
host=${VCPKG_HOST_TRIPLET:-x64-linux}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cd "$scratch"
if [[ ${1:-} == --local ]]; then
    # In a checkout with unpublished commits, redirect just the registry's remote.
    git init --quiet --bare -b master mirror.git
    git -C mirror.git fetch --quiet "$root" HEAD:refs/heads/master
    export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="url.file://$scratch/mirror.git.insteadOf"
    export GIT_CONFIG_VALUE_0="https://github.com/gopherex/ws-proto.git"
    export VCPKG_KEEP_ENV_VARS="GIT_CONFIG_COUNT;GIT_CONFIG_KEY_0;GIT_CONFIG_VALUE_0"
    export X_VCPKG_REGISTRIES_CACHE="$scratch/registry-cache"
    mkdir "$X_VCPKG_REGISTRIES_CACHE"
fi
cp "$root"/tests/cpp/registry/*.json "$root"/tests/cpp/browser/{CMakeLists.txt,index.html,main.cpp} .
cp -r "$root/example/proto" "$root"/example/protobuf.{mod,lock} "$root/tests/cpp/easyp.gen.yaml" .
"$VCPKG_ROOT/vcpkg" install --triplet wasm32-emscripten --host-triplet "$host"
PATH="$scratch/vcpkg_installed/$host/tools/protoc-gen-ws-cpp:$PATH" easyp generate --frozen
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release "-DECHO_GEN=$scratch/gen" \
    "-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
    "-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$EMSDK/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake" \
    "-DVCPKG_INSTALLED_DIR=$scratch/vcpkg_installed" "-DVCPKG_HOST_TRIPLET=$host" \
    -DVCPKG_TARGET_TRIPLET=wasm32-emscripten -DVCPKG_MANIFEST_INSTALL=OFF
cmake --build build
"$root/scripts/with-mobile-server.sh" node "$root/tests/cpp/browser/run.mjs" "$scratch/build"
