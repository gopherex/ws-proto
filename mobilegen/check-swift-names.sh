#!/bin/sh
# Compile generated names against the real SwiftProtobuf and WsProto modules.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM

protoc=${PROTOC:-protoc}
swift=${SWIFT:-swift}
swiftc=${SWIFTC:-swiftc}
plugin=${PROTOC_GEN_SWIFT:-$(command -v protoc-gen-swift)}
modules=${SWIFT_MODULE_PATH:-$("$swift" build --show-bin-path)/Modules}

go build -o "$work/protoc-gen-ws-swift" ./cmd/protoc-gen-ws-swift
"$protoc" -I mobilegen/testdata/naming \
  --plugin="protoc-gen-swift=$plugin" \
  --plugin="protoc-gen-ws-swift=$work/protoc-gen-ws-swift" \
  --swift_out="Visibility=Public:$work" \
  --ws-swift_out="$work" \
  messages.proto service.proto
"$swiftc" -typecheck -swift-version 6 -I "$modules" "$work"/*.swift
