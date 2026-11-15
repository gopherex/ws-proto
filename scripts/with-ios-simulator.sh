#!/usr/bin/env bash
# Execute a command with a known, booted iOS simulator, deleting only our device.
set -euo pipefail
if (( $# == 0 )) || [[ $(uname -s) != Darwin ]]; then
  echo "usage (macOS): $0 command [arguments...]" >&2
  exit 2
fi
if [[ -n ${WS_PROTO_IOS_DEVICE:-} ]]; then
  exec "$@"
fi
case $(uname -m) in
  arm64) export WS_PROTO_IOS_TARGET=iosSimulatorArm64 ;;
  x86_64) export WS_PROTO_IOS_TARGET=iosX64 ;;
  *) echo "Unsupported simulator host architecture: $(uname -m)" >&2; exit 2 ;;
esac
simulator=
cleanup() {
  if [[ -n "$simulator" ]]; then
    xcrun simctl shutdown "$simulator" >/dev/null 2>&1 || true
    xcrun simctl delete "$simulator" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

selection=$(xcrun simctl list -j | python3 -c '
import json, sys
state = json.load(sys.stdin)
runtimes = sorted((r for r in state["runtimes"] if r.get("isAvailable") and ".iOS-" in r["identifier"]), key=lambda r: tuple(int(v) for v in r["version"].split(".")), reverse=True)
for runtime in runtimes:
    devices = [d for d in state["devices"].get(runtime["identifier"], []) if d.get("isAvailable") and d["name"].startswith("iPhone") and d.get("deviceTypeIdentifier")]
    if devices:
        print(runtime["identifier"], devices[0]["deviceTypeIdentifier"])
        break
else:
    raise SystemExit("No available iPhone simulator/runtime pair")
')
read -r runtime device_type <<< "$selection"
simulator=$(xcrun simctl create "ws-proto-tests-$RANDOM" "$device_type" "$runtime")
export WS_PROTO_IOS_DEVICE="$simulator"
xcrun simctl boot "$simulator"
xcrun simctl bootstatus "$simulator" -b
"$@"
