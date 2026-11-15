#!/usr/bin/env bash
# Run a command against the Go interoperability fixture, then stop the fixture.
set -euo pipefail
if (( $# == 0 )); then
  echo "usage: $0 command [arguments...]" >&2
  exit 2
fi
root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d)
server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$scratch"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
go -C "$root" build -o "$scratch/server" ./tests/mobile-server
# Let the OS select a free port; the server logs the bound address.
WS_PROTO_TEST_ADDR=127.0.0.1:0 "$scratch/server" >"$scratch/server.log" 2>&1 &
server_pid=$!
for (( attempt=0; attempt<100; attempt++ )); do
  address=$(sed -n 's/.*LISTENING \(127\.0\.0\.1:[0-9]*\).*/\1/p' "$scratch/server.log")
  if [[ -n "$address" ]] && curl --noproxy '*' -fsS "http://$address/health" >/dev/null; then
    export WS_PROTO_TEST_URL="ws://$address/"
    "$@"
    exit 0
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$scratch/server.log" >&2
    exit 1
  fi
  sleep 0.1
done
cat "$scratch/server.log" >&2
echo "mobile fixture did not become ready" >&2
exit 1
