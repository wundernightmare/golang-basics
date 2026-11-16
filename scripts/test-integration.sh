#!/usr/bin/env bash
# test-integration.sh — the Docker-backed suites, several modules at a time.
#
#   scripts/test-integration.sh [go test flags…]
#   JOBS=8 scripts/test-integration.sh -run TestChaos
#
# Every module that requires libs/testx/containers (scripts/touched-modules.sh
# --integration) runs `go test -race -shuffle=on ./...` with the flags given.
# A module starts its own containers on random ports and isolates by name, so
# modules do not meet: JOBS of them run at once (default 4 — on a 32-core
# machine the whole layer took 76s at 4 and 52s at 8, against 7 minutes one
# after another). Each module's output is printed whole when it finishes, and
# the script exits 1 naming every module that failed.
#
# The tests run under gotestsum (mise.toml) for its summary: `go test` without
# -v says `ok` for a package whose every test skipped, and a fixture that
# skips — no container runtime, -short — would read as a green run. Each
# module lists its skipped tests with the reason, and the last lines count
# them per module.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
jobs="${JOBS:-4}"
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

command -v gotestsum >/dev/null || { echo "test-integration.sh: gotestsum not found (just setup)" >&2; exit 1; }

run_module() {
  local module="$1" log
  shift
  log="$out/$(echo "$module" | tr / -).log"

  local started=$SECONDS status=ok
  (cd "$root/$module" && gotestsum --format pkgname --no-color -- -race -shuffle=on ./... "$@") >"$log" 2>&1 || status=FAILED

  {
    echo "── integration $module — $status in $((SECONDS - started))s"
    cat "$log"
  } | flock "$out/.print" cat

  [ "$status" = ok ] || echo "$module" >>"$out/.failed"

  # gotestsum's last line: "DONE 41 tests, 3 skipped in 12.3s".
  local skipped
  skipped="$(sed -n 's/^DONE .* \([0-9][0-9]*\) skipped.*/\1/p' "$log" | tail -1)"
  [ -z "$skipped" ] || echo "$module $skipped" >>"$out/.skipped"
}
export -f run_module
export root out

started=$SECONDS
"$root/scripts/touched-modules.sh" --integration |
  xargs -P "$jobs" -I{} bash -c 'run_module "$@"' _ {} "$@"

echo "── integration: $((SECONDS - started))s, $jobs modules at a time"

if [ -s "$out/.skipped" ]; then
  echo "── SKIPPED: $(awk '{ n += $2 } END { print n }' "$out/.skipped") tests did not run (listed per module above):"
  sort "$out/.skipped" | sed 's/^/   /'
fi

if [ -s "$out/.failed" ]; then
  echo "── FAILED:" >&2
  sort "$out/.failed" | sed 's/^/   /' >&2
  exit 1
fi
