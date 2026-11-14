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
# after another). Each module's output is printed whole when it finishes, a
# failed one in full, and the script exits 1 naming every module that failed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
jobs="${JOBS:-4}"
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

run_module() {
  local module="$1" log
  shift
  log="$out/$(echo "$module" | tr / -).log"

  local started=$SECONDS status=ok
  (cd "$root/$module" && go test -race -shuffle=on ./... "$@") >"$log" 2>&1 || status=FAILED

  {
    echo "── integration $module — $status in $((SECONDS - started))s"
    # A green module is its package lines; a red one is everything it said.
    if [ "$status" = ok ]; then grep -E '^(ok|\?|---)' "$log" || true; else cat "$log"; fi
  } | flock "$out/.print" cat

  [ "$status" = ok ] || echo "$module" >>"$out/.failed"
}
export -f run_module
export root out

started=$SECONDS
"$root/scripts/touched-modules.sh" --integration |
  xargs -P "$jobs" -I{} bash -c 'run_module "$@"' _ {} "$@"

echo "── integration: $((SECONDS - started))s, $jobs modules at a time"

if [ -s "$out/.failed" ]; then
  echo "── FAILED:" >&2
  sort "$out/.failed" | sed 's/^/   /' >&2
  exit 1
fi
