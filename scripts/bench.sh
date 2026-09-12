#!/usr/bin/env bash
# bench.sh — run every Benchmark* function in the workspace, benchstat-ready.
#
#   scripts/bench.sh out.txt
#
# -count=6 gives benchstat enough samples for a confidence interval; -run='^$'
# skips the tests; -benchmem adds allocations. Modules without benchmarks
# contribute nothing. Compare two runs with `benchstat base.txt head.txt`.
set -euo pipefail

out="${1:?usage: bench.sh <out-file>}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: > "$out"
# shellcheck disable=SC2013 # module dirs are single words

for m in $(awk '/^use \(/{f=1;next} /^\)/{f=0} f{sub(/^[ \t]*\.\//,""); print}' "$root/go.work"); do
  if (cd "$root/$m" && grep -rqE '^func Benchmark' --include='*_test.go' .); then
    echo "── bench $m"
    (cd "$root/$m" && go test -run='^$' -bench=. -benchmem -count="${BENCH_COUNT:-6}" ./... | tee -a "$out")
  fi
done
