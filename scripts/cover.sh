#!/usr/bin/env bash
# cover.sh — coverage per test layer, merged across layers.
#
#   scripts/cover.sh unit          # go test -short   → .cover/unit
#   scripts/cover.sh integration   # go test (Docker) → .cover/integration
#   scripts/cover.sh e2e           # cover-built binaries + Playwright → .cover/e2e
#   scripts/cover.sh merge         # .cover/* → .cover/merged, coverage-merged.out, per-layer %
#
# Every layer writes Go's binary coverage format (GOCOVERDIR / -test.gocoverdir,
# Go ≥ 1.20), which `go tool covdata` merges exactly — counters summed per
# block across unit tests, container-backed tests and the real binaries the
# e2e harness spawns. The merged text profile is what .testcoverage.yml gates
# and what CI publishes; the per-layer numbers show what each layer adds.
#
# Every layer uses -covermode=atomic. covdata refuses to merge mixed modes
# ("counter mode clash"), and `-race` silently switches a run to atomic while
# a plain run defaults to set — so the mode is pinned rather than inferred.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cover="${COVER_DIR:-$root/.cover}"
# Instrument every workspace package, not just the module under test, so a
# service's tests also count the lib code they exercise.
pkgs="github.com/tracehubmmp/golang-basics/..."
modules="$(awk '/^use \(/{f=1;next} /^\)/{f=0} f{sub(/^[ \t]*\.\//,""); print}' "$root/go.work")"

layer_dir() { mkdir -p "$cover/$1"; echo "$cover/$1"; }
# go-test-coverage from PATH (CI installs it into bin/) or through mise (local).
gotestcov() { if command -v go-test-coverage >/dev/null; then go-test-coverage "$@"; else mise exec -- go-test-coverage "$@"; fi; }

case "${1:?usage: cover.sh unit|integration|e2e|merge}" in
  unit)
    dir="$(layer_dir unit)"
    for m in $modules; do
      (cd "$root/$m" && go test -short -cover -covermode=atomic -coverpkg="$pkgs" ./... -args -test.gocoverdir="$dir" >/dev/null)
    done
    ;;
  integration)
    dir="$(layer_dir integration)"
    for m in $modules; do
      (cd "$root/$m" && go test -cover -covermode=atomic -coverpkg="$pkgs" ./... -args -test.gocoverdir="$dir" >/dev/null)
    done
    ;;
  e2e)
    dir="$(layer_dir e2e)"
    # Cover-instrumented binaries write their counters to GOCOVERDIR when they
    # exit normally — the harness stops them with SIGTERM, which the services
    # turn into a graceful shutdown, so the data is flushed.
    for s in ping heartbeat tasks consumer; do COVER=1 "$root/scripts/build-service.sh" "$s"; done
    (cd "$root" && GOCOVERDIR="$dir" pnpm --filter @golang-basics/e2e test)
    ;;
  merge)
    inputs=""
    for l in unit integration e2e; do
      if [ -d "$cover/$l" ] && [ -n "$(ls -A "$cover/$l")" ]; then
        # Per-layer total, computed the same way as the gate (statements).
        go tool covdata textfmt -i="$cover/$l" -o "$cover/$l.out"
        printf '%-12s %s\n' "$l" "$(gotestcov --profile "$cover/$l.out" --threshold-total 0 2>/dev/null | sed -n 's/^Total test coverage: //p')"
        inputs="${inputs:+$inputs,}$cover/$l"
      fi
    done
    [ -n "$inputs" ] || { echo "cover.sh merge: no layer data under $cover" >&2; exit 1; }
    rm -rf "$cover/merged" "$root/coverage-merged.out" && mkdir -p "$cover/merged"
    # covdata reports some failures (e.g. a counter-mode clash) as "error:" on
    # stderr without a non-zero exit — check the output, never trust the gate
    # on a partial merge.
    go tool covdata merge -i="$inputs" -o "$cover/merged" 2>&1 | tee "$cover/merge.log"
    go tool covdata textfmt -i="$cover/merged" -o "$root/coverage-merged.out" 2>&1 | tee -a "$cover/merge.log"
    if grep -qi 'error' "$cover/merge.log" || [ ! -s "$root/coverage-merged.out" ]; then
      echo "cover.sh merge: covdata reported errors; refusing to gate on a partial profile" >&2
      exit 1
    fi
    echo "merged → coverage-merged.out ($(go tool covdata percent -i="$cover/merged" | wc -l | tr -d ' ') packages)"
    ;;
  *) echo "cover.sh: unknown layer '$1'" >&2; exit 2 ;;
esac
