#!/usr/bin/env bash
# mutate.sh — mutation testing (gremlins) of one module.
#
#   scripts/mutate.sh [MODULE] [OUT.json] [gremlins unleash flags…]
#                                             # default: libs/resilient-http-client
#
# Scope and thresholds are the module's .gremlins.yaml. This wrapper exists
# because gremlins tests each mutant in a copy of the module directory alone,
# somewhere under $TMPDIR — where neither go.work nor a `replace … => ../x`
# of go.mod resolves. Left alone, the copy does not build, `go test` exits 1,
# and gremlins books that as KILLED: every mutant "dies", efficacy reads 100 %
# and the thresholds can never fail. So:
#
#   1. the run uses a go.mod whose local replaces are absolute (-modfile),
#      with GOWORK=off, so the copy builds wherever it lands;
#   2. the suite must first pass in such a copy — if it does not, the script
#      stops instead of letting gremlins report a meaningless score.
#
# -count=1: the per-mutant timeout is derived from the initial coverage run,
# so a cached run would time every mutant out.
#
# -short: the oracle is the unit layer. A mutant costs one run of the suite,
# so the container-backed tests of a module (libs/tiercache: Valkey,
# Toxiproxy) would turn minutes into hours — and a mutant only they kill is
# logic the unit tests do not pin down, which is the gap this run is for.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module="${1:-libs/resilient-http-client}"
out="${2:-$root/gremlins-$(basename "$module").json}"
dir="$root/$module"

[ -f "$dir/.gremlins.yaml" ] || { echo "mutate: $module has no .gremlins.yaml" >&2; exit 2; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cp "$dir/go.mod" "$dir/go.sum" "$tmp/"
# Directory replaces (no version) → absolute paths. Listed first, on its own:
# a go.mod that is not tidy must stop the run here, not yield no replaces.
replaces="$(cd "$dir" && GOWORK=off go list -m -f '{{if .Replace}}{{if not .Replace.Version}}{{.Path}}={{.Replace.Dir}}{{end}}{{end}}' all)"
while IFS='=' read -r path target; do
  [ -n "$path" ] || continue
  go mod edit -replace="$path=$target" "$tmp/go.mod"
done <<<"$replaces"

export GOWORK=off
export GOFLAGS="-count=1 -short -modfile=$tmp/go.mod"

mkdir "$tmp/probe"
cp -R "$dir/." "$tmp/probe/"
if ! (cd "$tmp/probe" && go test ./... >"$tmp/probe.log" 2>&1); then
  cat "$tmp/probe.log" >&2
  echo "mutate: the suite of $module fails in an out-of-tree copy — gremlins would count every mutant as killed" >&2
  exit 2
fi

cd "$dir" && gremlins unleash --output "$out" "${@:3}"
