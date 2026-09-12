#!/bin/sh
# Merges per-module Go coverage profiles into one file.
#
#   scripts/merge-coverage.sh <out> <module-dir>...
#
# Each module writes coverage.out from `go test -coverprofile`; a Go profile is
# a `mode:` header plus one line per block, so concatenating the blocks under a
# single header is a valid merged profile (the modules share no packages).
# The merged file is what go-test-coverage gates on and what CI publishes.
set -eu

out="${1:?usage: merge-coverage.sh <out> <module-dir>...}"
shift

echo "mode: atomic" > "$out"
for m in "$@"; do
  [ -f "$m/coverage.out" ] || { echo "merge-coverage: $m/coverage.out missing" >&2; exit 1; }
  grep -v '^mode:' "$m/coverage.out" >> "$out"
done
