#!/usr/bin/env bash
# fuzz.sh — run every Fuzz* target in the workspace for FUZZTIME each.
#
# `go test -fuzz` accepts exactly one target per invocation, so this walks the
# modules, finds the targets and runs them one by one. A crash writes its input
# to <pkg>/testdata/fuzz/<Target>/ — commit it: it becomes a regression case
# that runs as a normal unit test from then on.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fuzztime="${FUZZTIME:-20s}"
status=0

# shellcheck disable=SC2013 # module dirs and target names are single words
for m in $(awk '/^use \(/{f=1;next} /^\)/{f=0} f{sub(/^[ \t]*\.\//,""); print}' "$root/go.work"); do
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    pkg="$(dirname "$file")"
    # shellcheck disable=SC2013
    for target in $(grep -oE '^func (Fuzz[A-Za-z0-9_]+)' "$root/$m/$file" | awk '{print $2}'); do
      echo "── fuzz $m/$pkg $target ($fuzztime)"
      if ! (cd "$root/$m" && go test -run='^$' -fuzz="^${target}\$" -fuzztime="$fuzztime" "./$pkg" 2>&1 | tail -2); then
        status=1
      fi
    done
  done < <(cd "$root/$m" && grep -rlE '^func Fuzz' --include='*_test.go' . 2>/dev/null || true)
done
exit $status
