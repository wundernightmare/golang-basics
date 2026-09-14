#!/bin/sh
# Fails when `go mod tidy` or `go work sync` would change anything, without
# touching the tree. The single source for `just tidy-check`, the lint-test /
# vet matrix jobs (one module each), the fmt jobs (workspace) and the pre-push
# hook, so the four cannot drift apart.
#
#   scripts/tidy-check.sh                 # every module + the workspace
#   scripts/tidy-check.sh libs/httpx …    # only these modules
#   scripts/tidy-check.sh --workspace     # only `go work sync`
#
# Why: `go work sync` and `go mod tidy` disagree silently when a bump adds an
# `// indirect` require to a module that does not import the package, and a
# module's go.sum can miss the entries the workspace-resolved versions need.
# Neither breaks the build, so only a diff check catches it.
set -eu
cd "$(dirname "$0")/.."

modules=""
workspace=0
if [ "$#" -eq 0 ]; then
  modules="$(scripts/touched-modules.sh --all)"
  workspace=1
elif [ "$1" = "--workspace" ]; then
  workspace=1
else
  modules="$*"
fi

rc=0
for m in $modules; do
  echo "── tidy-check $m"
  # -diff prints the change and exits 1 instead of rewriting go.mod / go.sum.
  if ! (cd "$m" && go mod tidy -diff); then
    echo "tidy-check: $m/go.mod or go.sum is not tidy — run 'just tidy' and commit" >&2
    rc=1
  fi
done

if [ "$workspace" -eq 1 ]; then
  echo "── tidy-check go.work"
  # `go work sync` has no -diff: run it against copies and restore them.
  all="$(scripts/touched-modules.sh --all)"
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  for m in $all; do
    mkdir -p "$tmp/$m"
    cp "$m/go.mod" "$tmp/$m/go.mod"
    [ -f "$m/go.sum" ] && cp "$m/go.sum" "$tmp/$m/go.sum"
  done
  [ -f go.work.sum ] && cp go.work.sum "$tmp/go.work.sum"
  go work sync
  for f in go.work.sum $(for m in $all; do echo "$m/go.mod"; [ -f "$m/go.sum" ] && echo "$m/go.sum"; done); do
    [ -f "$tmp/$f" ] || continue
    if ! cmp -s "$f" "$tmp/$f"; then
      echo "tidy-check: go work sync changes $f — run 'just tidy' and commit" >&2
      diff -u "$tmp/$f" "$f" || :
      rc=1
    fi
    cp "$tmp/$f" "$f"
  done
fi

[ "$rc" -eq 0 ] && echo "tidy-check OK"
exit "$rc"
