#!/bin/sh
# Module discovery — the one place module lists come from. go.work is the
# source of truth; nothing else in the repo (justfile, CI, hooks) spells out
# a module list.
#
#   scripts/touched-modules.sh --all           every go.work module, in go.work order
#   scripts/touched-modules.sh --integration   modules with Docker-backed suites:
#                                              exactly those whose go.mod requires
#                                              (or is) libs/testx/containers
#   scripts/touched-modules.sh --services      service names (services/<name>)
#   scripts/touched-modules.sh --mutation      modules with a .gremlins.yaml
#   scripts/touched-modules.sh FILE…           the modules containing FILE…
#
# The FILE form maps paths to the innermost module that contains them, one
# module directory per line, deduplicated — used by the lefthook hooks so a
# commit touching only services/ping lints and tests only services/ping, and a
# change under libs/testx/containers maps to that module, not libs/testx. A
# path outside any module (README, justfile, …) prints nothing.
#
# POSIX sh (the GitLab jobs run it in alpine / golang images); works from any
# working directory.
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"

all() {
  # Both `use ./x` and the `use ( … )` block form.
  awk '
    /^use[ \t]*\(/ { f = 1; next }
    f && /^\)/     { f = 0; next }
    /^use[ \t]+[^( \t]/ { sub(/^use[ \t]+/, ""); print_mod($0); next }
    f              { print_mod($0) }
    function print_mod(s) { sub(/\/\/.*/, "", s); gsub(/[ \t]/, "", s); sub(/^\.\//, "", s); if (s != "" && s != ".") print s }
  ' "$root/go.work"
}

case "${1:-}" in
  --all)
    all
    ;;
  --integration)
    all | while IFS= read -r m; do
      if grep -qE '(^|[[:space:]])github\.com/tracehubmmp/golang-basics/libs/testx/containers([[:space:]]|$)' "$root/$m/go.mod"; then
        printf '%s\n' "$m"
      fi
    done
    ;;
  --services)
    all | sed -n 's|^services/||p'
    ;;
  --mutation)
    all | while IFS= read -r m; do
      [ -f "$root/$m/.gremlins.yaml" ] && printf '%s\n' "$m"
    done
    ;;
  --*)
    echo "touched-modules.sh: unknown flag $1" >&2
    exit 2
    ;;
  *)
    mods="$(all | awk '{ print length($0) "\t" $0 }' | sort -rn | cut -f2)"
    for f in "$@"; do
      f="${f#./}"
      for m in $mods; do
        case "$f" in "$m"/*) printf '%s\n' "$m"; break ;; esac
      done
    done | sort -u
    ;;
esac
