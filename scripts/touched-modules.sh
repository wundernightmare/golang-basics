#!/bin/sh
# Maps a list of file paths to the workspace modules that contain them, one
# module directory per line, deduplicated. Used by the lefthook hooks so a
# commit touching only services/ping lints and tests only services/ping.
#
#   scripts/touched-modules.sh libs/httpx/health.go services/ping/main.go
#   → libs/httpx
#     services/ping
#
# A path outside any module (README, justfile, .golangci.yml, …) prints
# nothing. Pass --all to print every module from go.work instead.
set -eu

if [ "${1:-}" = "--all" ]; then
  awk '/^use \(/{f=1;next} /^\)/{f=0} f{sub(/^[ \t]*\.\//,""); print}' go.work
  exit 0
fi

for f in "$@"; do
  case "$f" in
    libs/*/*|services/*/*) printf '%s\n' "$f" | cut -d/ -f1-2 ;;
  esac
done | sort -u | while IFS= read -r m; do
  [ -f "$m/go.mod" ] && printf '%s\n' "$m"
done
