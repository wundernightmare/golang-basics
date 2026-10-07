#!/bin/sh
# check-waivers.sh — fail on any expired, time-boxed waiver.
#
#   scripts/check-waivers.sh            # today (UTC)
#   TODAY=2026-10-01 scripts/check-waivers.sh
#
# Every waiver in this repo (CVE ignores in .grype.yaml / osv-scanner.toml,
# oasdiff exceptions in api/oasdiff-breaking.ignore, pnpm
# minimumReleaseAgeExclude, lint exclusions, …) carries a removal trigger. When
# that trigger is a date, write it as `Remove after YYYY-MM-DD` and this script
# turns it into a gate: once the date has passed, `just sec` and the CI `sast`
# job fail until the waiver is removed or consciously re-dated.
#
# Scans every tracked and untracked-but-not-ignored text file (git grep), so a
# new waiver file needs no registration here. Markdown is skipped: docs talk
# about the convention, they do not carry waivers.
#
# POSIX sh + awk (the CI job runs it in the semgrep image).
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
today="${TODAY:-$(date -u +%Y-%m-%d)}"
pattern='[Rr]emove after [0-9]{4}-[0-9]{2}-[0-9]{2}'

if command -v git >/dev/null 2>&1 && git -C "$root" rev-parse --git-dir >/dev/null 2>&1; then
  hits="$(git -C "$root" grep --untracked -nIE "$pattern" -- . ':!*.md' ':!scripts/check-waivers.sh' || true)"
else
  hits="$(cd "$root" && grep -rnIE "$pattern" --exclude='*.md' --exclude=check-waivers.sh \
    --exclude-dir=.git --exclude-dir=node_modules --exclude-dir=.cache . || true)"
fi

printf '%s\n' "$hits" | awk -v today="$today" '
  NF == 0 { next }
  {
    line = $0
    while (match(line, /[Rr]emove after [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/)) {
      d = substr(line, RSTART + RLENGTH - 10, 10)
      if (d < today) { print "expired waiver (" d " < " today "): " $0; bad++ }
      else ok++
      line = substr(line, RSTART + RLENGTH)
    }
  }
  END {
    if (bad) { printf "check-waivers: %d expired waiver(s) — remove them or re-date with a reason\n", bad > "/dev/stderr"; exit 1 }
    printf "check-waivers: %d dated waiver(s), none expired (today %s)\n", ok + 0, today
  }'
