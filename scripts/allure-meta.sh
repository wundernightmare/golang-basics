#!/usr/bin/env bash
# allure-meta.sh — the report metadata Allure reads from the results directory:
#
#   scripts/allure-meta.sh <results-dir>
#
#   categories.json  copied from the tracked allure/categories.json: how a
#                    failure is bucketed on the Categories tab — a ticketed
#                    flake, an infrastructure failure (docker / testcontainers /
#                    connection refused / timeouts), a product defect (an
#                    assertion), or skipped. First match wins, in file order.
#   executor.json    who ran the suite and where (the Executors widget; TestOps
#                    keys a launch on it): GitHub Actions or GitLab CI from the
#                    runner's default variables, otherwise "local" with the
#                    build named by `git describe`.
#
# `just allure-report` and both pipelines' allure-report jobs call it after
# merging every layer's results into one directory, right before
# `allure generate`. GITHUB_* / CI_* are the runners' default environment —
# no `github.*` context is interpolated into a run: script for this.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dir="${1:?usage: allure-meta.sh <results-dir>}"
mkdir -p "$dir"

cp "$root/allure/categories.json" "$dir/categories.json"

# Minimal JSON string escaping: a branch name or a workflow name is free text.
json() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr -d '\n\r\t'; }

if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
  name="GitHub Actions"; type="github"
  url="${GITHUB_SERVER_URL:-https://github.com}"
  build_url="$url/${GITHUB_REPOSITORY:-}/actions/runs/${GITHUB_RUN_ID:-}"
  build_name="${GITHUB_WORKFLOW:-ci} #${GITHUB_RUN_NUMBER:-0} (${GITHUB_REF_NAME:-})"
  build_order="${GITHUB_RUN_NUMBER:-}"
elif [ "${GITLAB_CI:-}" = "true" ]; then
  name="GitLab CI"; type="gitlab"
  url="${CI_SERVER_URL:-}"
  build_url="${CI_PIPELINE_URL:-${CI_JOB_URL:-}}"
  build_name="${CI_PROJECT_PATH:-} #${CI_PIPELINE_IID:-0} (${CI_COMMIT_REF_NAME:-})"
  build_order="${CI_PIPELINE_IID:-}"
else
  name="local"; type="local"
  url=""; build_url=""
  build_name="$(git -C "$root" describe --tags --always --dirty 2>/dev/null || echo dev)"
  build_order=""
fi
# buildOrder is a number in Allure's schema; drop it rather than emit a string.
case "$build_order" in *[!0-9]*|"") build_order="" ;; esac

{
  printf '{\n  "name": "%s",\n  "type": "%s",\n  "buildName": "%s"' \
    "$(json "$name")" "$(json "$type")" "$(json "$build_name")"
  if [ -n "$url" ]; then printf ',\n  "url": "%s"' "$(json "$url")"; fi
  if [ -n "$build_url" ]; then printf ',\n  "buildUrl": "%s"' "$(json "$build_url")"; fi
  if [ -n "$build_order" ]; then printf ',\n  "buildOrder": %s' "$build_order"; fi
  printf '\n}\n'
} > "$dir/executor.json"

echo "allure-meta: categories.json + executor.json ($name: $build_name) → $dir"
