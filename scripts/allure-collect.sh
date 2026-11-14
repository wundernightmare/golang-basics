#!/usr/bin/env bash
# allure-collect.sh — one Allure results directory from the two kinds of
# results the test layers produce:
#
#   scripts/allure-collect.sh <results-dir> [junit-dir] [native-dir]
#
#   junit-dir   (default .reports/junit) JUnit XML, one file per layer × module:
#               gotestsum for the unit and integration layers, schemathesis.
#               Allure imports it as is — a name, a status, the output.
#   native-dir  (default .reports/allure-native) Allure's own result files,
#               one subdirectory per run of a suite that reports natively: the
#               e2e suite through testo (e2e/report_test.go) — steps, timings,
#               each service's output as an attachment.
#
# A run that reported natively must not be imported from JUnit as well: the
# report would count every test twice. A native subdirectory <name> that
# holds results therefore replaces <junit-dir>/<name>.xml (e2e, e2e-franz,
# e2e-rdkafka — the names the jobs give both). When it holds none — the suite
# did not build, or died before its first test — the JUnit file is the only
# record of that and stays in.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results="${1:?usage: allure-collect.sh <results-dir> [junit-dir] [native-dir]}"
junit="${2:-$root/.reports/junit}"
native="${3:-$root/.reports/allure-native}"

rm -rf "$results"
mkdir -p "$results"

replaced=" "
if [ -d "$native" ]; then
  for dir in "$native"/*/; do
    [ -d "$dir" ] || continue
    name="$(basename "$dir")"
    if ! compgen -G "$dir*-result.json" >/dev/null; then
      echo "allure-collect: $name: no native results — keeping its JUnit file, if any"
      continue
    fi
    # Result, container and attachment files are named by UUID: runs do not
    # collide. environment.properties is the same few lines from each.
    cp "$dir"* "$results/"
    replaced="$replaced$name "
    echo "allure-collect: $name: $(find "$dir" -name '*-result.json' | wc -l | tr -d ' ') native results"
  done
fi

count=0
if [ -d "$junit" ]; then
  for file in "$junit"/*.xml; do
    [ -f "$file" ] || continue
    name="$(basename "$file" .xml)"
    case "$replaced" in
      *" $name "*) echo "allure-collect: $name.xml: replaced by the native results" ;;
      *) cp "$file" "$results/"; count=$((count + 1)) ;;
    esac
  done
fi
echo "allure-collect: $count JUnit files → $results"

if [ -z "$(ls -A "$results")" ]; then
  echo "allure-collect: no results in $junit or $native — run just test-junit and/or just e2e first" >&2
  exit 1
fi
