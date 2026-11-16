#!/bin/sh
# leak-check.sh — fail when a test package has no goroutine-leak check.
#
#   scripts/leak-check.sh
#
# A directory with _test.go files is one test binary, and a binary checks for
# leaked goroutines only if its TestMain says so: testx.Main(m),
# containers.Main(m) (the same check, after the containers are gone), or
# goleak.VerifyTestMain(m) in a module that does not depend on libs/testx —
# or a bare Main(m), which is how the internal tests of libs/testx/containers
# call their own package's containers.Main. A
# new package without one passes with goroutines left running, and nothing
# else would notice: the gate is the file list, not a reviewer's memory.
#
# POSIX sh (the GitLab job runs it in the golang image).
set -eu

cd "$(dirname "$0")/.."

if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1; then
  files="$(git ls-files --cached --others --exclude-standard -- '*_test.go')"
else
  files="$(find . -name '*_test.go' -not -path '*/.cache/*' -not -path '*/node_modules/*' | sed 's|^\./||')"
fi

missing=""
for dir in $(printf '%s\n' "$files" | grep -v '/testdata/' | sed 's|/[^/]*$||' | sort -u); do
  # A deleted file is still in the index until the commit.
  [ -d "$dir" ] || continue

  if ! grep -qE 'testx\.Main\(|containers\.Main\(|goleak\.VerifyTestMain\(|os\.Exit\(Main\(m' "$dir"/*_test.go; then
    missing="$missing  $dir
"
  fi
done

if [ -n "$missing" ]; then
  echo "test packages without a goroutine-leak check (TestMain → testx.Main, containers.Main or goleak.VerifyTestMain):"
  printf '%s' "$missing"
  exit 1
fi

echo "leak-check OK"
