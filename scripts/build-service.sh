#!/usr/bin/env bash
# build-service.sh — the one place the release build line lives.
#
#   scripts/build-service.sh <service> [output-path]
#
# Static, trimmed, stripped binary with the version stamped into
# libs/httpx.Version (surfaces in build_info, /version, the OTel resource and
# the startup log line). VERSION defaults to `git describe` when built from a
# checkout, "dev" otherwise; CI passes the tag / commit sha. Used by the
# justfile, both CI pipelines, the e2e harness and the k6 scripts — the
# Dockerfiles repeat the same flags because they build inside the image.
set -euo pipefail

svc="${1:?usage: build-service.sh <service> [output]}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${2:-$root/services/$svc/bin/$svc}"

version="${VERSION:-}"
if [ -z "$version" ]; then
  version="$(git -C "$root" describe --tags --always --dirty 2>/dev/null || echo dev)"
fi

# COVER=1 builds a coverage-instrumented binary (every workspace package) that
# writes its counters to GOCOVERDIR on exit — how the e2e layer contributes to
# the merged coverage (scripts/cover.sh e2e). Never for a release artefact.
cover_flags=""
if [ -n "${COVER:-}" ]; then
  cover_flags="-cover -coverpkg=github.com/tracehubmmp/golang-basics/..."
fi

cd "$root/services/$svc"
# shellcheck disable=SC2086 # cover_flags is a flag list
CGO_ENABLED=0 go build -trimpath $cover_flags \
  -ldflags="-s -w -X github.com/tracehubmmp/golang-basics/libs/httpx.Version=${version}" \
  -o "$out" .
