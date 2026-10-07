#!/usr/bin/env bash
# build-service.sh — the one place the host release build line lives.
#
#   scripts/build-service.sh <service> [output-path]
#
# Static, trimmed, stripped binary with the build stamped into libs/httpx
# (surfaces in build_info, /version, the OTel resource and the startup log):
#
#   VERSION     httpx.Version    default: `git describe --tags --always --dirty`, else "dev"
#   GIT_SHA     httpx.Revision   default: `git rev-parse HEAD`, else empty
#   BUILD_TIME  httpx.BuildTime  default: the HEAD commit time (RFC 3339, UTC) —
#                                reproducible: the same commit stamps the same time
#
# COVER=1 builds a coverage-instrumented binary (every workspace package,
# -covermode=atomic) that writes its counters to GOCOVERDIR on a clean exit —
# how the e2e layer contributes to the merged coverage. Never for a release.
#
# Used by the justfile (release, e2e-build), both CI pipelines, the host
# spawner, schemathesis and the k6 scripts. docker/service.Dockerfile repeats
# the same ldflags because it builds inside the image, from the same build
# arguments (VERSION / GIT_SHA / BUILD_TIME).
set -euo pipefail

svc="${1:?usage: build-service.sh <service> [output]}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${2:-$root/services/$svc/bin/$svc}"
# The build runs from the service dir: anchor a relative output path to the
# caller's working directory first.
case "$out" in /*) ;; *) out="$PWD/$out" ;; esac
[ -d "$root/services/$svc" ] || { echo "build-service.sh: no services/$svc" >&2; exit 2; }

version="${VERSION:-$(git -C "$root" describe --tags --always --dirty 2>/dev/null || echo dev)}"
revision="${GIT_SHA:-$(git -C "$root" rev-parse HEAD 2>/dev/null || true)}"
build_time="${BUILD_TIME:-$(TZ=UTC git -C "$root" log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ 2>/dev/null || true)}"

pkg="github.com/tracehubmmp/golang-basics/libs/httpx"
ldflags="-s -w -X $pkg.Version=$version -X $pkg.Revision=$revision -X $pkg.BuildTime=$build_time"

cover=()
if [ -n "${COVER:-}" ] && [ "${COVER}" != 0 ]; then
  cover=(-cover -covermode=atomic "-coverpkg=github.com/tracehubmmp/golang-basics/...")
fi

cd "$root/services/$svc"
CGO_ENABLED=0 go build -trimpath ${cover[@]+"${cover[@]}"} -ldflags="$ldflags" -o "$out" .
