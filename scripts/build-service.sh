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
# KAFKA_PROVIDER picks the Kafka client a service with Kafka links (tasks,
# consumer — the ones with internal/kafkaprovider; libs/kafka/README.md):
#
#   rdkafka (default)  librdkafka via confluent-kafka-go: CGO_ENABLED=1, the
#                      bundled static librdkafka (the host's libc stays dynamic;
#                      the image links a fully static musl binary)
#   franz              franz-go: CGO_ENABLED=0 -tags kafka_franz
#
# FIPS=1 builds against Go's frozen FIPS 140-3 module (GOFIPS140, no cgo, no
# BoringCrypto): the binary runs with GODEBUG fips140=on by default and says so
# on /version ("fips140": true) and in its start-up line. docs/RUNTIME.md
# "FIPS 140-3" has what that does and does not guarantee.
#
#   GOFIPS140_VERSION  the module snapshot, default v1.0.0 — must exist under
#                      $(go env GOROOT)/lib/fips140
#
# Only Go's own crypto is covered: librdkafka brings OpenSSL through cgo, so a
# FIPS build of a service with Kafka needs KAFKA_PROVIDER=franz and is refused
# otherwise.
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

provider="${KAFKA_PROVIDER:-rdkafka}"
cgo=0 tags=""
if [ -d "$root/services/$svc/internal/kafkaprovider" ]; then
  case "$provider" in
    rdkafka) cgo=1 ;;
    franz) tags=kafka_franz ;;
    *) echo "build-service.sh: KAFKA_PROVIDER is rdkafka or franz, not '$provider'" >&2; exit 2 ;;
  esac
fi

# GOFIPS140 is always set from FIPS, so a stray GOFIPS140 in the environment
# cannot make a build FIPS (or not) behind the knob's back.
fips=off
case "${FIPS:-0}" in
  0) ;;
  1)
    if [ "$cgo" = 1 ]; then
      echo "build-service.sh: FIPS=1 needs KAFKA_PROVIDER=franz for $svc: the rdkafka provider links librdkafka and OpenSSL through cgo, which Go's FIPS module does not cover" >&2
      exit 2
    fi
    fips="${GOFIPS140_VERSION:-v1.0.0}"
    ;;
  *) echo "build-service.sh: FIPS is 1 or 0, not '$FIPS'" >&2; exit 2 ;;
esac

cd "$root/services/$svc"
GOFIPS140=$fips CGO_ENABLED=$cgo go build -trimpath -tags "$tags" ${cover[@]+"${cover[@]}"} -ldflags="$ldflags" -o "$out" .
