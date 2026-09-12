#!/usr/bin/env bash
# schemathesis.sh — property-based API testing against a real binary.
#
#   scripts/schemathesis.sh ping            # dependency-free
#   scripts/schemathesis.sh tasks           # needs the deps up: `just infra-up`
#
# Builds the service, starts it on a scratch port, waits for /readyz on its
# admin port, then runs Schemathesis against the service's OpenAPI document
# (api/openapi3/<svc>.openapi.yaml): generated positive and negative requests
# for every operation, checking that no request yields a 5xx and that every
# response — status, content type, headers, body — matches the contract.
# This is the generative layer of the pyramid: it finds inputs no hand-written
# test thought of. Results go to Allure (native reporter) under
# ALLURE_RESULTS_DIR, next to every other layer.
#
# Schemathesis runs from its pinned image (DOCKER_HUB for a closed network)
# unless a `schemathesis` binary is on PATH (the GitLab job runs inside the
# image itself).
set -euo pipefail

svc="${1:?usage: schemathesis.sh <ping|tasks>}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${DOCKER_HUB:-docker.io}/schemathesis/schemathesis:${SCHEMATHESIS_VERSION:-4.27.0}"
results="${ALLURE_RESULTS_DIR:-$root/services/$svc/allure-results}"
spec="api/openapi3/$svc.openapi.yaml"
max_examples="${SCHEMATHESIS_MAX_EXAMPLES:-100}"
mkdir -p "$results" "$root/.run"

case "$svc" in
  ping)  port=18280; admin=19280; upper=PING ;;
  tasks) port=18282; admin=19282; upper=TASKS ;;
  *) echo "schemathesis.sh: unknown service '$svc'" >&2; exit 2 ;;
esac

log() { printf '\033[36m▸ %s\033[0m\n' "$*"; }

# SERVICE_BIN: a pre-built binary (CI hands over build-services' artefact and
# has no Go toolchain in the Schemathesis image); otherwise build it here.
bin="${SERVICE_BIN:-}"
if [ -z "$bin" ]; then
  log "build $svc"
  bin="$root/.run/schemathesis-$svc"
  "$root/scripts/build-service.sh" "$svc" "$bin"
fi

log "start $svc on :$port (admin :$admin)"
env "${upper}_HTTP_ADDR=:$port" "${upper}_ADMIN_ADDR=:$admin" "${upper}_LOG_LEVEL=warn" \
  "$bin" > "$root/.run/schemathesis-$svc.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
  if command -v curl >/dev/null; then curl -fsS -o /dev/null "http://localhost:$admin/readyz" 2>/dev/null && break
  else wget -q -O /dev/null "http://localhost:$admin/readyz" 2>/dev/null && break; fi
  sleep 0.2
done

log "schemathesis run — $spec against :$port ($max_examples examples/operation)"
if command -v schemathesis >/dev/null; then
  schemathesis run "$root/$spec" --url "http://localhost:$port" \
    --checks all --max-examples "$max_examples" --report allure --report-allure-path "$results"
else
  # host.docker.internal: Docker Desktop / OrbStack resolve it; Linux needs the
  # host-gateway alias.
  # --user: the results directory belongs to the invoking user; on Linux the
  # image's default user could not write into it (found on the GitHub runner).
  docker run --rm --add-host=host.docker.internal:host-gateway --user "$(id -u):$(id -g)" \
    -v "$root/api:/api:ro" -v "$results:/results" "$image" \
    run "/$spec" --url "http://host.docker.internal:$port" \
    --checks all --max-examples "$max_examples" --report allure --report-allure-path /results
fi
