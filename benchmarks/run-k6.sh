#!/usr/bin/env bash
# run-k6.sh — orchestrate a k6 load test against a freshly-built ping service.
#
# Builds the ping release binary, starts it, waits for health, runs the chosen
# k6 profile, then tears the service down. Results (summary.json + stdout) land
# in benchmarks/results/. The Go analogue of the Rust sibling repo's
# run-k6-e2e.ts orchestrator — minus the Kafka/DB infra these services don't have.
#
# Usage: benchmarks/run-k6.sh <smoke|load|stress|soak|peak>
set -euo pipefail

SCENARIO="${1:-smoke}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="$ROOT/benchmarks/results"
PORT="${PING_PORT:-18080}"
BASE_URL="http://localhost:$PORT"
mkdir -p "$RESULTS"

log() { printf '\033[36m▸ %s\033[0m\n' "$*"; }

command -v k6 >/dev/null || { echo "k6 not found — install via mise (pinned in mise.toml)" >&2; exit 1; }

log "build ping (release)"
( cd "$ROOT/services/ping" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$RESULTS/ping" . )

log "start ping on :$PORT"
PING_HTTP_ADDR=":$PORT" PING_LOG_LEVEL=warn "$RESULTS/ping" > "$RESULTS/ping.log" 2>&1 &
PING_PID=$!
trap 'kill "$PING_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  curl -fsS -o /dev/null "$BASE_URL/healthz" 2>/dev/null && break
  sleep 0.1
done

log "k6 run — scenario=$SCENARIO target=$BASE_URL"
k6 run \
  -e BASE_URL="$BASE_URL" \
  -e SCENARIO="$SCENARIO" \
  --summary-export "$RESULTS/summary-$SCENARIO.json" \
  "$ROOT/benchmarks/k6-ping.js" | tee "$RESULTS/stdout-$SCENARIO.log"

log "done — results in benchmarks/results/ (summary-$SCENARIO.json)"
