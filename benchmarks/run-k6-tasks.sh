#!/usr/bin/env bash
# run-k6-tasks.sh — orchestrate a k6 load test against a freshly-built tasks service.
#
# Unlike run-k6.sh (ping), the tasks service needs its backing dependencies, so
# this script assumes Postgres + Valkey + Kafka are already up:
#
#   just infra-up            # docker compose -f docker/deps.yml up -d
#   benchmarks/run-k6-tasks.sh smoke
#
# It builds the tasks release binary, points it at the host deps (localhost),
# starts it, waits for /readyz, runs the chosen k6 profile, then tears the
# service down. Results land in benchmarks/results/.
#
# Usage: benchmarks/run-k6-tasks.sh <smoke|load|stress|soak>
set -euo pipefail

SCENARIO="${1:-smoke}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="$ROOT/benchmarks/results"
PORT="${TASKS_PORT:-18082}"
BASE_URL="http://localhost:$PORT"
mkdir -p "$RESULTS"

log() { printf '\033[36m▸ %s\033[0m\n' "$*"; }

command -v k6 >/dev/null || { echo "k6 not found — install via mise (pinned in mise.toml)" >&2; exit 1; }

log "build tasks (release)"
( cd "$ROOT/services/tasks" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$RESULTS/tasks" . )

log "start tasks on :$PORT (deps on localhost — run 'just infra-up' first)"
TASKS_HTTP_ADDR=":$PORT" \
  TASKS_DATABASE_URL="${TASKS_DATABASE_URL:-postgres://app:app@localhost:5432/app?sslmode=disable}" \
  TASKS_VALKEY_URL="${TASKS_VALKEY_URL:-valkey://localhost:6379}" \
  TASKS_KAFKA_BROKERS="${TASKS_KAFKA_BROKERS:-localhost:9092}" \
  TASKS_LOG_LEVEL=warn \
  "$RESULTS/tasks" > "$RESULTS/tasks.log" 2>&1 &
TASKS_PID=$!
trap 'kill "$TASKS_PID" 2>/dev/null || true' EXIT

log "wait for /readyz (postgres + valkey + kafka)"
ready=""
for _ in $(seq 1 100); do
  if curl -fsS -o /dev/null "$BASE_URL/readyz" 2>/dev/null; then ready=1; break; fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "tasks never became ready — are the deps up? (just infra-up)"; cat "$RESULTS/tasks.log"; exit 1; }

log "k6 run — scenario=$SCENARIO target=$BASE_URL"
k6 run \
  -e BASE_URL="$BASE_URL" \
  -e SCENARIO="$SCENARIO" \
  --summary-export "$RESULTS/summary-tasks-$SCENARIO.json" \
  "$ROOT/benchmarks/k6-tasks.js" | tee "$RESULTS/stdout-tasks-$SCENARIO.log"

log "done — results in benchmarks/results/ (summary-tasks-$SCENARIO.json)"
