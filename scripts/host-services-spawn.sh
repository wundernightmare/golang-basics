#!/usr/bin/env bash
# host-services-spawn.sh — build + run the golang-basics services on the host.
#
# The Go analogue of the Rust sibling repo's host-services-spawn.sh: it spawns
# each service as a background process, writing one pidfile + logfile per
# service under .run/, plus a shared env file you can source to point tools
# (curl, k6) at the right ports. There is no external infra to wait on — these
# services have no dependencies — so bring-up is just "build, run, health-poll".
#
# Usage:
#   scripts/host-services-spawn.sh                 # all services
#   scripts/host-services-spawn.sh ping            # only these
#   scripts/host-services-spawn.sh --stop          # stop everything started here
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/.run"
mkdir -p "$RUN_DIR"

# service : module-dir : health-url
declare -A HEALTH=(
  [ping]="http://localhost:8080/healthz"
  [heartbeat]="http://localhost:8081/healthz"
)
ALL_SERVICES=(ping heartbeat)

log() { printf '\033[36m▸ %s\033[0m\n' "$*"; }
die() { echo "host-services: $*" >&2; exit 1; }

stop_all() {
  shopt -s nullglob
  local stopped=0
  for pidfile in "$RUN_DIR"/*.pid; do
    local svc pid; svc="$(basename "$pidfile" .pid)"; pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      log "stopped $svc (pid $pid)"
      stopped=1
    fi
    rm -f "$pidfile"
  done
  [[ "$stopped" == 0 ]] && log "nothing running"
  exit 0
}

wait_for_health() {
  local url="$1" name="$2" tries=50
  for ((i = 0; i < tries; i++)); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  die "$name did not become healthy at $url"
}

[[ "${1:-}" == "--stop" ]] && stop_all

# Which services to start.
if [[ $# -gt 0 ]]; then
  services=("$@")
else
  services=("${ALL_SERVICES[@]}")
fi

: > "$RUN_DIR/env.sh"
for svc in "${services[@]}"; do
  [[ -n "${HEALTH[$svc]:-}" ]] || die "unknown service: $svc (known: ${ALL_SERVICES[*]})"

  log "build $svc"
  ( cd "$ROOT/services/$svc" && CGO_ENABLED=0 go build -o "$RUN_DIR/$svc" . )

  log "start $svc"
  ( cd "$ROOT" && "$RUN_DIR/$svc" ) > "$RUN_DIR/$svc.log" 2>&1 &
  echo $! > "$RUN_DIR/$svc.pid"

  wait_for_health "${HEALTH[$svc]}" "$svc"
  log "$svc healthy → ${HEALTH[$svc]}"
done

cat >> "$RUN_DIR/env.sh" <<'EOF'
export PING_URL=http://localhost:8080
export HEARTBEAT_URL=http://localhost:8081
EOF

cat <<EOF

✓ services up. Logs + pids under .run/
  curl -s localhost:8080/ping
  curl -s localhost:8081/metrics | grep heartbeat
  source .run/env.sh          # PING_URL / HEARTBEAT_URL
  just down                   # stop everything
EOF
