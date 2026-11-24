#!/usr/bin/env bash
# Chaos lab infra: Postgres <- PgBouncer, ClickHouse (HTTP), Toxiproxy in front of both.
# Everything lives in its own podman network and uses its own host ports (281xx/28474),
# so it does not collide with the repo's singleton docker/local.yml stack.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NET=chaoslab
TOXI_API_PORT=28474
PROXY_PORTS=28100-28399
# Registry hosts are variables with public defaults (closed-network template rule).
DOCKER_HUB="${DOCKER_HUB:-docker.io}"
GHCR="${GHCR:-ghcr.io}"

up() {
  podman network exists "$NET" || podman network create "$NET" >/dev/null
  podman image exists localhost/chaoslab-pgbouncer:dev ||
    podman build -q --build-arg "DOCKER_HUB=$DOCKER_HUB" -t localhost/chaoslab-pgbouncer:dev -f "$HERE/pgbouncer.Containerfile" "$HERE" >/dev/null
  podman run -d --replace --name chaoslab-pg --network "$NET" \
    -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=app \
    "$DOCKER_HUB/library/postgres:18.6-alpine" >/dev/null
  podman run -d --replace --name chaoslab-ch --network "$NET" --ulimit nofile=262144:262144 \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    "$DOCKER_HUB/clickhouse/clickhouse-server:25.8-alpine" >/dev/null
  podman run -d --replace --name chaoslab-pgbouncer --network "$NET" \
    -v "$HERE/pgbouncer.ini:/etc/pgbouncer/pgbouncer.ini:ro,Z" \
    localhost/chaoslab-pgbouncer:dev >/dev/null
  podman run -d --replace --name chaoslab-toxiproxy --network "$NET" \
    -p "127.0.0.1:${TOXI_API_PORT}:8474" -p "127.0.0.1:${PROXY_PORTS}:${PROXY_PORTS}" \
    "$GHCR/shopify/toxiproxy:2.12.0" -host=0.0.0.0 >/dev/null
  echo "waiting for postgres / clickhouse / pgbouncer…"
  for _ in $(seq 60); do
    podman exec chaoslab-pg pg_isready -q -U postgres -d app 2>/dev/null &&
      podman exec chaoslab-ch clickhouse-client -q 'SELECT 1' >/dev/null 2>&1 &&
      podman exec chaoslab-pg psql -qtA -h chaoslab-pgbouncer -p 6432 -U postgres app -c 'select 1' >/dev/null 2>&1 &&
      curl -fsS "http://127.0.0.1:${TOXI_API_PORT}/version" >/dev/null 2>&1 && {
        podman exec chaoslab-ch clickhouse-client -q "CREATE DATABASE IF NOT EXISTS chaos; CREATE TABLE IF NOT EXISTS chaos.sink (s String) ENGINE = Null"
        echo "lab is up"; return 0
      }
    sleep 1
  done
  echo "lab did not become ready" >&2; exit 1
}

down() {
  sudo nft delete table inet chaoslab 2>/dev/null || true
  podman rm -f -t 0 chaoslab-toxiproxy chaoslab-pgbouncer chaoslab-ch chaoslab-pg >/dev/null 2>&1 || true
  podman network rm -f "$NET" >/dev/null 2>&1 || true
  echo "lab is down"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
