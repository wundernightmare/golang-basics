#!/usr/bin/env bash
# Reproduces the production incident end to end.
#
#   replica A, replica B ──pgx(pool=1)──> toxiproxy ──> PgBouncer ──> Postgres
#   replica A ──clickhouse-go HTTP(pool=1)──> toxiproxy:28302 ──> ClickHouse   <- fault goes here
#   replica B ──clickhouse-go HTTP(pool=1)──> toxiproxy:28303 ──> ClickHouse   (healthy)
#
# Usage: ./incident.sh [fault] [job] [observe_seconds] [op_timeout] [ch_fix]
#   fault: nft-drop (default) | toxi-bandwidth | toxi-timeout-down | ch-pause
#   job:   insert (default) | stream
#   op_timeout: 0 (prod behaviour, default) or e.g. 5s (fix A: context deadline)
#   ch_fix: false (default) | true (fix B: chfix idle deadlines on the ClickHouse connection)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

FAULT="${1:-nft-drop}"
JOB="${2:-insert}"
OBSERVE="${3:-60}"
OP_TIMEOUT="${4:-0}"
CH_FIX="${5:-false}"
TOXI=http://127.0.0.1:28474
PGB_PORT=28301 CH_A_PORT=28302 CH_B_PORT=28303
A=http://127.0.0.1:28501 B=http://127.0.0.1:28502
OUT="out/incident-${FAULT}-${JOB}-optimeout-${OP_TIMEOUT}-chfix-${CH_FIX}"
mkdir -p "$OUT"

psql_pg() { podman exec chaoslab-pg psql -qtA -U postgres app -c "$1"; }
toxi() { curl -fsS -X "$1" -H 'Content-Type: application/json' "$TOXI$2" ${3:+-d "$3"} >/dev/null; }
proxy() { toxi DELETE "/proxies/$1" 2>/dev/null || true; toxi POST /proxies "{\"name\":\"$1\",\"listen\":\"0.0.0.0:$2\",\"upstream\":\"$3\",\"enabled\":true}"; }

inject() {
  case "$FAULT" in
    nft-drop) sudo nft -f - <<EOF ;;
add table inet chaoslab
add chain inet chaoslab incident { type filter hook output priority 0; }
add rule inet chaoslab incident oifname lo tcp dport $CH_A_PORT drop
add rule inet chaoslab incident oifname lo tcp sport $CH_A_PORT drop
EOF
    toxi-bandwidth)
      toxi POST /proxies/inc_ch_a/toxics '{"name":"b_down","type":"bandwidth","stream":"downstream","attributes":{"rate":1}}'
      toxi POST /proxies/inc_ch_a/toxics '{"name":"b_up","type":"bandwidth","stream":"upstream","attributes":{"rate":1}}' ;;
    toxi-timeout-down)
      toxi POST /proxies/inc_ch_a/toxics '{"name":"t_down","type":"timeout","stream":"downstream","attributes":{"timeout":0}}' ;;
    ch-pause) podman pause chaoslab-ch >/dev/null ;;
    *) echo "unknown fault $FAULT" >&2; exit 2 ;;
  esac
}
heal() {
  sudo nft delete chain inet chaoslab incident 2>/dev/null || true
  toxi DELETE /proxies/inc_ch_a/toxics/b_down 2>/dev/null || true
  toxi DELETE /proxies/inc_ch_a/toxics/b_up 2>/dev/null || true
  toxi DELETE /proxies/inc_ch_a/toxics/t_down 2>/dev/null || true
  podman unpause chaoslab-ch >/dev/null 2>&1 || true
}
cleanup() {
  heal
  kill "${PID_A:-}" "${PID_B:-}" 2>/dev/null || true
  for p in inc_pgb inc_ch_a inc_ch_b; do toxi DELETE "/proxies/$p" 2>/dev/null || true; done
}
trap cleanup EXIT

ready() { # -> "READY" | "NOT READY (pg: …; ch: …)"
  local body
  body="$(curl -s -m 3 "$1/readyz" || echo '{"ready":false,"probe":"timeout"}')"
  python3 - "$body" <<'EOF'
import json, sys
d = json.loads(sys.argv[1])
if d.get("ready"):
    print("READY")
else:
    parts = [f"{k}: {v.get('err','ok')[:70]} [{v.get('took')}]" for k, v in d.items() if isinstance(v, dict) and not v.get("ok")]
    print("NOT READY (" + "; ".join(parts or ["probe timed out"]) + ")")
EOF
}
snapshot() {
  local t="$1"
  printf '\n[t=%+4ss] ---------------------------------------------------------------\n' "$t"
  printf '  A: pid %s %s | %s | %s\n' "$PID_A" "$(kill -0 "$PID_A" 2>/dev/null && echo alive || echo DEAD)" "$(curl -s -m 2 $A/state)" "$(ready $A)"
  printf '  B: pid %s %s | %s | %s\n' "$PID_B" "$(kill -0 "$PID_B" 2>/dev/null && echo alive || echo DEAD)" "$(curl -s -m 2 $B/state)" "$(ready $B)"
  echo "  pg advisory lock $(psql_pg "select coalesce(string_agg(format('pid=%s granted=%s state=%s xact_age=%ss wait=%s', l.pid, l.granted, a.state, extract(epoch from now()-a.xact_start)::int, coalesce(a.wait_event,'-')), ' || ' order by l.granted desc), 'nobody holds it') from pg_locks l join pg_stat_activity a using (pid) where l.locktype='advisory'")"
  echo "  job progress     $(psql_pg "select format('runs=%s last_by=%s %ss ago', runs, last_by, extract(epoch from now()-last_at)::int) from chaos_job")"
}

echo "== setup: fault=$FAULT job=$JOB op_timeout=$OP_TIMEOUT ch_fix=$CH_FIX observe=${OBSERVE}s"
GOWORK=off GOFLAGS=-mod=mod go build -o bin/svc ./cmd/svc
proxy inc_pgb "$PGB_PORT" chaoslab-pgbouncer:6432
proxy inc_ch_a "$CH_A_PORT" chaoslab-ch:8123
proxy inc_ch_b "$CH_B_PORT" chaoslab-ch:8123
podman exec chaoslab-ch clickhouse-client -q "CREATE DATABASE IF NOT EXISTS chaos; CREATE TABLE IF NOT EXISTS chaos.sink (s String) ENGINE = Null"
psql_pg "create table if not exists chaos_job (id int primary key, runs bigint not null default 0, last_by text, last_at timestamptz default now()); insert into chaos_job (id) values (1) on conflict (id) do update set runs = 0, last_by = null, last_at = now()"

./bin/svc -name a -addr 127.0.0.1:28501 -ch "127.0.0.1:$CH_A_PORT" -job "$JOB" -op-timeout "$OP_TIMEOUT" -ch-fix="$CH_FIX" 2>"$OUT/a.log" & PID_A=$!
./bin/svc -name b -addr 127.0.0.1:28502 -ch "127.0.0.1:$CH_B_PORT" -job "$JOB" -op-timeout "$OP_TIMEOUT" -ch-fix="$CH_FIX" 2>"$OUT/b.log" & PID_B=$!
sleep 6

echo; echo "== baseline (healthy)"
snapshot -4; sleep 2; snapshot -2

if [[ "$JOB" == stream ]]; then # a response-body fault only matters while a body is being read
  until curl -s -m 1 $A/state | grep -q '"phase":"ch-stream"'; do sleep 0.05; done
fi
echo; echo "== FAULT INJECTED on replica A's ClickHouse path: $FAULT"
inject
T0=$(date +%s)
while (( $(date +%s) - T0 < OBSERVE )); do
  sleep 10
  snapshot "$(( $(date +%s) - T0 ))"
done
curl -s -m 3 $A/stacks >"$OUT/a.stacks.txt" || true
curl -s -m 3 $B/stacks >"$OUT/b.stacks.txt" || true

echo; echo "== FAULT REMOVED (network healthy again)"
heal
T1=$(date +%s)
for _ in 1 2 3; do sleep 8; snapshot "heal+$(( $(date +%s) - T1 ))"; done

echo; echo "== replica logs"
sed 's/^/  A| /' "$OUT/a.log" | tail -n 8
sed 's/^/  B| /' "$OUT/b.log" | tail -n 8
echo; echo "goroutine dumps taken during the fault: $OUT/{a,b}.stacks.txt"
