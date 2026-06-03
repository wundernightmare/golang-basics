# benchmarks

k6 load tests for the golang-basics services. The Go analogue of the Rust
sibling repo's `benchmarks/` — same profile catalogue, minus the cross-store
invariant checks (these services have no datastores).

## Layout

| File           | Purpose                                                        |
| -------------- | ------------------------------------------------------------- |
| `k6-ping.js`   | The k6 script: profiles + checks + custom metrics for `/ping` |
| `run-k6.sh`    | Orchestrator — builds + starts ping, runs k6, tears down      |
| `results/`     | Per-run `summary-<profile>.json` + stdout log (gitignored)    |

## Profiles

Selected by the `SCENARIO` env var (the `just bench-*` recipes set it):

| Profile  | Shape                                  | `just` recipe   |
| -------- | -------------------------------------- | --------------- |
| `smoke`  | 50 VUs × 30s                           | `just bench-smoke`  |
| `load`   | ramp 0→500 VUs (~3.5m)                 | `just bench-load`   |
| `stress` | ramp 0→2000 VUs (~4m)                  | `just bench-stress` |
| `soak`   | 500 VUs × 30m (leak detection)         | `just bench-soak`   |
| `peak`   | constant-arrival-rate 25k req/s × 1m   | `just bench-peak`   |

## Run

```sh
just bench-smoke                 # build ping, run the smoke profile, tear down
# or directly:
benchmarks/run-k6.sh load
# or against an already-running service:
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=smoke benchmarks/k6-ping.js
```

## Thresholds

The script fails the run if any threshold is breached:

- `pong_errors` rate `< 1%`
- `http_req_failed` rate `< 1%`
- `http_req_duration` p95 `< 50ms` (a trivial handler should be well under this)

Tune them in `k6-ping.js` → `options.thresholds`.
