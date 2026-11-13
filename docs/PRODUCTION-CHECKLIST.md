# Production checklist

What a service built from this repo gets, rule by rule, where it lives and
which test proves it. "Opt-in" means the capability is there and off by
default; the key that turns it on is named.

## Runtime

| Rule | Where | Proof |
| --- | --- | --- |
| Memory limit from the cgroup, not a hand-written `GOMEMLIMIT`; re-read on an in-place resize | `libs/httpx/runtime.go` (`RUNTIME_MEMORY_LIMIT_RATIO`), [RUNTIME.md](RUNTIME.md) | `libs/httpx/runtime_test.go` |
| `GOMAXPROCS` from the cgroup CPU limit | the Go runtime itself (≥ 1.25); never set in a manifest | [RUNTIME.md](RUNTIME.md) |
| pprof behind a token on the internal port | `libs/httpx/admin.go` (`/debug/pprof/*`) | `libs/httpx/admin_test.go` |
| Profiles shipped to object storage when it matters: heap + goroutine profile at memory pressure, the crash report of a dead pod | `libs/httpx/diagnostics.go`, `srv.SetDumpSink(store)`, [DIAGNOSTICS.md](DIAGNOSTICS.md) | `libs/httpx/diagnostics_test.go` |
| Flight recorder: the last seconds of the execution trace, shipped on a slow request or memory pressure — **opt-in** `DIAG_FLIGHT_RECORDER` | `libs/httpx/flight.go` | `libs/httpx/flight_test.go` (a real server; `go tool trace` reads the snapshot); into a real object store: `services/ingest/internal/integration/diagnostics_test.go` |
| Continuous profiling | pull model: Parca / Pyroscope / Alloy scrape `/debug/pprof/*` on the admin port with the admin token. No push SDK in the binary | the endpoints: `libs/httpx/admin_test.go` |
| Runtime metrics from `runtime/metrics`, not `ReadMemStats` | `libs/httpx/metrics.go`: the Go collector (GC, heap, goroutines) plus `go_sched_latencies_seconds` | `libs/httpx/metrics_test.go` |
| Allocation control on hot paths | where there is one: `libs/tiercache` pools its buffers and has an allocation budget test (`AllocsPerRun`) | `libs/tiercache/cache_test.go` |
| FIPS 140-3 crypto without cgo — **opt-in** `FIPS=1` at build | `scripts/build-service.sh`, `docker/service.Dockerfile`, [RUNTIME.md](RUNTIME.md); needs the pure-Go Kafka provider | `e2e/fips_test.go` |

## Transport and resilience

| Rule | Where | Proof |
| --- | --- | --- |
| Graceful shutdown: SIGINT/SIGTERM → readiness off → drain with a hard timeout → workers stop | `httpx.SignalContext`, `Server.Run` (`HTTP_SHUTDOWN_DELAY`, `HTTP_SHUTDOWN_TIMEOUT`) | `libs/httpx/server_test.go`, `e2e/` (SIGTERM exits 0) |
| Server timeouts (Slowloris): `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`, a per-request deadline | `libs/httpx/config.go` (`HTTP_*_TIMEOUT`) | `libs/httpx/server_test.go` |
| No `http.DefaultClient`: a transport with connect / TLS / response-header timeouts and pool limits | `libs/resilient-http-client/transport.go` | `libs/resilient-http-client/integration` (real servers, HTTP/1.1–3) |
| Outbound resilience: backoff with full jitter, `Retry-After`, retry budget, circuit breaker, per-attempt timeout, deadlines propagated | `libs/resilient-http-client`, `libs/grpcx`, `libs/resilience` | unit + mutation; `…/integration/chaos_test.go` (Toxiproxy) |
| Inbound rate limit: token bucket, global and per client — **opt-in** `HTTP_RATE_LIMIT`, `HTTP_RATE_LIMIT_PER_CLIENT` | `libs/httpx/ratelimit.go` | `libs/httpx/ratelimit_test.go` (virtual time) |
| Request body limit (`http.MaxBytesReader`) → 413 | `libs/httpx/middleware.go` (`HTTP_MAX_BODY_BYTES`) | `libs/httpx/server_test.go`; `just schemathesis` |

## Observability and operations

| Rule | Where | Proof |
| --- | --- | --- |
| Operational endpoints on a separate port, token-guarded | `libs/httpx/admin.go` (`ADMIN_ADDR`, `ADMIN_TOKEN`) | `libs/httpx/admin_test.go`, `e2e/` |
| Liveness is not readiness; readiness is cached, tiered (critical / optional) | `libs/httpx/health.go` | `libs/httpx/health_test.go`; chaos in `services/tasks/internal/integration` |
| RED metrics in and out, runtime metrics, `build_info` | `libs/httpx/metrics.go`, the clients' `metrics.go`; the contract: [METRICS.md](METRICS.md) | `telemetry_test.go` per module (`promlint` included) |
| Traces over OTLP, W3C propagation | `libs/otelx` (`OTEL_*`) | `libs/otelx/telemetry_test.go` |
| Tail-based sampling: errors, slow traces and debug subjects kept in full | the collector: `docker/observability/otel-collector.yml` | `libs/otelx/integration` (the real collector with that file) |
| Structured logs with trace correlation; ECS field names — **opt-in** `LOG_FORMAT=ecs` | `libs/httpx/logging.go`, `logformat.go` | `libs/httpx/logformat_test.go` |
| Private data masked in logs and in `/admin/config`; credentials typed so they cannot be printed | `libs/httpx/logformat.go`, `redact.go`, `libs/secret` | `logformat_test.go`, `redact_test.go` (+ fuzz), `libs/secret` |
| Log level changed at runtime, with a TTL | `PUT /admin/log-level` | `libs/httpx/admin_test.go` |
| Debug logs and a kept trace for one tenant / user, with a TTL | `PUT /admin/debug-subjects/{subject}`, `srv.DebugSubject` | `libs/httpx/subjects_test.go` (virtual time) |
| Debug logs for one request | `X-Debug-Token` | `libs/httpx/debug_test.go` |
| Panic recovery: handlers (500 problem, stack in the log, metric) and background tasks (`srv.Guard`: error, metric, orderly stop) | `libs/httpx/middleware.go`, `guard.go`; `libs/kafka/consumer.go` for a message handler | `server_test.go`, `ratelimit_test.go`, `libs/kafka` |
| Minimal container: multi-stage, distroless static, non-root, CA certificates, read-only root | `docker/service.Dockerfile`, `examples/ingest-k8s` | `just docker-scan-ci` (hadolint, grype) |

## Data and configuration

| Rule | Where | Proof |
| --- | --- | --- |
| Config validated at start, fail fast; unknown YAML keys are errors; `--check-config`, `--config-reference` | `httpx.LoadYAML`, `Boot`, each lib's `Validate` | `libs/httpx/boot_test.go`, `config_test.go` |
| Connection pools sized explicitly | `libs/pgx` (`DB_MAX_CONNS`, `DB_MIN_CONNS`, lifetimes), the HTTP client's pool keys | `libs/pgx`; behind PgBouncer: `libs/pgx/pgbouncer_test.go` |
| Server-side guard rails on every database session, verified at boot | `libs/pgx` (`DB_STATEMENT_TIMEOUT`, …, `ErrGuardRailOff`) | `libs/pgx/pgx_test.go`, `pgbouncer_test.go` |
| Version, revision and build time from ldflags: start-up log, `build_info`, `/version` | `scripts/build-service.sh`, `libs/httpx/version.go` | `e2e/` |

## Code quality and supply chain

| Rule | Where | Proof |
| --- | --- | --- |
| Linting: golangci-lint with everything on (`errcheck`, `govet`, `gosec`, `bodyclose`, `sqlclosecheck`, …); exceptions listed with reasons | `.golangci.yml` | CI `lint` per module |
| Tests under `-race` | CI `unit`, `integration`; nightly `stress` (`-count=3`) | `.github/workflows/ci.yml` |
| Vulnerabilities: `govulncheck`, osv-scanner, grype; secrets: gitleaks; SAST: semgrep | `just sec`, CI `appsec` | the jobs |
| Dependencies pinned and checked: `go.sum`, `tidy-check` (a stale `go.mod` fails), actions pinned by SHA | `scripts/tidy-check.sh` | CI `tidy` |
| Real dependencies in tests, not mocks: Postgres, Valkey, Kafka, S3, Toxiproxy; libraries without Docker keep such suites in a nested module | `libs/testx/containers`, `<lib>/integration` | CI `integration` matrix |
| Deterministic concurrency tests on virtual time (`testing/synctest`) | `libs/tiercache`, `libs/httpx` (rate limit, debug subjects) | those tests |
| Fuzzing in CI | every parser of external bytes has a `Fuzz*` target; nightly `fuzz` job | `scripts/fuzz.sh` |
| Mutation testing of decision logic | `.gremlins.yaml` in the HTTP client, `grpcx`, `resilience` | nightly `mutation` job |

## Deliberately not here

- **A push-mode profiler SDK** (Pyroscope) in the binary: the pprof endpoints
  are scraped instead, so the profiler is the platform's choice, not a
  dependency of every service.
- **OTLP for metrics and logs**: metrics are Prometheus (scraped from the
  admin port), logs are stdout for whatever tails the container. One
  exporter, one failure mode.
- **Arenas**: not a supported Go feature; pooled buffers where a hot path
  needs them.
- **A gRPC server scaffold**: `grpcx` is the outbound side. A service that
  serves gRPC needs its own recovery interceptor — the rule in the table
  above covers HTTP handlers and background tasks.
