# httpx

Shared HTTP-server scaffolding for golang-basics services — the example
**common library** of this monorepo (the Go analogue of a shared crate like
`worker-core` / `telemetry` in the Rust sibling repo).

It packages the boilerplate every service otherwise re-writes:

| Concern            | What you get                                                                 |
| ------------------ | ---------------------------------------------------------------------------- |
| HTTP engine        | A configured [`gin`](https://github.com/gin-gonic/gin) engine via `NewServer` for the API listener |
| Admin listener     | A second, plain `net/http` listener with `/healthz`, `/readyz`, `/metrics`, `/version`, `/debug/pprof` — never on the API port |
| Request logging    | One structured line per API request (`route`, `status`, `latency_ms`, `bytes`): info, 4xx → warn, 5xx → error, slower than `HTTP_SLOW_REQUEST` → warn |
| Logging setup      | `NewLogger(LogConfig)` — JSON/text to stdout, `trace_id`/`span_id` from the context, zap-style sampling of debug/info |
| Metrics            | `build_info`, `http_requests_total{method,path,status}`, `http_request_duration_seconds{method,path}` (classic + native histogram), `http_requests_in_flight`, `log_dropped_total{level}` on a private registry; register the data libs' `Collectors()` on it |
| Health             | `/healthz` (liveness) and `/readyz` (gate + pluggable checks)                |
| Build identity     | `httpx.Version` (set via `-ldflags -X`), `httpx.Build(service)`               |
| Config             | `LoadConfig(prefix)` — env-driven, fully defaulted; `LoadYAML` for a config-file overlay |
| Graceful shutdown  | `Server.Run(ctx)` drains the API within `ShutdownTimeout` while probes keep answering; `SignalContext()` for SIGINT/SIGTERM |

## Usage

```go
cfg, err := httpx.LoadConfig("PING_")   // PING_HTTP_ADDR, PING_ADMIN_ADDR, PING_LOG_LEVEL, …
if err != nil {
    log.Fatal(err)
}
cfg.Service = "ping"
logger := httpx.NewLogger(cfg.LogConfig())
srv := httpx.NewServer(cfg, logger)

// Register your routes on the API engine.
srv.Engine().GET("/ping", func(c *gin.Context) { c.String(200, "pong") })

// Optional readiness checks and extra collectors (from libs/pgx, valkey, kafka).
srv.Health.Register("upstream", func(ctx context.Context) error { return nil })
srv.Metrics.Registry.MustRegister(db.Collectors()...)

ctx, stop := httpx.SignalContext()
defer stop()
if err := srv.Run(ctx); err != nil {
    logger.Error("server exited", "err", err)
    os.Exit(1)
}
```

Log with the context in hand — `logger.InfoContext(ctx, "…")` — and the line
carries the trace it belongs to.

## Endpoints provided for free (admin listener)

| Route            | Purpose                                                  |
| ---------------- | -------------------------------------------------------- |
| `/healthz`       | Liveness — `200 {"status":"ok"}` while the process runs  |
| `/readyz`        | Readiness — `200` only when the gate is open and every registered check passes, else `503` with a per-check breakdown |
| `/metrics`       | Prometheus exposition for this server's private registry (OpenMetrics negotiated) |
| `/version`       | `BuildInfo` JSON: service, version, revision, build time, Go version |
| `/debug/pprof/…` | Go runtime profiles (`profile`, `heap`, `goroutine`, `block`, `mutex`, `trace`) |

Nothing on this listener goes through the access log, the request metrics or
tracing, so probes and scrapes never show up as traffic. Keep it off the
ingress: it is the listener that exposes internals.

## Configuration

Loaded from the environment with a per-service prefix (so several binaries can
coexist). With prefix `PING_`:

| Variable                      | Default          | Meaning                                                    |
| ----------------------------- | ---------------- | ---------------------------------------------------------- |
| `PING_SERVICE_NAME`           | executable name  | `service` attribute in logs, `build_info`, spans           |
| `PING_HTTP_ADDR`              | `:8080`          | API listen address (empty = no API listener, worker shape) |
| `PING_ADMIN_ADDR`             | `:9080`          | admin listen address                                       |
| `PING_HTTP_SHUTDOWN_TIMEOUT`  | `10s`            | graceful-shutdown budget                                   |
| `PING_HTTP_SLOW_REQUEST`      | `1s`             | log a request at warn above this latency (`0` = off)       |
| `PING_LOG_LEVEL`              | `info`           | `debug`/`info`/`warn`/`error`                              |
| `PING_LOG_FORMAT`             | `json`           | `json` or `text`                                           |
| `PING_LOG_SAMPLE_INITIAL`     | `100`            | per message per second: pass the first N (`0` = no sampling) |
| `PING_LOG_SAMPLE_THEREAFTER`  | `100`            | … then every M-th                                          |

## Develop

```sh
just test     # go test ./...
just lint     # golangci-lint run
just cov      # coverage summary
```
