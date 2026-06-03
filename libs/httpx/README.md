# httpx

Shared HTTP-server scaffolding for golang-basics services — the example
**common library** of this monorepo (the Go analogue of a shared crate like
`worker-core` / `telemetry` in the Rust sibling repo).

It packages the boilerplate every service otherwise re-writes:

| Concern            | What you get                                                                 |
| ------------------ | ---------------------------------------------------------------------------- |
| HTTP engine        | A configured [`gin`](https://github.com/gin-gonic/gin) engine via `NewServer` |
| Request logging    | One structured `log/slog` line per request (4xx→warn, 5xx→error)             |
| Metrics            | `/metrics` + a middleware recording `http_requests_total` / `_duration_seconds` on a private registry |
| Health             | `/healthz` (liveness) and `/readyz` (gate + pluggable checks)                |
| Config             | `LoadConfig(prefix)` — env-driven, fully defaulted                           |
| Logging setup      | `NewLogger(level, format)` — JSON or text                                    |
| Graceful shutdown  | `Server.Run(ctx)` drains within `ShutdownTimeout`; `SignalContext()` for SIGINT/SIGTERM |

## Usage

```go
cfg, err := httpx.LoadConfig("PING_")   // PING_HTTP_ADDR, PING_LOG_LEVEL, …
if err != nil {
    log.Fatal(err)
}
logger := httpx.NewLogger(cfg.LogLevel, cfg.LogFormat)
srv := httpx.NewServer(cfg, logger)

// Register your routes on the shared engine.
srv.Engine().GET("/ping", func(c *gin.Context) { c.String(200, "pong") })

// Optional readiness checks.
srv.Health.Register("upstream", func(ctx context.Context) error { return nil })

ctx, stop := httpx.SignalContext()
defer stop()
if err := srv.Run(ctx); err != nil {
    logger.Error("server exited", "err", err)
    os.Exit(1)
}
```

## Endpoints provided for free

| Route       | Purpose                                                  |
| ----------- | -------------------------------------------------------- |
| `/healthz`  | Liveness — `200 {"status":"ok"}` while the process runs  |
| `/readyz`   | Readiness — `200` only when the gate is open and every registered check passes, else `503` with a per-check breakdown |
| `/metrics`  | Prometheus exposition for this server's private registry |

## Configuration

Loaded from the environment with a per-service prefix (so several binaries can
coexist). With prefix `PING_`:

| Variable                     | Default  | Meaning                       |
| ---------------------------- | -------- | ----------------------------- |
| `PING_HTTP_ADDR`             | `:8080`  | listen address                |
| `PING_HTTP_SHUTDOWN_TIMEOUT` | `10s`    | graceful-shutdown budget      |
| `PING_LOG_LEVEL`             | `info`   | `debug`/`info`/`warn`/`error` |
| `PING_LOG_FORMAT`            | `json`   | `json` or `text`              |

## Develop

```sh
just test     # go test ./...
just lint     # golangci-lint run
just cov      # coverage summary
```
