# httpx

Shared HTTP-server scaffolding for golang-basics services — the example
**common library** of this monorepo (the Go analogue of a shared crate like
`worker-core` / `telemetry` in the Rust sibling repo).

It packages the boilerplate every service otherwise re-writes:

| Concern            | What you get                                                                 |
| ------------------ | ---------------------------------------------------------------------------- |
| HTTP engine        | A configured [`gin`](https://github.com/gin-gonic/gin) engine via `NewServer` for the API listener |
| Admin listener     | A second, plain `net/http` listener with `/healthz`, `/readyz`, `/metrics`, `/version`, `/admin/config`, `/admin/log-level`, `/debug/pprof` — never on the API port |
| Request id         | `X-Request-Id` honoured or generated, echoed on the response, in every log line of the request (`request_id`) and in every `problem+json` body |
| Request logging    | One structured line per API request (`route`, `status`, `latency_ms`, `bytes`): info, 4xx → warn, 5xx → error, slower than `HTTP_SLOW_REQUEST` → warn |
| Logging setup      | `NewLogger(LogConfig)` — JSON/text to stdout, `trace_id`/`span_id`/`request_id` from the context, zap-style sampling of debug/info |
| Runtime debugging  | Log level switchable at runtime with an auto-revert TTL (`PUT /admin/log-level`); debug logging for one request via `X-Debug-Token`; the effective config, secrets redacted, on `/admin/config` |
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

// Optional: show the service's own config on /admin/config (redacted) instead
// of the bare httpx.Config — see "Runtime debugging" below.
// srv := httpx.NewServer(cfg, logger, httpx.WithConfig(serviceCfg))

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

| Route                      | Purpose                                                  |
| -------------------------- | -------------------------------------------------------- |
| `GET /healthz`             | Liveness — `200 {"status":"ok"}` while the process runs  |
| `GET /readyz`              | Readiness — `200` only when the gate is open and every registered check passes, else `503` with a per-check breakdown |
| `GET /metrics`             | Prometheus exposition for this server's private registry (OpenMetrics negotiated) |
| `GET /version`             | `BuildInfo` JSON (service, version, revision, build time, Go version) plus `started_at` and `uptime_seconds` |
| `GET /admin/config`        | The effective configuration, passed through `Redact` (see below) |
| `GET /admin/log-level`     | `{"level","base","expires_at","max_ttl"}` — what is in effect and when it reverts |
| `PUT /admin/log-level`     | `?level=debug\|info\|warn\|error&ttl=30m` — change the level for a while; **auth** |
| `DELETE /admin/log-level`  | Revert to the configured level now; **auth**             |
| `GET /debug/pprof/…`       | Go runtime profiles (`profile`, `heap`, `goroutine`, `block`, `mutex`, `trace`) |

Nothing on this listener goes through the access log, the request metrics or
tracing, so probes and scrapes never show up as traffic. Keep it off the
ingress: it is the listener that exposes internals.

**auth**: the two mutations require `Authorization: Bearer <ADMIN_TOKEN>` when
`*_ADMIN_TOKEN` is set (constant-time compare, `401 problem+json` otherwise).
An empty token leaves them open — the laptop / compose default — and the
"admin server listening" log line says `auth=off` so it is never a surprise in
a cluster. Reads and probes never need a token.

## Runtime debugging

Three tools for "what is this pod doing", none of which needs a redeploy or a
new dependency:

**Log level for a while.** Every change made through the admin listener
expires — `ttl` defaults to and is capped at `*_LOG_LEVEL_MAX_TTL` (24h) — so
a debug level nobody remembered to turn off cannot fill a disk. The change and
the revert are logged at warn regardless of the level in effect.

```sh
curl -X PUT 'localhost:9080/admin/log-level?level=debug&ttl=30m' \
     -H "Authorization: Bearer $ADMIN_TOKEN"
# {"level":"debug","base":"info","previous":"info","expires_at":"…","max_ttl":"24h0m0s"}
curl -X DELETE localhost:9080/admin/log-level -H "Authorization: Bearer $ADMIN_TOKEN"
```

In code the same handle is `srv.LogLevel` (or `httpx.LogLevelOf(logger)`).

**Debug logging for one request.** With `*_DEBUG_TOKEN` set, a request that
carries it in `X-Debug-Token` runs with a context under which every log record
passes — whatever the level, never sampled — and the response carries
`X-Debug-Logging: on`. Nothing else changes, for anyone. A wrong or missing
token is ignored silently (the API port is public; it must not become an
oracle). The same switch is `httpx.WithDebugLogging(ctx)` for a worker that
wants to debug one message.

```sh
curl -H "X-Debug-Token: $DEBUG_TOKEN" localhost:8080/ping
```

Handlers get this for free as long as they log with the request context
(`log.DebugContext(ctx, …)`), which is the rule anyway.

**The effective config.** `GET /admin/config` shows what the process is
actually running with — after YAML, env and defaults — through `httpx.Redact`:
fields tagged `secret:"true"` or named like a secret (password, secret, token,
api key, private key, credential) become `[redacted]`, the password of any
URL with userinfo becomes `xxxxx`, durations read as `10s`. `secret:"false"`
opts a false positive out. Pass your service's config with
`httpx.WithConfig(cfg)`; without it the endpoint shows the `httpx.Config` the
server was built with.

**Correlation without tracing.** Tracing is opt-in and sampled; the request id
is neither. Every response carries `X-Request-Id` (the client's, if sane, else
16 hex chars), every log line written with the request context carries it as
`request_id`, and every `problem+json` body carries it as `request_id` next to
`instance` (the request path). `httpx.RequestIDFromContext(ctx)` reads it.

## Configuration

Loaded from the environment with a per-service prefix (so several binaries can
coexist). With prefix `PING_`:

| Variable                      | Default          | Meaning                                                    |
| ----------------------------- | ---------------- | ---------------------------------------------------------- |
| `PING_SERVICE_NAME`           | executable name  | `service` attribute in logs, `build_info`, spans           |
| `PING_HTTP_ADDR`              | `:8080`          | API listen address (empty = no API listener, worker shape) |
| `PING_ADMIN_ADDR`             | `:9080`          | admin listen address                                       |
| `PING_ADMIN_TOKEN`            | *(empty)*        | bearer token for `PUT`/`DELETE /admin/*`; empty = open     |
| `PING_DEBUG_TOKEN`            | *(empty)*        | `X-Debug-Token` value that turns on debug logging for one request; empty = off |
| `PING_HTTP_SHUTDOWN_TIMEOUT`  | `10s`            | graceful-shutdown budget                                   |
| `PING_HTTP_SLOW_REQUEST`      | `1s`             | log a request at warn above this latency (`0` = off)       |
| `PING_LOG_LEVEL`              | `info`           | `debug`/`info`/`warn`/`error` — the base level; changeable at runtime |
| `PING_LOG_LEVEL_MAX_TTL`      | `24h`            | cap (and default) for how long a runtime level change lasts |
| `PING_LOG_FORMAT`             | `json`           | `json` or `text`                                           |
| `PING_LOG_SAMPLE_INITIAL`     | `100`            | per message per second: pass the first N (`0` = no sampling) |
| `PING_LOG_SAMPLE_THEREAFTER`  | `100`            | … then every M-th                                          |

## Develop

```sh
just test     # go test ./...
just lint     # golangci-lint run
just cov      # coverage summary
```
