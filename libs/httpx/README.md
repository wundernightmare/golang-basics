# httpx

Shared HTTP-server scaffolding for golang-basics services — the example
**common library** of this monorepo (the Go analogue of a shared crate like
`worker-core` / `telemetry` in the Rust sibling repo). It is built on the
standard library's `net/http` and the Go 1.22+ `ServeMux` (method + pattern
routes, `r.PathValue`); it pulls in no web framework.

It packages the boilerplate every service otherwise re-writes:

| Concern            | What you get                                                                 |
| ------------------ | ---------------------------------------------------------------------------- |
| API listener       | `NewServer(cfg, log, opts...)` → an `http.ServeMux` (`srv.Mux()`) behind the built-in middleware chain (`srv.Handler()`) |
| Admin listener     | A second listener with `/healthz` (`/livez`), `/readyz`, `/metrics`, `/version`, `/admin/config`, `/admin/log-level`, `/debug/pprof` — never on the API port; `/admin` and `/debug` need a bearer token |
| Errors             | RFC 9457 `problem+json` everywhere: 404/405 from routing, panics, body limits, `WriteProblem` / `WriteError` from handlers; the cause of a 5xx is logged and recorded on the span, never sent |
| Request id         | `X-Request-Id` generated (or honoured from a trusted proxy), echoed on the response, in every log line of the request (`request_id`) and in every `problem+json` body |
| Request logging    | One structured line per API request: info (sampled) for 2xx–4xx, warn above `HTTP_SLOW_REQUEST`, error for 5xx |
| Logging setup      | `NewLogger(LogConfig)` — JSON/text to stdout, `trace_id`/`span_id`/`request_id` from the context (top level, also under `WithGroup`), zap-style sampling of debug/info |
| Runtime debugging  | Log level switchable at runtime with an auto-revert TTL; debug logging for one request via `X-Debug-Token`; the effective config, secrets redacted, on `/admin/config` |
| Metrics            | Prometheus, on a private registry: `build_info`, request count / latency / sizes / in-flight / panics by `route` template, health check results and latency, `log_dropped_total`; exemplars carry the `trace_id` |
| Tracing            | A server span per request from the global OpenTelemetry provider (install one with `libs/otelx`), named `GET /items/{id}` after routing |
| Health             | `/healthz` (liveness) and `/readyz` (gate + pluggable checks evaluated in the background, answered from cache) |
| Config             | `LoadConfig(prefix)` (env) or `LoadYAML(path, prefix, &cfg)` (file + env + defaults), validated before the server starts |
| Build identity     | `httpx.Version` / `Revision` / `BuildTime` (set via `-ldflags -X`), `httpx.Build(service)` |
| Graceful shutdown  | `Server.Run(ctx)`: readiness off → drain delay → bounded drain → forced close; `SignalContext()` for SIGINT/SIGTERM |

## Usage

```go
cfg, err := httpx.LoadConfig("PING_") // PING_HTTP_ADDR, PING_ADMIN_ADDR, PING_LOG_LEVEL, …
if err != nil {
    fmt.Fprintln(os.Stderr, err) // no logger yet
    os.Exit(1)
}
cfg.Service = "ping"
logger := httpx.NewLogger(cfg.LogConfig())

srv, err := httpx.NewServer(cfg, logger /*, httpx.WithConfig(svcCfg), httpx.WithMiddleware(auth) */)
if err != nil {
    logger.Error("invalid config", "err", err)
    os.Exit(1)
}

// Routes: Go 1.22 patterns, path parameters via r.PathValue.
srv.Mux().HandleFunc("GET /tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
    task, err := store.Get(r.Context(), r.PathValue("id"))
    if err != nil {
        httpx.WriteError(w, r, err) // a Problem as is, a deadline as 504, anything else as 500
        return
    }
    httpx.WriteJSON(w, http.StatusOK, task)
})

// Optional readiness checks and extra collectors (from libs/pgx, valkey, kafka).
srv.Health.Register("postgres", db.Ping)
srv.Health.Register("cache", cache.Ping, httpx.Optional()) // degraded, not unready
srv.Metrics.Registry.MustRegister(db.Collectors()...)

ctx, stop := httpx.SignalContext()
defer stop()
if err := srv.Run(ctx); err != nil {
    logger.Error("server exited", "err", err)
    os.Exit(1)
}
```

Log with the context in hand — `logger.InfoContext(r.Context(), "…")` — and
the line carries the trace and the request it belongs to.

In tests, drive `srv.Handler()` (the full API chain) and `srv.Admin()` with
`httptest` — no listener needed. When a test does need real sockets, use
`Addr: "127.0.0.1:0"` / `AdminAddr: "127.0.0.1:0"` and read the bound
addresses from `srv.ListenAddr()` / `srv.AdminListenAddr()` once `Run` has
started; never a fixed port.

## The middleware chain

In order, outermost first:

1. **entry** — request id, client address, per-request deadline
   (`HTTP_REQUEST_TIMEOUT`), `X-Request-Id` on the response;
2. **debug token** — when `DEBUG_TOKEN` is set (see below);
3. **tracing** — a server span per request (see *Traces* below);
4. **access log** — one line per request;
5. **metrics** — count, latency, sizes, in-flight;
6. **`WithMiddleware(...)` extras** — an authenticator, a tenant resolver, a
   rate limiter; they see the request id and the deadline, the handler sees
   what they add to the context, and a request they reject (401, 429) is
   still traced, logged and counted like any other;
7. **recovery** — a panic becomes a 500 problem, an error log with the stack,
   an error span and a tick of `http_panics_total`;
8. **body limit** — `HTTP_MAX_BODY_BYTES`; `DecodeJSON` turns an overrun into 413;
9. **routing** — unknown path → 404, known path with the wrong method → 405
   with `Allow`, both as `problem+json`; the matched pattern becomes the
   `route` of the logs, metrics and span (`httpx.RouteFromContext`).

`httpx.Middleware` is `func(http.Handler) http.Handler`.

## Errors: problem+json

| Helper                             | Does                                                                 |
| ---------------------------------- | -------------------------------------------------------------------- |
| `httpx.NewProblem(status, detail)` | A `Problem` (`type` defaults to `about:blank`, `title` to the status text) |
| `httpx.Internal(detail, cause)`    | A 500 with a cause                                                   |
| `p.WithCause(err)`                 | Attach the underlying error: logged (5xx) and recorded on the span, never sent |
| `httpx.WriteProblem(w, r, p)`      | Write it; fills `instance` (the path) and the `request_id` extension |
| `httpx.WriteError(w, r, err)`      | `Problem` (also wrapped) → itself; `context.DeadlineExceeded` → 504; anything else → 500 with `err` as the cause |
| `httpx.DecodeJSON(r, &v)`          | Decode one JSON value; returns a 413 / 400 `Problem` ready for `WriteError` |
| `httpx.WriteJSON(w, status, v)`    | A plain JSON response                                                |

`Problem` implements `error` (and `Unwrap`), so a store or service layer can
return one and the handler passes whatever it got to `WriteError`. Every 5xx
written through these helpers is logged at error level as `request failed`
with its cause, `request_id`, `trace_id` and `span_id`, and the span is marked
as an error. An out-of-range status degrades to 500 instead of panicking.

## Endpoints provided for free (admin listener)

| Route                      | Auth  | Purpose                                                  |
| -------------------------- | ----- | -------------------------------------------------------- |
| `GET /healthz`, `/livez`   | open  | Liveness — `200 {"status":"ok"}` while the process runs  |
| `GET /readyz`              | open  | Readiness — `200 ready` / `200 degraded` (only optional checks failing) / `503 not_ready`, with a per-check breakdown |
| `GET /metrics`             | open  | Prometheus exposition of the server's registry (OpenMetrics negotiated: exemplars, native histograms) |
| `GET /version`             | open  | `BuildInfo` (service, version, revision, build time, Go version) plus `started_at` and `uptime_seconds` |
| `GET /admin/config`        | token | The effective configuration, passed through `Redact`     |
| `GET /admin/log-level`     | token | `{"level","base","expires_at","max_ttl"}`                |
| `PUT /admin/log-level`     | token | `?level=debug\|info\|warn\|error&ttl=30m` — change the level for a while |
| `DELETE /admin/log-level`  | token | Revert to the configured level now                       |
| `GET /debug/pprof/…`       | token | Go runtime profiles (`profile`, `heap`, `goroutine`, `block`, `mutex`, `trace`) |

Nothing on this listener goes through the access log, the request metrics or
tracing, so probes and scrapes never show up as traffic. Keep it off the
ingress: it is the listener that exposes internals.

### Admin auth

Everything under `/admin/` and `/debug/` requires
`Authorization: Bearer <ADMIN_TOKEN>`. The scheme is matched
case-insensitively (RFC 9110); both sides are SHA-256-hashed before a
constant-time compare, so neither the token nor its length leaks through
timing. A rejection is `401 problem+json` with `WWW-Authenticate` and a warn
log line. The probes, `/metrics` and `/version` stay open: the kubelet and the
scraper call them without credentials.

An empty `ADMIN_TOKEN` leaves the guarded routes open, which `Config.Validate`
only allows when `ADMIN_ADDR` is loopback (`127.0.0.1:9080`, `localhost:9080`,
`[::1]:9080`) or `ADMIN_INSECURE=true` is set explicitly. The "admin server
listening" log line says `auth=bearer` or `auth=off`.

## Runtime debugging

**Log level for a while.** Every change expires — `ttl` defaults to and is
capped at `LOG_LEVEL_MAX_TTL` (24h) — so a debug level nobody remembered to
turn off cannot fill a disk. The change and the revert are logged at warn
regardless of the level in effect.

```sh
curl -X PUT 'localhost:9080/admin/log-level?level=debug&ttl=30m' -H "Authorization: Bearer $ADMIN_TOKEN"
# {"level":"debug","base":"info","previous":"info","expires_at":"…","max_ttl":"24h0m0s"}
curl -X DELETE localhost:9080/admin/log-level -H "Authorization: Bearer $ADMIN_TOKEN"
```

In code the same handle is `srv.LogLevel` (or `httpx.LogLevelOf(logger)`).

**Debug logging for one request.** With `DEBUG_TOKEN` set, a request that
carries it in `X-Debug-Token` runs with a context under which every log record
passes — whatever the level, never sampled — and the response carries
`X-Debug-Logging: on`. A wrong or missing token is ignored silently (the API
port is public; it must not become an oracle). `httpx.WithDebugLogging(ctx)`
is the same switch for a worker debugging one message.

**The effective config.** `GET /admin/config` shows what the process runs
with — after YAML, env and defaults — through `httpx.Redact`:

- fields and map keys named like a secret (`password`, `passwd`, `pwd`,
  `secret`, `token`, `api_key`, `private_key`, `credential`, `dsn`, `auth`,
  `cert`) or tagged `secret:"true"` become `[redacted]` (an empty value stays
  empty); a struct, slice or map under such a name is redacted as a whole;
  `secret:"false"` opts a false positive out;
- the password of a URL with userinfo becomes `xxxxx`
  (`postgres://app:xxxxx@db/app`, also when `net/url` cannot parse it), secret-looking query parameters
  (`?sslpassword=`, `?token=`, `?api_key=`) become `[redacted]`, and so does
  the password pair of a libpq keyword DSN (`host=db password=[redacted]`);
- durations read as `10s`; fields are named by their yaml tag.

Pass your service's config with `httpx.WithConfig(cfg)`; without it the
endpoint shows the `httpx.Config` the server was built with.

## Trusted proxies

`HTTP_TRUSTED_PROXIES` lists the CIDRs (or bare IPs) of the load balancers in
front of the service. Only when the TCP peer is one of them:

- `X-Forwarded-For` is walked from the right, skipping trusted hops, and the
  first untrusted address becomes the client (`client_ip` in the access log,
  `client.address` on the span, `httpx.ClientIPFromContext`); a malformed hop
  stops the walk;
- an inbound `X-Request-Id` (1–128 printable ASCII characters) is kept
  instead of minting one.

From anyone else both headers are ignored: the public internet does not get
to choose its address or how its requests are correlated.

## Observability: metrics are Prometheus, traces are OTel

**Metrics** are Prometheus-native (pulled from `/metrics`). Each `Server` has
its own registry, `srv.Metrics.Registry`; register the data libs'
`Collectors()` on it.

| Metric                                            | Type      | Labels                     |
| ------------------------------------------------- | --------- | -------------------------- |
| `build_info`                                      | gauge (1) | `service, version, revision, go_version` |
| `http_requests_total`                             | counter   | `method, route, status`    |
| `http_request_duration_seconds`                   | histogram (classic + native) | `method, route` |
| `http_request_size_bytes`                         | histogram | `method, route` (requests with a body) |
| `http_response_size_bytes`                        | histogram | `method, route`            |
| `http_requests_in_flight`                         | gauge     |                            |
| `http_panics_total`                               | counter   |                            |
| `health_check_up`                                 | gauge     | `check, critical`          |
| `health_check_duration_seconds`                   | histogram | `check`                    |
| `log_dropped_total`                               | counter   | `level`                    |

`route` is the matched pattern (`/tasks/{id}`) or `unmatched`, never the raw
URL; `method` is one of the standard methods or `_OTHER`. No client can grow
the series. When the request's span is sampled, `http_requests_total` and the
latency histogram carry its `trace_id` as an exemplar.

**Traces** are OpenTelemetry. The server starts one `SpanKindServer` span per
request from the *global* tracer provider, continuing an inbound
`traceparent`. It is renamed to `GET /tasks/{id}` after routing and carries
`http.request.method`, `http.route`, `http.response.status_code`, `url.path`,
`client.address` and friends; a 5xx marks it as an error. Without a provider
(`libs/otelx` not initialised, or tracing disabled) the spans are no-ops. The
OTel meter is never installed.

## Graceful shutdown

When the context passed to `Run` is cancelled (`SignalContext` cancels it on
the first SIGINT/SIGTERM and then releases the handler, so a second signal
kills the process immediately):

1. readiness flips to 503, so load balancers stop sending new connections;
2. `HTTP_SHUTDOWN_DELAY` passes while requests are still served — the time
   the readiness change takes to propagate;
3. the API listener drains within `HTTP_SHUTDOWN_TIMEOUT`; requests still
   running after that have their contexts cancelled and their connections
   closed, and `Run` returns an error;
4. the admin listener stops last, so probes answer throughout.

A clean shutdown returns nil. A port in use is returned by `Run` directly.

## Configuration

`LoadConfig(prefix)` reads the environment; `LoadYAML(path, prefix, &cfg)`
reads an optional YAML file and overlays the environment, with the precedence
**env > yaml > envDefault > zero**. Unknown YAML keys are an error. A service
config embeds the lib configs instead of copying fields:

```go
type Config struct {
    httpx.Config `yaml:",inline"`
    Postgres     pgx.Config `yaml:"postgres" envPrefix:"DB_"`
}
```

Either way `Config.Validate` (also run by `NewServer`) rejects a config the
server cannot run safely with, reporting every problem at once.

With prefix `PING_`:

| Variable                         | YAML key                | Default          | Meaning                                                    |
| -------------------------------- | ----------------------- | ---------------- | ---------------------------------------------------------- |
| `PING_SERVICE_NAME`              | `service_name`          | executable name  | `service` in logs, `build_info`, spans                     |
| `PING_HTTP_ADDR`                 | `http_addr`             | `:8080`          | API listen address (empty = no API listener, worker shape) |
| `PING_ADMIN_ADDR`                | `admin_addr`            | `:9080`          | admin listen address                                       |
| `PING_ADMIN_TOKEN`               | `admin_token`           | *(empty)*        | bearer token for `/admin/*` and `/debug/*`                 |
| `PING_ADMIN_INSECURE`            | `admin_insecure`        | `false`          | allow an empty token on a non-loopback admin address       |
| `PING_DEBUG_TOKEN`               | `debug_token`           | *(empty)*        | `X-Debug-Token` value for one-request debug logging; empty = off |
| `PING_HTTP_READ_HEADER_TIMEOUT`  | `read_header_timeout`   | `5s`             | time to read the request headers                           |
| `PING_HTTP_READ_TIMEOUT`         | `read_timeout`          | `30s`            | time to read the whole request                             |
| `PING_HTTP_WRITE_TIMEOUT`        | `write_timeout`         | `30s`            | time to write the response                                 |
| `PING_HTTP_IDLE_TIMEOUT`         | `idle_timeout`          | `120s`           | keep-alive idle limit                                      |
| `PING_HTTP_REQUEST_TIMEOUT`      | `request_timeout`       | `30s`            | per-request context deadline (≤ 0 = none)                  |
| `PING_HTTP_MAX_HEADER_BYTES`     | `max_header_bytes`      | `1048576`        | request header limit                                       |
| `PING_HTTP_MAX_BODY_BYTES`       | `max_body_bytes`        | `1048576`        | request body limit (larger → 413)                          |
| `PING_HTTP_SHUTDOWN_DELAY`       | `shutdown_delay`        | `2s`             | serve on after readiness flips, before draining            |
| `PING_HTTP_SHUTDOWN_TIMEOUT`     | `shutdown_timeout`      | `10s`            | drain budget                                               |
| `PING_HTTP_SLOW_REQUEST`         | `slow_request`          | `1s`             | log a request at warn above this latency (`0` = off)       |
| `PING_HTTP_TRUSTED_PROXIES`      | `trusted_proxies`       | *(none)*         | comma-separated CIDRs / IPs of the proxies in front        |
| `PING_HEALTH_CHECK_INTERVAL`     | `health_check_interval` | `10s`            | how often readiness checks run in the background           |
| `PING_HEALTH_CHECK_TIMEOUT`      | `health_check_timeout`  | `3s`             | per-check timeout (must be shorter than the interval)      |
| `PING_LOG_LEVEL`                 | `log_level`             | `info`           | `debug`/`info`/`warn`/`error` — the base level             |
| `PING_LOG_LEVEL_MAX_TTL`         | `log_level_max_ttl`     | `24h`            | cap (and default) for a runtime level change               |
| `PING_LOG_FORMAT`                | `log_format`            | `json`           | `json` or `text`                                           |
| `PING_LOG_SAMPLE_INITIAL`        | `log_sample_initial`    | `100`            | per message per second: pass the first N (`0` = no sampling) |
| `PING_LOG_SAMPLE_THEREAFTER`     | `log_sample_thereafter` | `100`            | … then every M-th                                          |

Note that the defaults put the admin listener on every interface with no
token, which `Validate` rejects: set `ADMIN_TOKEN`, a loopback `ADMIN_ADDR`,
or `ADMIN_INSECURE=true` (local development).

## Build identity

```sh
go build -ldflags "-X github.com/tracehubmmp/golang-basics/libs/httpx.Version=v1.4.2 \
                   -X github.com/tracehubmmp/golang-basics/libs/httpx.Revision=$(git rev-parse HEAD) \
                   -X github.com/tracehubmmp/golang-basics/libs/httpx.BuildTime=$(date -u +%FT%TZ)"
```

The toolchain's embedded VCS info wins when present (a build inside a
checkout); the injected values cover Docker builds, whose context has no `.git`.

## Develop

```sh
just test     # go test ./...
just lint     # golangci-lint run
just cov      # coverage summary
go test -run XXX -fuzz FuzzRedact -fuzztime 30s   # FuzzProblemJSON, FuzzLoadYAML, FuzzRedact
```
