// Package httpx is the shared HTTP-server scaffolding for golang-basics
// services — the example "common library" of this monorepo (the Go analogue
// of a shared crate like worker-core / telemetry in the Rust sibling repo).
// It is built on the standard library's net/http and the Go 1.22+ ServeMux;
// it pulls in no web framework.
//
// It bundles the boilerplate every service repeats:
//
//   - a configured [http.ServeMux] for the API behind a middleware chain:
//     request ids, tracing (a server span per request from the global
//     OpenTelemetry provider), sampled, trace-correlated request logging,
//     Prometheus metrics with trace exemplars, panic recovery, a request-body
//     limit, a per-request deadline, and 404/405 as problem+json (see [Server]);
//   - a separate admin listener with /healthz, /livez, /readyz, /metrics, /version,
//     /admin/config, /admin/log-level and /debug/pprof, kept off the API port
//     so an ingress never exposes them; /admin and /debug take a bearer token;
//   - runtime debugging without a redeploy: the log level switchable for a
//     while (see [LogLevel]), debug logging for one request via X-Debug-Token
//     (see [WithDebugLogging]), the effective config with secrets redacted
//     (see [Redact], [WithConfig]);
//   - Prometheus metrics: build_info, request count / latency / sizes /
//     in-flight / panics, plus whatever collectors the service registers from
//     the data libs (see [Metrics]);
//   - liveness and readiness backed by a pluggable check registry (see [Health]);
//   - environment- and YAML-driven configuration with a per-service prefix,
//     validated before the server starts (see [Config], [LoadYAML]);
//   - structured logging via log/slog with trace_id/span_id/request_id from
//     the context and zap-style sampling of debug/info records (see [NewLogger]);
//   - RFC 9457 problem details as the one error shape, with the cause of a 5xx
//     logged and recorded on the span (see [Problem], [WriteError]);
//   - the binary's build identity, injected at build time (see [Version], [Build]);
//   - graceful shutdown wired to an [os/signal] context: readiness off, a
//     drain delay, a bounded drain, then a forced close (see [Server.Run] and
//     [SignalContext]).
//
// A service typically does:
//
//	cfg, err := httpx.LoadConfig("PING_")
//	cfg.Service = "ping"
//	log := httpx.NewLogger(cfg.LogConfig())
//	srv, err := httpx.NewServer(cfg, log)
//	srv.Mux().HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {
//		httpx.WriteJSON(w, http.StatusOK, map[string]string{"message": "pong"})
//	})
//	ctx, stop := httpx.SignalContext()
//	defer stop()
//	err = srv.Run(ctx)
//
// Log with the context in hand (log.InfoContext(ctx, …)) so the line carries
// the trace it belongs to. Metrics are Prometheus-native (pull); traces are
// the only signal pushed through OpenTelemetry — see libs/otelx.
package httpx
