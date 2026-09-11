// Package httpx is the shared HTTP-server scaffolding for golang-basics
// services — the example "common library" of this monorepo (the Go analogue
// of a shared crate like worker-core / telemetry in the Rust sibling repo).
//
// It bundles the boilerplate every service repeats:
//
//   - a configured [gin.Engine] for the API with sampled, trace-correlated
//     request logging and panic recovery (see [Server]);
//   - a separate admin listener with /healthz, /readyz, /metrics, /version and
//     /debug/pprof, kept off the API port so an ingress never exposes them;
//   - Prometheus metrics: build_info, request count / latency / in-flight, plus
//     whatever collectors the service registers from the data libs (see [Metrics]);
//   - liveness and readiness backed by a pluggable check registry (see [Health]);
//   - environment-driven configuration with a per-service prefix (see [Config]);
//   - structured logging via log/slog with trace_id/span_id from the context
//     and zap-style sampling of debug/info records (see [NewLogger]);
//   - the binary's build identity, injected at build time (see [Version], [Build]);
//   - graceful shutdown wired to an [os/signal] context (see [Server.Run] and
//     [SignalContext]).
//
// A service typically does:
//
//	cfg, _ := httpx.LoadConfig("PING_")
//	cfg.Service = "ping"
//	log := httpx.NewLogger(cfg.LogConfig())
//	srv := httpx.NewServer(cfg, log)
//	srv.Engine().GET("/ping", handler)
//	ctx, stop := httpx.SignalContext()
//	defer stop()
//	srv.Run(ctx)
//
// Log with the context in hand (log.InfoContext(ctx, …)) so the line carries
// the trace it belongs to.
package httpx
