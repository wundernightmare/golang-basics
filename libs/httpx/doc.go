// Package httpx is the shared HTTP-server scaffolding for golang-basics
// services — the example "common library" of this monorepo (the Go analogue
// of a shared crate like worker-core / telemetry in the Rust sibling repo).
//
// It bundles the boilerplate every service repeats:
//
//   - a configured [gin.Engine] with structured request logging and panic
//     recovery (see [Server]);
//   - Prometheus metrics on /metrics, plus an HTTP middleware that records
//     request count and latency (see [Metrics]);
//   - liveness (/healthz) and readiness (/readyz) endpoints backed by a
//     pluggable check registry (see [Health]);
//   - environment-driven configuration with a per-service prefix (see [Config]);
//   - structured logging via log/slog (see [NewLogger]);
//   - graceful shutdown wired to an [os/signal] context (see [Server.Run] and
//     [SignalContext]).
//
// A service typically does:
//
//	cfg, _ := httpx.LoadConfig("PING_")
//	log := httpx.NewLogger(cfg.LogLevel, cfg.LogFormat)
//	srv := httpx.NewServer(cfg, log)
//	srv.Engine().GET("/ping", handler)
//	ctx, stop := httpx.SignalContext()
//	defer stop()
//	srv.Run(ctx)
package httpx
