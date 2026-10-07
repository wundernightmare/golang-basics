// Package otelx is the shared distributed-tracing setup for golang-basics
// services. It is the tracing analogue of libs/httpx's metrics: a service calls
// [Init] once at boot to wire an OpenTelemetry tracer provider (OTLP/HTTP
// exporter, TLS by default) and the W3C trace-context propagators; libs/httpx
// starts a server span per request from that global provider, and the data
// libs' instrumentation (otelpgx, kotel, valkeyotel) hangs client spans off it.
//
// It is kept out of libs/httpx on purpose: ping and heartbeat stay free of the
// OpenTelemetry dependency tree, while services that genuinely span process
// boundaries (services/tasks → Kafka → services/consumer) opt in by importing
// otelx. Tracing is also opt-in at runtime — with OTEL_ENABLED=false (the
// default) [Init] installs only the propagators and a no-op provider, so the
// same binary runs with or without a collector.
//
// A service typically does:
//
//	otelCfg, _ := otelx.LoadConfig("TASKS_")
//	shutdown, _ := otelx.Init(ctx, otelCfg, log)
//	defer shutdown(context.Background())
//
// Metrics are deliberately not OpenTelemetry: the workspace scrapes Prometheus
// from the admin listeners (libs/httpx.Metrics and the libs' collectors), so
// no meter provider is installed and the instrumentation libraries' meters
// stay no-ops.
package otelx
