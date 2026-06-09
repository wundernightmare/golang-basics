// Package otelx is the shared distributed-tracing setup for golang-basics
// services. It is the tracing analogue of libs/httpx's metrics: a service calls
// [Init] once at boot to wire an OpenTelemetry tracer provider (OTLP/gRPC
// exporter) and the W3C trace-context propagators, then attaches
// [GinMiddleware] to its HTTP engine so every request becomes a span.
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
//	shutdown, _ := otelx.Init(ctx, otelCfg)
//	defer shutdown(context.Background())
//	srv.Engine().Use(otelx.GinMiddleware(otelCfg.ServiceName))
package otelx
