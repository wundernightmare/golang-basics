// Package integration is the integration suite of libs/otelx: the path a span
// really takes — the SDK that otelx.Init builds, OTLP over HTTP, the
// OpenTelemetry Collector running the configuration this repository ships
// (docker/observability/otel-collector.yml), a trace store — and what the
// collector's tail sampling lets through.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client — stays out of otelx's
// go.mod.
//
// See README.md for the stack and what each test owns.
package integration
