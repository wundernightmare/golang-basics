package pgx

import "go.opentelemetry.io/otel/trace"

// Option configures [New].
type Option func(*options)

type options struct {
	tp trace.TracerProvider
}

// WithTracerProvider sets the provider the per-query client spans (otelpgx)
// come from. Without it otelpgx uses the global provider (otel.GetTracerProvider,
// installed by otelx.Init) — the OpenTelemetry convention for instrumentation.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tp = tp }
}
