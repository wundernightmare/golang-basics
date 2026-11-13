package s3

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName names this library's tracer.
const instrumentationName = "github.com/tracehubmmp/golang-basics/libs/s3"

// Option configures [New].
type Option func(*options)

type options struct {
	tp trace.TracerProvider
}

// WithTracerProvider sets the provider the per-call client spans come from.
// Without it the global provider (otel.GetTracerProvider, installed by
// otelx.Init) is used — the OpenTelemetry convention for instrumentation.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tp = tp }
}

func newTracer(opts []Option) trace.Tracer {
	var applied options
	for _, opt := range opts {
		opt(&applied)
	}

	if applied.tp == nil {
		applied.tp = otel.GetTracerProvider() // a delegating proxy: picks up a later otelx.Init too
	}

	return applied.tp.Tracer(instrumentationName)
}
