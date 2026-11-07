package outbox

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"

// Option configures [NewRelay].
type Option func(*options)

type options struct {
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
}

// WithTracerProvider sets the provider the relay spans come from. Without it
// the global provider (installed by otelx.Init) is used.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tp = tp }
}

// WithPropagator sets the propagator that restores a row's stored trace
// context. It must match the one [Enqueue] wrote with. Without it the global
// one is used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.prop = p }
}

func resolve(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if o.tp == nil {
		o.tp = otel.GetTracerProvider()
	}

	if o.prop == nil {
		o.prop = otel.GetTextMapPropagator()
	}

	return o
}
