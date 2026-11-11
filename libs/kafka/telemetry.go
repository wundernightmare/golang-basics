package kafka

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName names this library's tracer.
const instrumentationName = "github.com/tracehubmmp/golang-basics/libs/kafka"

// Option configures a [Producer] or a [Consumer].
type Option func(*options)

type options struct {
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
	lazy bool
}

// WithLazyConnect makes [NewProducer] skip the boot-time broker check: the
// producer is returned whether or not a broker answers, and connects on the
// first send. For a service that has another place to put its data and must
// start — and restart, mid-outage — while the brokers are away; its sends fail
// with [KindUnavailable] until they are back. Everything else keeps the
// default: a broker address or credential that is wrong should stop the boot.
func WithLazyConnect() Option {
	return func(o *options) { o.lazy = true }
}

// WithTracerProvider sets the provider the publish / process spans come
// from. Without it the global provider (otel.GetTracerProvider, installed by
// otelx.Init) is used — the OpenTelemetry convention for instrumentation.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tp = tp }
}

// WithPropagator sets the propagator that writes / reads the trace context in
// record headers. Without it the global one (otel.GetTextMapPropagator) is used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.prop = p }
}

// telemetry is what a client traces with, resolved once at construction.
type telemetry struct {
	tracer trace.Tracer
	prop   propagation.TextMapPropagator
}

func applyOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

func newTelemetry(opts []Option) telemetry {
	o := applyOptions(opts)

	if o.tp == nil {
		o.tp = otel.GetTracerProvider() // a delegating proxy: picks up a later otelx.Init too
	}

	if o.prop == nil {
		o.prop = otel.GetTextMapPropagator()
	}

	return telemetry{tracer: o.tp.Tracer(instrumentationName), prop: o.prop}
}
