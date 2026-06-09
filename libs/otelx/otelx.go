package otelx

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// ShutdownFunc flushes and tears down the tracer provider. Defer it in main so
// buffered spans are exported before the process exits; a nil-safe no-op is
// returned when tracing is disabled.
type ShutdownFunc func(ctx context.Context) error

// Init installs the global tracer provider and W3C trace-context + baggage
// propagators. With cfg.Enabled false it wires only the propagators and a no-op
// provider — the service still threads context across hops, just without
// exporting — so the same binary runs with or without a collector.
//
// When enabled it builds an OTLP/gRPC exporter to cfg.Endpoint, tags spans with
// the service resource, and applies parent-based ratio head sampling. The
// returned [ShutdownFunc] must be called on shutdown.
func Init(ctx context.Context, cfg Config, log *slog.Logger) (ShutdownFunc, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	// Propagators are always installed: even an unsampled/disabled service must
	// forward an incoming trace context to the next hop.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if !cfg.Enabled {
		log.Info("tracing disabled (propagation only)", "service", cfg.ServiceName)
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otelx: build otlp exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
	))
	if err != nil {
		return nil, fmt.Errorf("otelx: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplerRatio))),
	)
	otel.SetTracerProvider(tp)

	log.Info("tracing enabled",
		"service", cfg.ServiceName, "endpoint", cfg.Endpoint, "sampler_ratio", cfg.SamplerRatio)
	return tp.Shutdown, nil
}

// GinMiddleware returns the OpenTelemetry gin middleware: it starts a server
// span per request (named by the matched route), records status and method, and
// continues any incoming trace context. Attach it to the server engine before
// registering routes.
func GinMiddleware(service string) gin.HandlerFunc {
	return otelgin.Middleware(service)
}
