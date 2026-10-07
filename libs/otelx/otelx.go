package otelx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// ShutdownFunc flushes and tears down the tracer provider. Defer it in main so
// buffered spans are exported before the process exits; a nil-safe no-op is
// returned when tracing is disabled.
type ShutdownFunc func(ctx context.Context) error

// Init installs the global tracer provider, the W3C trace-context + baggage
// propagators, and routes the SDK's own errors and diagnostics to log. With
// cfg.Enabled false it wires only the propagators and leaves the no-op
// provider — the service still threads context across hops, just without
// exporting — so the same binary runs with or without a collector.
//
// When enabled it builds an OTLP/HTTP exporter to cfg.Endpoint (TLS unless
// cfg.Insecure; optional CA, mTLS and headers), describes the process in the
// resource (service name / version / instance, deployment environment, host,
// container, runtime, plus OTEL_RESOURCE_ATTRIBUTES), and applies
// parent-based ratio head sampling. The returned [ShutdownFunc] must be called
// on shutdown. Export failures — a collector that is down, a rejected batch —
// are logged at warn, at most once per errorLogInterval, and never affect the
// service.
func Init(ctx context.Context, cfg Config, log *slog.Logger) (ShutdownFunc, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cfg.withDefaults()

	// Propagators are always installed: even an unsampled/disabled service must
	// forward an incoming trace context to the next hop.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	otel.SetErrorHandler(newErrorHandler(log))
	otel.SetLogger(logr.FromSlogHandler(log.Handler()))

	if !cfg.Enabled {
		log.Info("tracing disabled (propagation only)")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(cfg.BatchTimeout),
			sdktrace.WithMaxQueueSize(cfg.MaxQueueSize),
			sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
			sdktrace.WithExportTimeout(cfg.ExportTimeout),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(newSampler(cfg)),
	)
	otel.SetTracerProvider(tp)

	log.Info("tracing enabled",
		"endpoint", cfg.Endpoint, "insecure", cfg.Insecure, "sampler_ratio", cfg.SamplerRatio,
		"trust_remote_parent", cfg.TrustRemoteParent, "service", cfg.ServiceName, "version", cfg.Version)
	return func(ctx context.Context) error {
		if err := tp.Shutdown(ctx); err != nil {
			log.Warn("tracing shutdown: spans may have been lost", "err", err)
			return err
		}
		return nil
	}, nil
}

// newSampler is parent-based ratio head sampling: a root span is sampled
// with probability cfg.SamplerRatio (never when it is negative), a child
// follows its parent. With TrustRemoteParent off, a remote parent that says
// "sampled" is treated like a root — the local ratio decides — so a client
// cannot force 100% sampling on an internet-facing edge.
func newSampler(cfg Config) sdktrace.Sampler {
	var root sdktrace.Sampler
	switch {
	case cfg.SamplerRatio < 0:
		root = sdktrace.NeverSample()
	default:
		root = sdktrace.TraceIDRatioBased(cfg.SamplerRatio)
	}
	var opts []sdktrace.ParentBasedSamplerOption
	if !cfg.TrustRemoteParent {
		opts = append(opts, sdktrace.WithRemoteParentSampled(root))
	}
	return sdktrace.ParentBased(root, opts...)
}

// newExporter builds the OTLP/HTTP trace exporter. HTTP rather than gRPC on
// purpose: the same wire protocol, one fewer dependency tree (grpc, its
// generated protos and gateway are the largest single contributor to a
// service binary otherwise), and it passes through any HTTP proxy.
func newExporter(ctx context.Context, cfg Config) (*otlptrace.Exporter, error) {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.Endpoint),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
	}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	} else {
		tlsCfg, err := tlsConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, otlptracehttp.WithTLSClientConfig(tlsCfg))
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otelx: build otlp exporter: %w", err)
	}
	return exporter, nil
}

// tlsConfig builds the exporter's TLS config: the system roots plus cfg.CACert
// when given, and a client certificate for mTLS when both halves are set.
func tlsConfig(cfg Config) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("otelx: read OTLP CA certificate: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("otelx: OTLP CA certificate: no PEM certificates found")
		}
		tc.RootCAs = pool
	}
	switch {
	case cfg.ClientCert != "" && cfg.ClientKey != "":
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("otelx: load OTLP client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	case cfg.ClientCert != "" || cfg.ClientKey != "":
		return nil, errors.New("otelx: OTLP client certificate and key must both be set")
	}
	return tc, nil
}

// newResource describes the process: the identity a trace backend groups and
// filters on. service.instance.id defaults to the hostname (the pod name in
// Kubernetes) so replicas are told apart; OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME from the environment are merged in the standard way.
func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	// An empty name or version is left out rather than set to "": it would
	// override OTEL_SERVICE_NAME and the SDK's unknown_service:<exe> default.
	attrs := []attribute.KeyValue{semconv.ServiceInstanceID(instanceID())}
	if cfg.ServiceName != "" {
		attrs = append(attrs, semconv.ServiceName(cfg.ServiceName))
	}
	if cfg.Version != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.Version))
	}
	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentNameKey.String(cfg.Environment))
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithContainerID(),
		resource.WithProcessPID(),
		resource.WithProcessExecutableName(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithAttributes(attrs...),
	)
	if err != nil {
		// A partial resource (a detector that could not run, a schema URL
		// conflict) is still a resource; only a nil one is fatal.
		if res == nil {
			return nil, fmt.Errorf("otelx: build resource: %w", err)
		}
		otel.Handle(err)
	}
	merged, err := resource.Merge(resource.Default(), res)
	if err != nil {
		return res, nil //nolint:nilerr // the detected resource is complete without the SDK defaults
	}
	return merged, nil
}

func instanceID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return fmt.Sprintf("pid-%d", os.Getpid())
}

// errorLogInterval throttles the SDK error log: a collector outage otherwise
// produces one error per failed batch, every few seconds, per replica.
const errorLogInterval = 30 * time.Second

// newErrorHandler returns an [otel.ErrorHandler] that logs at warn, at most
// once per errorLogInterval, with a count of what was suppressed since.
func newErrorHandler(log *slog.Logger) otel.ErrorHandler {
	h := &errorHandler{log: log}
	return h
}

type errorHandler struct {
	log        *slog.Logger
	lastLogged atomic.Int64 // unix nanos
	suppressed atomic.Int64
}

func (h *errorHandler) Handle(err error) {
	if err == nil {
		return
	}
	now := time.Now().UnixNano()
	last := h.lastLogged.Load()
	if now-last < int64(errorLogInterval) || !h.lastLogged.CompareAndSwap(last, now) {
		h.suppressed.Add(1)
		return
	}
	h.log.Warn("opentelemetry: export error", "err", err, "suppressed_since_last", h.suppressed.Swap(0))
}
