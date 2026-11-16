package otelx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/otelx/retro"
	"github.com/wundernightmare/golang-basics/libs/secret"
)

// Telemetry is what [Init] built. Pass TracerProvider / Propagator to the
// libraries explicitly (httpx.WithTracerProvider, kafka.WithTracerProvider,
// pgx / valkey / resilient WithTracerProvider, …): explicit wiring is what
// lets tests run in parallel with their own providers. Init also installs
// both globally, as the fallback for code that is not wired explicitly.
type Telemetry struct {
	// TracerProvider is the SDK provider, or a no-op one when tracing is disabled.
	TracerProvider trace.TracerProvider
	// Propagator is W3C trace context + baggage — always, even when disabled,
	// so an incoming trace context is still forwarded to the next hop.
	Propagator propagation.TextMapPropagator
	// Retro is the retroactive sampling buffer: nil when tracing is disabled
	// or the buffer is off (OTEL_RETRO_BUFFER_MIB < 0). A bus between
	// services would call its Trigger and OnLocalTrigger.
	Retro *retro.Processor

	// shutdown is nil when there is no provider to stop.
	shutdown func(ctx context.Context) error
}

// Shutdown exports what is buffered and stops the provider; a service calls
// it once, on exit, usually as [Telemetry.Stop]. It lasts until ctx's
// deadline, or five seconds when ctx has none, and does not stop when ctx is
// cancelled: the context of a service is cancelled by the signal that ends
// it, and a provider stopped under a cancelled context drops its spans for
// good. The error — also logged at warn — says that spans may have been lost:
// the time ran out, or the collector did not take the last export. A no-op
// when tracing is disabled, and on a nil or zero Telemetry.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.shutdown == nil {
		return nil
	}

	return t.shutdown(ctx)
}

// Collectors returns the Prometheus collectors of the tracing pipeline — the
// trace_retro_* families of the retroactive buffer — for the service's
// registry; none when there is no buffer.
func (t *Telemetry) Collectors() []prometheus.Collector {
	if t == nil || t.Retro == nil {
		return nil
	}

	return t.Retro.Collectors()
}

// shutdownBudget is how long the last spans have to leave when the caller of
// Shutdown sets no deadline: below the grace period an orchestrator gives a
// process by default, whatever the queue holds and the export timeout is.
const shutdownBudget = 5 * time.Second

// Stop is [Telemetry.Shutdown] for a defer, within its five seconds; the
// error is dropped, Shutdown has logged it.
//
//	tel, err := otelx.Init(ctx, cfg.OTel, logger)
//	if err != nil { … }
//	defer tel.Stop()
func (t *Telemetry) Stop() {
	_ = t.Shutdown(context.Background())
}

// flushContext is the context of the final flush: ctx without its
// cancellation, ending at ctx's deadline or, without one, after
// shutdownBudget.
func flushContext(ctx context.Context) (context.Context, context.CancelFunc) {
	flush := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(flush, deadline)
	}

	return context.WithTimeout(flush, shutdownBudget)
}

// Init builds the tracer provider and the W3C trace-context + baggage
// propagators, installs both globally (see [Telemetry]), and routes the
// SDK's own errors and diagnostics to log. A cfg that [Config.Validate]
// refuses is an error. With cfg.Enabled false it installs only the
// propagators: the global tracer provider is left as it was and the returned
// one is a no-op — the service still threads context across hops, just
// without exporting — so the same binary runs with or without a collector.
//
// When enabled it builds an OTLP/HTTP exporter to cfg.Endpoint (TLS unless
// cfg.Insecure; optional CA, mTLS and headers), describes the process in the
// resource (service name / version / instance, deployment environment, host,
// container, runtime, plus OTEL_RESOURCE_ATTRIBUTES), and applies
// parent-based ratio head sampling. The service ends with [Telemetry.Stop].
// Export failures — a collector that is down, a rejected batch —
// are logged at warn, at most once per errorLogInterval, and never affect the
// service.
func Init(ctx context.Context, cfg Config, log *slog.Logger) (*Telemetry, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	cfg.withDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	exportErrors := &errorHandler{log: log}

	// Propagators are always installed: even an unsampled/disabled service must
	// forward an incoming trace context to the next hop.
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	otel.SetTextMapPropagator(prop)
	otel.SetErrorHandler(exportErrors)
	otel.SetLogger(logr.FromSlogHandler(log.Handler()))

	if !cfg.Enabled {
		log.InfoContext(ctx, "tracing disabled (propagation only)")

		return &Telemetry{TracerProvider: noop.NewTracerProvider(), Propagator: prop}, nil
	}

	exporter, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, err
	}

	tp, buffer := newProvider(cfg, exporter, res)
	otel.SetTracerProvider(tp)

	bufferMiB := 0

	if buffer != nil {
		bufferMiB = buffer.Capacity() >> 20 //nolint:mnd // bytes to MiB

		// A warn or error line of an unsampled trace exports it; a logger
		// that is not httpx's has no such lines, and nothing is bound.
		httpx.BindTraceTrigger(log, buffer)
	}

	// "service" is the key httpx's logger puts on every line.
	log.InfoContext(ctx, "tracing enabled",
		"endpoint", cfg.Endpoint, "insecure", cfg.Insecure, "sampler_ratio", cfg.SamplerRatio,
		"redraw_remote_parent", cfg.RedrawRemoteParent, "service_name", cfg.ServiceName, "version", cfg.Version,
		"retro_buffer_mib", bufferMiB)

	shutdown := func(ctx context.Context) error {
		if buffer != nil {
			// A line written from here on cannot export its trace, and so
			// may not carry trace_id.
			httpx.BindTraceTrigger(log, nil)
		}

		flush, cancel := flushContext(ctx)
		defer cancel()

		// The SDK hands a failed export of the drain to the error handler,
		// not to the caller of Shutdown.
		failed := exportErrors.collect()
		err := tp.Shutdown(flush)
		exports, exportErr := failed()

		if err = errors.Join(err, exportErr); err != nil {
			log.WarnContext(ctx, "tracing shutdown: spans may have been lost",
				"err", err, "failed_exports", exports, "suppressed_since_last", exportErrors.suppressed.Swap(0))
		}

		return err
	}

	return &Telemetry{TracerProvider: tp, Propagator: prop, Retro: buffer, shutdown: shutdown}, nil
}

// newProvider assembles the SDK provider: the batch processor in front of
// exporter — behind the retroactive buffer when cfg asks for one — and the
// sampler that feeds it.
func newProvider(cfg Config, exporter sdktrace.SpanExporter, res *resource.Resource) (*sdktrace.TracerProvider, *retro.Processor) {
	processor := sdktrace.NewBatchSpanProcessor(exporter,
		sdktrace.WithBatchTimeout(cfg.BatchTimeout),
		sdktrace.WithMaxQueueSize(cfg.MaxQueueSize),
		sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
		sdktrace.WithExportTimeout(cfg.ExportTimeout),
	)

	var buffer *retro.Processor
	if cfg.Retro.Enabled() {
		buffer = retro.New(processor, cfg.Retro)
		processor = buffer
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(newSampler(cfg, buffer != nil)),
	), buffer
}

// newExporter builds the OTLP/HTTP trace exporter. HTTP rather than gRPC on
// purpose: the same wire protocol, one fewer dependency tree (grpc, its
// generated protos and gateway are the largest single contributor to a
// service binary otherwise), and it passes through any HTTP proxy.
//
// The exporter applies the unprefixed OTEL_EXPORTER_OTLP_* variables of the
// process before its options (otlptracehttp v1.47.0,
// internal/otlpconfig/envconfig.go: endpoint, insecure, certificates,
// headers, compression, protocol, timeout). Every setting it takes from
// there has an option here, so that cfg is the only source: a variable meant
// for another SDK in the pod neither redirects the export nor fails Init.
func newExporter(ctx context.Context, cfg Config) (*otlptrace.Exporter, error) {
	scheme, tlsCfg := "http", (*tls.Config)(nil)

	if !cfg.Insecure {
		var err error
		if tlsCfg, err = tlsConfig(cfg); err != nil {
			return nil, err
		}

		scheme = "https"
	}

	exporter, err := otlptracehttp.New(ctx,
		// Host, path and — by the scheme — plaintext or TLS in one option:
		// the exporter has no option that turns TLS back on.
		otlptracehttp.WithEndpointURL(scheme+"://"+cfg.Endpoint+"/v1/traces"),
		otlptracehttp.WithTLSClientConfig(tlsCfg),
		otlptracehttp.WithHeaders(cfg.Headers),
		otlptracehttp.WithCompression(otlptracehttp.NoCompression),
		otlptracehttp.WithEncoding(otlptracehttp.EncodingProtobuf),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("otelx: build otlp exporter: %w", err)
	}

	return exporter, nil
}

// tlsConfig builds the exporter's TLS config: the system roots, or cfg.CACert
// in their place when given (what the SDK's own OTEL_EXPORTER_OTLP_CERTIFICATE
// means), and a client certificate for mTLS when both halves are set.
func tlsConfig(cfg Config) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}

	if cfg.CACert != "" {
		pool, err := secret.CertPool(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("otelx: OTLP CA certificate: %w", err)
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

// errorHandler is the [otel.ErrorHandler] of the SDK: it logs at warn, at
// most once per errorLogInterval, with a count of what was suppressed since.
type errorHandler struct {
	log        *slog.Logger
	lastLogged atomic.Int64 // unix nanos
	suppressed atomic.Int64

	mu         sync.Mutex
	collecting bool
	first      error
	collected  int
}

// collect makes the handler keep the errors it is handed instead of logging
// them, until the returned function is called: that one gives how many
// there were and the first of them.
func (h *errorHandler) collect() (stop func() (count int, first error)) {
	h.mu.Lock()
	h.collecting, h.first, h.collected = true, nil, 0
	h.mu.Unlock()

	return func() (int, error) {
		h.mu.Lock()
		defer h.mu.Unlock()

		h.collecting = false

		return h.collected, h.first
	}
}

func (h *errorHandler) keep(err error) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.collecting {
		return false
	}

	if h.first == nil {
		h.first = err
	}

	h.collected++

	return true
}

func (h *errorHandler) Handle(err error) {
	if err == nil || h.keep(err) {
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
