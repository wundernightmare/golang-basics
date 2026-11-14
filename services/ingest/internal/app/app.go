// Package app assembles the ingest service from its parts — the server, the
// sinks, the pipeline, the compactor — and runs it until its context ends.
// It is main.go minus the process concerns (flags, signals, exit codes), so
// the integration tests start exactly what the binary starts.
package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/api"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/compact"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/config"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/pipeline"
)

// Run builds the service for cfg.Mode and serves until ctx is cancelled (the
// signal context in main), then shuts down in order. onServer, when set, is
// called with the server before it starts listening — tests take the bound
// addresses and the metrics registry from it. Failures are logged here; the
// caller only needs the error for its exit code.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger, onServer func(*httpx.Server)) error {
	tel, err := otelx.Init(context.Background(), cfg.OTel, logger)
	if err != nil {
		logger.ErrorContext(ctx, "tracing init failed", "err", err)
		return err
	}

	defer tel.Stop()

	srv, err := httpx.NewServer(cfg.Config, logger, httpx.WithConfig(cfg),
		httpx.WithTracerProvider(tel.TracerProvider), httpx.WithPropagator(tel.Propagator))
	if err != nil {
		logger.ErrorContext(ctx, "invalid server config", "err", err)
		return err
	}

	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBoot()

	// The client is built without touching the network: a store that is down
	// must not stop a pod from starting while Kafka can still take events.
	store, err := s3.New(bootCtx, cfg.S3, logger, s3.WithTracerProvider(tel.TracerProvider))
	if err != nil {
		logger.ErrorContext(ctx, "s3 init failed", "err", err)
		return err
	}
	defer store.Close()

	srv.Metrics.Registry.MustRegister(store.Collectors()...)
	// Crash reports and memory-pressure profiles go to the same bucket,
	// under diag_key_prefix (libs/httpx diagnostics.go).
	srv.SetDumpSink(store)

	if onServer != nil {
		onServer(srv)
	}

	logger.InfoContext(ctx, "ingest starting", "mode", cfg.Mode, "addr", cfg.Addr, "admin_addr", cfg.AdminAddr, "version", httpx.Version, "fips140", httpx.FIPS140())

	if cfg.Mode == config.ModeCompact {
		err = runCompact(ctx, cfg, srv, store, logger)
	} else {
		err = runIngest(ctx, bootCtx, cfg, srv, store, tel, logger)
	}

	if err != nil {
		logger.ErrorContext(ctx, "ingest exited with error", "mode", cfg.Mode, "err", err)
	}

	return err
}

// runIngest wires and serves the request path.
func runIngest(ctx, bootCtx context.Context, cfg config.Config, srv *httpx.Server, store *s3.Client, tel *otelx.Telemetry, logger *slog.Logger) error {
	registry := srv.Metrics.Registry
	breakers := newBreakerMetrics(registry)

	// The bucket is checked, not required: without it the archive's circuit
	// opens on the first batches and Kafka carries the traffic.
	if err := store.Ensure(bootCtx); err != nil {
		logger.WarnContext(ctx, "s3 bucket not reachable at boot: starting on kafka alone until it is", "err", err)
	}

	srv.Health.Register("s3", store.ReadyCheck(), httpx.Optional())

	// The breaker policies were validated with the configuration.
	archiveBreaker := breakers.breaker(cfg.Ingest.ArchiveBreaker.Resilient(), sinkArchive, logger)

	writerID, err := archive.NewWriterID()
	if err != nil {
		logger.ErrorContext(ctx, "writer id", "err", err)
		return err
	}

	writer, err := archive.NewWriter(archive.WriterConfig{
		BatchBytes:           cfg.Archive.BatchBytes,
		Linger:               cfg.Archive.Linger,
		UrgentLinger:         cfg.Archive.UrgentLinger,
		MaxConcurrentFlushes: cfg.Archive.MaxConcurrentFlushes,
		Compression:          cfg.Archive.Compression,
	}, store, archive.NewLayout(cfg.Archive.Prefix), writerID, archiveBreaker, logger)
	if err != nil {
		logger.ErrorContext(ctx, "archive writer init failed", "err", err)
		return err
	}

	registry.MustRegister(writer.Collectors()...)

	var (
		publisher    pipeline.Publisher // nil interface: no Kafka
		kafkaBreaker *resilience.Breaker
	)

	reportIntegrations(cfg, logger, registry)

	if cfg.KafkaEnabled() {
		// Lazy: a rollout in the middle of a Kafka outage must produce pods
		// that start and serve from the archive.
		producer, err := kafka.NewProducer(bootCtx, cfg.Kafka, logger, kafka.WithLazyConnect(),
			kafka.WithTracerProvider(tel.TracerProvider), kafka.WithPropagator(tel.Propagator))
		if err != nil {
			logger.ErrorContext(ctx, "kafka init failed", "err", err)
			return err
		}
		// Closed last (deferred first): after the pipeline has drained.
		defer producer.Close()

		kafkaBreaker = breakers.breaker(cfg.Ingest.KafkaBreaker.Resilient(), sinkKafka, logger)

		srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
		registry.MustRegister(producer.Collectors()...)

		publisher = producer
	}

	pipe, err := pipeline.New(pipeline.Config{
		Topic:            cfg.Kafka.Topic,
		MaxInFlightBytes: cfg.Ingest.MaxInFlightBytes,
		KafkaAckGrace:    cfg.Ingest.KafkaAckGrace,
		KafkaMaxInFlight: cfg.Ingest.KafkaMaxInFlight,
	}, publisher, writer, kafkaBreaker, logger)
	if err != nil {
		logger.ErrorContext(ctx, "pipeline init failed", "err", err)
		return err
	}

	registry.MustRegister(pipe.Collectors()...)
	// The one critical check: no sink at all. Either sink alone keeps the
	// pod in the load balancer.
	srv.Health.Register("sinks", pipe.ReadyCheck(func() bool { return archiveBreaker.State() == resilience.StateOpen }))

	api.Register(srv, api.Deps{
		Ingester:       pipe,
		Logger:         logger,
		MaxEventBytes:  cfg.Ingest.MaxEventBytes,
		MaxBatchEvents: cfg.Ingest.MaxBatchEvents,
		RetryAfter:     cfg.Ingest.RetryAfter,
	})

	return serveIngest(ctx, srv, writer, pipe, cfg.Ingest.DrainTimeout, logger)
}

// serveIngest runs the server and the archive writer until ctx is cancelled.
// Shutdown order: the server drains first — every request in flight gets its
// answer from a sink that is still running, or a 503 — and only then the
// writer flushes its last batch and the Kafka sends finish. What remains
// after that is the second copy of events a sink already holds, never an
// acknowledged event without a home; it is bounded by drainTimeout and
// reported.
func serveIngest(ctx context.Context, srv *httpx.Server, writer *archive.Writer, pipe *pipeline.Pipeline, drainTimeout time.Duration, log *slog.Logger) error {
	// The writer outlives the signal until the server has drained, and a
	// panic in it stops the service in order (httpx.Server.RunWith). When
	// this returns the writer has returned too: its last batch is flushed.
	err := srv.RunWith(ctx, httpx.Worker{Name: "archive-writer", Run: writer.Run})

	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	if unsettled := pipe.Drain(drainCtx); unsettled > 0 {
		log.WarnContext(drainCtx, "shutdown: second copies still in flight after DRAIN_TIMEOUT — these events are in one sink only",
			"unsettled_bytes", unsettled, "drain_timeout", drainTimeout.String())
	} else {
		log.InfoContext(drainCtx, "shutdown: both sinks drained")
	}

	return err
}

// runCompact serves the admin listener and runs the compactor.
func runCompact(ctx context.Context, cfg config.Config, srv *httpx.Server, store *s3.Client, logger *slog.Logger) error {
	// Compaction has nothing else to do without the store: critical.
	srv.Health.Register("s3", store.ReadyCheck())

	compactor, err := compact.New(compact.Config{
		Interval:         cfg.Compact.Interval,
		RunOnce:          cfg.Compact.RunOnce,
		CloseDelay:       cfg.Compact.CloseDelay,
		PartBytes:        cfg.Compact.PartBytes,
		FetchConcurrency: cfg.Compact.FetchConcurrency,
		MaxObjectBytes:   cfg.Compact.MaxObjectBytes,
		Compression:      cfg.Compact.Compression,
		DeleteSources:    cfg.Compact.DeleteSources,
		HourTimeout:      cfg.Compact.HourTimeout,
	}, store, archive.NewLayout(cfg.Archive.Prefix), logger)
	if err != nil {
		logger.ErrorContext(ctx, "compactor init failed", "err", err)
		return err
	}

	srv.Metrics.Registry.MustRegister(compactor.Collectors()...)

	g, gctx := errgroup.WithContext(ctx)
	runCtx, stop := context.WithCancel(gctx)

	g.Go(func() error { return srv.Run(runCtx) })
	g.Go(func() error {
		// With RUN_ONCE the pass ending ends the process; otherwise Run
		// returns only on the signal, and stopping the server is a no-op.
		defer stop()
		// Guarded: a panic in a pass is an error — the server stops in
		// order and the process exits non-zero — not a crash.
		return srv.Guard("compactor", gctx, compactor.Run)()
	})

	return g.Wait()
}

// Sink names: the `target` label of the breaker metrics.
const (
	sinkKafka   = "kafka"
	sinkArchive = "archive"
)

type breakerMetrics struct {
	state       *prometheus.GaugeVec
	transitions *prometheus.CounterVec
}

func newBreakerMetrics(reg prometheus.Registerer) *breakerMetrics {
	m := &breakerMetrics{
		state: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ingest_sink_circuit_breaker_state",
			Help: "Circuit breaker of a sink (kafka, archive): 0 closed, 1 open, 2 half-open.",
		}, []string{"target"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_sink_circuit_breaker_transitions_total",
			Help: "Circuit breaker state changes of a sink, by the state entered.",
		}, []string{"target", "state"}),
	}
	reg.MustRegister(m.state, m.transitions)

	return m
}

// breaker builds a sink's circuit breaker with its transition hook: the
// gauge, the counter and a log line that is never sampled away — a sink
// going down or coming back is the event an operator reads the log for.
func (m *breakerMetrics) breaker(cfg resilience.BreakerConfig, sink string, log *slog.Logger) *resilience.Breaker {
	var (
		mu      sync.Mutex
		breaker *resilience.Breaker
	)

	m.state.WithLabelValues(sink).Set(float64(resilience.StateClosed))

	breaker = resilience.NewBreaker(cfg, time.Now, func(from, to resilience.State) {
		m.transitions.WithLabelValues(sink, to.String()).Inc()
		// The current state, not `to`, and read under the lock: the
		// breaker notifies concurrently and possibly out of order, but
		// whichever notification sets the gauge last has read a state at
		// least as new as every earlier writer's.
		mu.Lock()
		m.state.WithLabelValues(sink).Set(float64(breaker.State()))
		mu.Unlock()
		log.Warn("sink circuit breaker changed state", "sink", sink, "from", from.String(), "to", to.String())
	})

	return breaker
}

// reportIntegrations says whether this process publishes to Kafka: a log
// line at boot and app_integration_enabled{integration="kafka"}.
func reportIntegrations(cfg config.Config, log *slog.Logger, reg prometheus.Registerer) {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "app_integration_enabled",
		Help: "Optional integrations this process runs with (1 on, 0 off: its address is not set).",
	}, []string{"integration"})

	value := 0.0
	if cfg.KafkaEnabled() {
		value = 1
	}

	g.WithLabelValues("kafka").Set(value)
	reg.MustRegister(g)
	log.Info("integrations resolved", "kafka", cfg.KafkaEnabled())
}
