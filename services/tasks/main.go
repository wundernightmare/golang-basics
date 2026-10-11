// Command tasks is the "everything" HTTP service of this monorepo and its
// worked example of a durable write path: a CRUD API over PostgreSQL
// (libs/pgx, versioned migrations) with a Valkey cache-aside read path
// (libs/valkey, delete-safe invalidation), task.created / task.updated /
// task.deleted events delivered to Kafka through a transactional outbox
// (libs/kafka), distributed tracing (libs/otelx) and RFC 9457 problem+json
// errors — all on the shared libs/httpx server. services/consumer drains the
// events it produces.
//
// Valkey and Kafka are optional, on when their address is set (see
// internal/config): the service runs on Postgres alone, reads uncached and
// events kept in the outbox.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/cache"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/config"
	_ "github.com/tracehubmmp/golang-basics/services/tasks/internal/kafkaprovider" // the Kafka client, picked at build time
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
	"github.com/tracehubmmp/golang-basics/services/tasks/migrations"
)

func main() {
	boot := httpx.Boot[config.Config]{
		Name: "tasks", Prefix: config.EnvPrefix, File: "TASKS_CONFIG",
		Load:     func() (config.Config, error) { return config.Load(os.Getenv("TASKS_CONFIG")) },
		Defaults: config.Defaults,
	}
	cfg, code, ok := boot.Config(os.Args[1:], os.Stdout, os.Stderr)
	if !ok {
		os.Exit(code)
	}
	if err := run(cfg); err != nil {
		// run() owns logging on the failure path; main only sets the exit code,
		// after every deferred cleanup in run() has executed.
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	logger := httpx.NewLogger(cfg.LogConfig())

	// Tracing first, so spans from the dependency setup below are captured.
	shutdownTracing, err := otelx.Init(context.Background(), cfg.OTel, logger)
	if err != nil {
		logger.Error("tracing init failed", "err", err)
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBoot()

	// Dependencies — each fails fast at boot if unreachable. Deferred closes
	// run in reverse: producer, cache, then the database, after the server
	// and the relay below have stopped.
	db, err := pgx.New(bootCtx, cfg.Postgres, logger)
	if err != nil {
		logger.Error("postgres init failed", "err", err)
		return err
	}
	defer db.Close()
	if err := pgx.Migrate(bootCtx, db.Pool(), migrations.FS, logger); err != nil {
		logger.Error("migration failed", "err", err)
		return err
	}

	srv, err := httpx.NewServer(cfg.Config, logger, httpx.WithConfig(cfg))
	if err != nil {
		logger.Error("invalid server config", "err", err)
		return err
	}
	// Postgres is the source of truth and holds the outbox: without it nothing
	// works → critical. The cache is bypassed when down, and the outbox keeps
	// events until Kafka is back, so those two only degrade the service; they
	// must not pull every replica out of the load balancer at once.
	srv.Health.Register("postgres", db.ReadyCheck())
	srv.Metrics.Registry.MustRegister(db.Collectors()...)
	reportIntegrations(cfg, logger, srv.Metrics.Registry)

	var taskCache api.Cache = cache.None{}
	if cfg.ValkeyEnabled() {
		vk, err := valkey.New(cfg.Valkey, logger)
		if err != nil {
			logger.Error("valkey init failed", "err", err)
			return err
		}
		defer vk.Close()
		srv.Health.Register("valkey", vk.ReadyCheck(), httpx.Optional())
		srv.Metrics.Registry.MustRegister(vk.Collectors()...)
		taskCache = cache.New(vk, cfg.Cache.TTL, cfg.Cache.TombstoneTTL, logger)
	}

	// Without Kafka the relay publishes nothing — events wait in the outbox —
	// but still keeps outbox_pending / outbox_relay_lag_seconds current, so
	// the backlog of a deploy that lost its broker address is visible.
	var pub outbox.Publisher // nil interface: no Kafka
	committed := func() {}
	if cfg.KafkaEnabled() {
		producer, err := kafka.NewProducer(bootCtx, cfg.Kafka, logger)
		if err != nil {
			logger.Error("kafka init failed", "err", err)
			return err
		}
		defer producer.Close()
		srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
		srv.Metrics.Registry.MustRegister(producer.Collectors()...)
		pub = producer
	}
	relay := outbox.NewRelay(db.Pool(), pub, outbox.Config{
		Topic:         cfg.Kafka.Topic,
		PollInterval:  cfg.Outbox.PollInterval,
		BatchSize:     cfg.Outbox.BatchSize,
		MaxBackoff:    cfg.Outbox.MaxBackoff,
		MaxAttempts:   cfg.Outbox.MaxAttempts,
		RejectBackoff: cfg.Outbox.RejectBackoff,
	}, logger)
	srv.Metrics.Registry.MustRegister(relay.Collectors()...)
	if pub != nil {
		committed = relay.Wake
	}

	st := store.New(db.Pool())
	api.Register(srv, api.Deps{
		Store:     st,
		Cache:     taskCache,
		Logger:    logger,
		Committed: committed,
	})

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("tasks starting", "addr", cfg.Addr, "admin_addr", cfg.AdminAddr, "version", httpx.Version)
	if err := serve(ctx, srv, relay, st, cfg, logger); err != nil {
		logger.Error("tasks exited with error", "err", err)
		return err
	}
	return nil
}

// serve runs the HTTP server, the outbox relay (watching only, without Kafka) and the
// idempotency-key janitor until ctx is cancelled or one of them fails. Shutdown order: the
// server drains first (no new writes), then the relay and the janitor stop —
// the relay finishing its batch so what the broker acknowledged is not sent
// twice — and only then does run() close the producer and the pool.
func serve(ctx context.Context, srv *httpx.Server, relay *outbox.Relay, st *store.Store, cfg config.Config, log *slog.Logger) error {
	// A worker returning an error cancels gctx, which stops the server too.
	g, gctx := errgroup.WithContext(ctx)
	// The workers outlive the signal until the server has drained.
	workers, stopWorkers := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWorkers()

	g.Go(func() error {
		defer stopWorkers()
		return srv.Run(gctx)
	})
	g.Go(func() error { return relay.Run(workers) })
	g.Go(func() error {
		return st.RunJanitor(workers, cfg.Idempotency.TTL, cfg.Idempotency.PurgeInterval, log)
	})
	return g.Wait()
}

// reportIntegrations says which optional integrations this process runs with:
// one log line at boot and app_integration_enabled{integration} (1 on, 0 off)
// on /metrics, so a deploy that lost an address shows on a dashboard instead
// of as quietly uncached reads or a growing outbox.
func reportIntegrations(cfg config.Config, log *slog.Logger, reg prometheus.Registerer) {
	on := map[string]bool{"valkey": cfg.ValkeyEnabled(), "kafka": cfg.KafkaEnabled()}
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "app_integration_enabled",
		Help: "Optional integrations this process runs with (1 on, 0 off: its address is not set).",
	}, []string{"integration"})
	for name, enabled := range on {
		v := 0.0
		if enabled {
			v = 1
		}
		g.WithLabelValues(name).Set(v)
	}
	reg.MustRegister(g)
	log.Info("integrations resolved", "valkey", on["valkey"], "kafka", on["kafka"])
}
