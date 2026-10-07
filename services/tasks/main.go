// Command tasks is the "everything" HTTP service of this monorepo and its
// worked example of a durable write path: a CRUD API over PostgreSQL
// (libs/pgx, versioned migrations) with a Valkey cache-aside read path
// (libs/valkey, delete-safe invalidation), task.created / task.updated /
// task.deleted events delivered to Kafka through a transactional outbox
// (libs/kafka), distributed tracing (libs/otelx) and RFC 9457 problem+json
// errors — all on the shared libs/httpx server. services/consumer drains the
// events it produces.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/cache"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/config"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
	"github.com/tracehubmmp/golang-basics/services/tasks/migrations"
)

func main() {
	if err := run(); err != nil {
		// run() owns logging on the failure path; main only sets the exit code,
		// after every deferred cleanup in run() has executed.
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv("TASKS_CONFIG"))
	if err != nil {
		// No logger yet (its settings are part of what failed to load).
		fmt.Fprintln(os.Stderr, "tasks:", err)
		return err
	}

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

	vk, err := valkey.New(cfg.Valkey, logger)
	if err != nil {
		logger.Error("valkey init failed", "err", err)
		return err
	}
	defer vk.Close()

	producer, err := kafka.NewProducer(bootCtx, cfg.Kafka, logger)
	if err != nil {
		logger.Error("kafka init failed", "err", err)
		return err
	}
	defer producer.Close()

	srv, err := httpx.NewServer(cfg.Config, logger, httpx.WithConfig(cfg))
	if err != nil {
		logger.Error("invalid server config", "err", err)
		return err
	}
	st := store.New(db.Pool())
	relay := outbox.NewRelay(db.Pool(), producer, outbox.Config{
		Topic:        cfg.Kafka.Topic,
		PollInterval: cfg.Outbox.PollInterval,
		BatchSize:    cfg.Outbox.BatchSize,
		MaxBackoff:   cfg.Outbox.MaxBackoff,
	}, logger)

	// Postgres is the source of truth and holds the outbox: without it nothing
	// works → critical. The cache is bypassed when down, and the outbox keeps
	// events until Kafka is back, so those two only degrade the service; they
	// must not pull every replica out of the load balancer at once.
	srv.Health.Register("postgres", db.ReadyCheck())
	srv.Health.Register("valkey", vk.ReadyCheck(), httpx.Optional())
	srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
	srv.Metrics.Registry.MustRegister(db.Collectors()...)
	srv.Metrics.Registry.MustRegister(vk.Collectors()...)
	srv.Metrics.Registry.MustRegister(producer.Collectors()...)
	srv.Metrics.Registry.MustRegister(relay.Collectors()...)

	api.Register(srv, api.Deps{
		Store:     st,
		Cache:     cache.New(vk, cfg.Cache.TTL, cfg.Cache.TombstoneTTL, logger),
		Logger:    logger,
		Committed: relay.Wake,
	})

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("tasks starting", "addr", cfg.Addr, "admin_addr", cfg.AdminAddr,
		"topic", cfg.Kafka.Topic, "version", httpx.Version)
	if err := serve(ctx, srv, relay, st, cfg, logger); err != nil {
		logger.Error("tasks exited with error", "err", err)
		return err
	}
	return nil
}

// serve runs the HTTP server, the outbox relay and the idempotency-key
// janitor until ctx is cancelled or one of them fails. Shutdown order: the
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
