// Command tasks is the "everything" HTTP service of this monorepo: a small CRUD
// API over PostgreSQL (libs/pgx) with a Valkey cache-aside read path
// (libs/valkey), a task.created event published to Kafka on write (libs/kafka),
// distributed tracing (libs/otelx) and RFC 9457 problem+json errors — all on
// the shared libs/httpx server scaffolding. services/consumer drains the events
// it produces.
package main

import (
	"context"
	"os"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/config"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv("TASKS_CONFIG"))
	if err != nil {
		return err
	}

	logger := httpx.NewLogger(cfg.HTTP().LogConfig())

	// Tracing first, so spans from the dependency setup below are captured.
	shutdownTracing, err := otelx.Init(context.Background(), cfg.OTel(), logger)
	if err != nil {
		logger.Error("tracing init failed", "err", err)
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelBoot()

	// Dependencies — each fails fast at boot if unreachable.
	db, err := pgx.New(bootCtx, cfg.Postgres(), logger)
	if err != nil {
		logger.Error("postgres init failed", "err", err)
		return err
	}
	defer db.Close()

	st := store.New(db)
	if err := st.Migrate(bootCtx); err != nil {
		logger.Error("migration failed", "err", err)
		return err
	}

	cache, err := valkey.New(cfg.Valkey(), logger)
	if err != nil {
		logger.Error("valkey init failed", "err", err)
		return err
	}
	defer cache.Close()

	producer, err := kafka.NewProducer(bootCtx, cfg.Kafka(), logger)
	if err != nil {
		logger.Error("kafka init failed", "err", err)
		return err
	}
	defer producer.Close()

	// HTTP server + tracing middleware + routes. The data libs each expose
	// their collectors (pool stats, cache hit/miss, publish latency); registering
	// them here puts everything on one /metrics on the admin listener.
	srv := httpx.NewServer(cfg.HTTP(), logger,
		httpx.WithMiddleware(otelx.GinMiddleware(cfg.OTel().ServiceName)))
	// Postgres is the source of truth: without it nothing works → critical.
	// The cache is bypassed on a miss and events are published best-effort,
	// so those two only degrade the service; they must not pull every replica
	// out of the load balancer at once.
	srv.Health.Register("postgres", db.ReadyCheck())
	srv.Health.Register("valkey", cache.ReadyCheck(), httpx.Optional())
	srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
	srv.Metrics.Registry.MustRegister(db.Collectors()...)
	srv.Metrics.Registry.MustRegister(cache.Collectors()...)
	srv.Metrics.Registry.MustRegister(producer.Collectors()...)

	api.Register(srv, api.Deps{
		Store:     st,
		Cache:     cache,
		Publisher: producer,
		Topic:     cfg.KafkaTopic,
		CacheTTL:  cfg.CacheTTL,
		Logger:    logger,
	})

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("tasks starting", "addr", cfg.HTTPAddr, "admin_addr", cfg.AdminAddr, "topic", cfg.KafkaTopic, "version", httpx.Version)
	if err := srv.Run(ctx); err != nil {
		logger.Error("tasks exited with error", "err", err)
		return err
	}
	return nil
}
