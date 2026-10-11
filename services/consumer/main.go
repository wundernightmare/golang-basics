// Command consumer is a background worker that drains the tasks.events Kafka
// topic produced by services/tasks. It runs the consumer loop and the shared
// libs/httpx admin server (health/metrics/pprof) concurrently under one signal-driven
// context (the services/heartbeat pattern), so it is observable like any other
// service and either half failing tears the other down.
package main

import (
	"context"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/config"
	_ "github.com/tracehubmmp/golang-basics/services/consumer/internal/kafkaprovider" // the Kafka client, picked at build time
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

func main() {
	// CONSUMER_CONFIG names an optional YAML file; the environment overrides it.
	boot := httpx.Boot[config.Config]{
		Name: "consumer", Prefix: config.Prefix, File: config.Prefix + "CONFIG",
		Load:     func() (config.Config, error) { return config.Load(os.Getenv(config.Prefix + "CONFIG")) },
		Defaults: config.Defaults,
	}
	cfg, code, ok := boot.Config(os.Args[1:], os.Stdout, os.Stderr)
	if !ok {
		os.Exit(code)
	}
	if err := run(cfg); err != nil {
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	logger := httpx.NewLogger(cfg.LogConfig())

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

	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelBoot()

	consumer, err := kafka.NewConsumer(bootCtx, cfg.Kafka, logger)
	if err != nil {
		logger.Error("kafka consumer init failed", "err", err)
		return err
	}
	defer consumer.Close() // Run closes it too; this covers an early return

	srv, err := httpx.NewServer(cfg.Config, logger, httpx.WithConfig(cfg))
	if err != nil {
		logger.Error("admin server init failed", "err", err)
		return err
	}
	srv.Health.Register("kafka", consumer.ReadyCheck())
	// Consume outcomes / retries / DLQ / lag from the lib, the worker's own
	// counters next to them — one /metrics on the admin listener has it all.
	srv.Metrics.Registry.MustRegister(consumer.Collectors()...)
	w := worker.New(consumer, logger, srv.Metrics.Registry, worker.Options{DedupeSize: cfg.DedupeSize})

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("consumer starting", "admin_addr", cfg.AdminAddr, "topics", cfg.Kafka.Topics, "group", cfg.Kafka.Group, "version", httpx.Version)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Run(gctx) })
	g.Go(func() error { return w.Run(gctx) })

	if err := g.Wait(); err != nil {
		logger.Error("consumer exited with error", "err", err)
		return err
	}
	logger.Info("consumer stopped cleanly")
	return nil
}
