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
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := httpx.NewLogger(cfg.HTTP().LogConfig())

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

	consumer, err := kafka.NewConsumer(bootCtx, cfg.Kafka(), logger)
	if err != nil {
		logger.Error("kafka consumer init failed", "err", err)
		return err
	}
	defer consumer.Close()

	srv := httpx.NewServer(cfg.HTTP(), logger, httpx.WithConfig(cfg))
	srv.Health.Register("kafka", consumer.ReadyCheck())
	// Records / handler latency / group lag from the lib, the worker's own
	// counters next to them — one /metrics on the admin listener has it all.
	srv.Metrics.Registry.MustRegister(consumer.Collectors()...)
	w := worker.New(consumer, logger, srv.Metrics.Registry)

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("consumer starting", "admin_addr", cfg.AdminAddr, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup, "version", httpx.Version)

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
