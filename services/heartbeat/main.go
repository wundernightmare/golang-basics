// Command heartbeat is a minimal background worker built on libs/httpx. It
// runs a ticker loop (the "worker" shape of this monorepo) while also serving
// /healthz, /readyz and /metrics from the shared HTTP scaffolding, so it is
// observable like any other service. The HTTP server and the worker loop run
// concurrently under one signal-driven context; either failing tears down the
// other.
package main

import (
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/heartbeat/internal/config"
	"github.com/tracehubmmp/golang-basics/services/heartbeat/internal/worker"
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

	logger := httpx.NewLogger(cfg.LogLevel, cfg.LogFormat)
	srv := httpx.NewServer(cfg.HTTP(), logger)

	// The worker registers its counter on the server's registry, so beats
	// show up on /metrics alongside the HTTP metrics.
	w := worker.New(cfg.Interval, logger, srv.Metrics.Registry)

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("heartbeat starting", "addr", cfg.Addr, "interval", cfg.Interval)

	// Both the HTTP server and the worker return nil on graceful (ctx-driven)
	// shutdown; a non-nil result means one of them genuinely failed, which
	// cancels gctx and tears the other down too.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Run(gctx) })
	g.Go(func() error { return w.Run(gctx) })

	if err := g.Wait(); err != nil {
		logger.Error("heartbeat exited with error", "err", err)
		return err
	}
	logger.Info("heartbeat stopped cleanly")
	return nil
}
