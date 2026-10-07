// Command ping is a minimal ping/pong HTTP service built on the shared
// libs/httpx scaffolding. It demonstrates the "HTTP service" shape of this
// monorepo (the analogue of an edge crate in the Rust sibling repo): load
// config, build a server from the common lib, register a couple of routes,
// and serve with graceful shutdown.
package main

import (
	"fmt"
	"os"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/ping/internal/api"
)

func main() {
	if err := run(); err != nil {
		// run() owns reporting on the failure path; main only sets the exit
		// code, after every deferred cleanup in run() has executed.
		os.Exit(1)
	}
}

func run() error {
	cfg, err := httpx.LoadConfig("PING_")
	if err != nil {
		// There is no logger before the config is known: say it on stderr,
		// or the process exits 1 without a word.
		fmt.Fprintln(os.Stderr, "ping: "+err.Error())
		return err
	}
	cfg.Service = "ping"

	logger := httpx.NewLogger(cfg.LogConfig())
	srv, err := httpx.NewServer(cfg, logger)
	if err != nil {
		logger.Error("ping: invalid configuration", "err", err)
		return err
	}

	api.Register(srv)

	ctx, stop := httpx.SignalContext()
	defer stop()

	logger.Info("ping starting", "addr", cfg.Addr, "admin_addr", cfg.AdminAddr, "version", httpx.Version)
	if err := srv.Run(ctx); err != nil {
		logger.Error("ping exited with error", "err", err)
		return err
	}
	return nil
}
