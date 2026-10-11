// Command ping is a minimal ping/pong HTTP service built on the shared
// libs/httpx scaffolding. It demonstrates the "HTTP service" shape of this
// monorepo (the analogue of an edge crate in the Rust sibling repo): load
// config, build a server from the common lib, register a couple of routes,
// and serve with graceful shutdown.
package main

import (
	"os"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/ping/internal/api"
)

const prefix = "PING_"

func main() {
	boot := httpx.Boot[httpx.Config]{
		Name: "ping", Prefix: prefix,
		Load: func() (httpx.Config, error) {
			cfg, err := httpx.LoadConfig(prefix)
			cfg.Service = "ping"
			return cfg, err
		},
		Defaults: func() (httpx.Config, error) {
			cfg := httpx.Config{Service: "ping"}
			err := httpx.Defaults(prefix, &cfg)
			return cfg, err
		},
	}
	cfg, code, ok := boot.Config(os.Args[1:], os.Stdout, os.Stderr)
	if !ok {
		os.Exit(code)
	}
	if err := run(cfg); err != nil {
		// run() owns reporting on the failure path; main only sets the exit
		// code, after every deferred cleanup in run() has executed.
		os.Exit(1)
	}
}

func run(cfg httpx.Config) error {

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
