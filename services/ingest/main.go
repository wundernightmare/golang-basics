// Command ingest accepts batches of events over HTTP and makes each durable
// in at least one of two sinks before it answers: Kafka (libs/kafka) and an
// S3 archive (libs/s3). It keeps accepting while either works, holds no
// state on disk, and can therefore be killed at any moment without losing
// an event it acknowledged (internal/pipeline has the argument).
//
// One binary, two roles (INGEST_MODE):
//
//	ingest   POST /events — the stateless pods behind the ingress
//	compact  merges closed hours of the archive into large objects — one
//	         replica, admin listener only
package main

import (
	"os"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/app"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/config"
	_ "github.com/tracehubmmp/golang-basics/services/ingest/internal/kafkaprovider" // the Kafka client, picked at build time
)

func main() {
	boot := httpx.Boot[config.Config]{
		Name: "ingest", Prefix: config.EnvPrefix, File: "INGEST_CONFIG",
		Load:     func() (config.Config, error) { return config.Load(os.Getenv("INGEST_CONFIG")) },
		Defaults: config.Defaults,
	}

	cfg, code, ok := boot.Config(os.Args[1:], os.Stdout, os.Stderr)
	if !ok {
		os.Exit(code)
	}

	if err := run(cfg); err != nil {
		// app.Run owns logging on the failure path; main only sets the exit
		// code, after every deferred cleanup has executed.
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	ctx, stop := httpx.SignalContext()
	defer stop()

	return app.Run(ctx, cfg, httpx.NewLogger(cfg.LogConfig()), nil)
}
