package otelx_test

import (
	"context"
	"os"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/otelx"
)

// A service's main: tracing first, so that the clients built after it record
// spans, and stopped last, so that their final spans are exported.
//
//nolint:testableexamples // compiled only: run, it would install the OpenTelemetry globals of the test binary
func Example() {
	logger := httpx.NewLogger(httpx.LogConfig{Service: "tasks", Writer: os.Stdout})

	cfg, err := otelx.LoadConfig("TASKS_")
	if err != nil {
		logger.Error("tracing config", "err", err)
		return
	}

	cfg.ServiceName, cfg.Version = "tasks", httpx.Version

	tel, err := otelx.Init(context.Background(), cfg, logger)
	if err != nil {
		logger.Error("tracing init failed", "err", err)
		return
	}
	defer tel.Stop()

	srv, err := httpx.NewServer(httpx.Config{Service: "tasks"}, logger,
		httpx.WithTracerProvider(tel.TracerProvider), httpx.WithPropagator(tel.Propagator))
	if err != nil {
		logger.Error("server init failed", "err", err)
		return
	}

	_ = srv // srv.Run(ctx)
}
