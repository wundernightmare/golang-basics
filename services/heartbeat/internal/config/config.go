// Package config loads the heartbeat worker's settings from the environment.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// Config is the heartbeat worker configuration. It carries the shared
// admin-listener and logging fields (so the health/metrics/pprof server can be
// built from libs/httpx) plus the worker's own tick interval. A worker has no
// API listener, only the admin one.
//
// Keys (all prefixed HEARTBEAT_):
//
//	HEARTBEAT_ADMIN_ADDR             health/metrics/pprof listen address (default ":9081")
//	HEARTBEAT_HTTP_SHUTDOWN_TIMEOUT  graceful-shutdown budget            (default "10s")
//	HEARTBEAT_LOG_LEVEL              debug|info|warn|error               (default "info")
//	HEARTBEAT_LOG_FORMAT             json|text                           (default "json")
//	HEARTBEAT_LOG_SAMPLE_INITIAL     log sampling: first N per msg per second (default 100, 0 = off)
//	HEARTBEAT_LOG_SAMPLE_THEREAFTER  … then every M-th                   (default 100)
//	HEARTBEAT_INTERVAL               tick period                         (default "5s")
type Config struct {
	AdminAddr           string        `env:"ADMIN_ADDR" envDefault:":9081"`
	ShutdownTimeout     time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel            string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat           string        `env:"LOG_FORMAT" envDefault:"json"`
	LogSampleInitial    int           `env:"LOG_SAMPLE_INITIAL" envDefault:"100"`
	LogSampleThereafter int           `env:"LOG_SAMPLE_THEREAFTER" envDefault:"100"`
	Interval            time.Duration `env:"INTERVAL" envDefault:"5s"`
}

// Load parses the configuration from HEARTBEAT_-prefixed environment vars.
func Load() (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: "HEARTBEAT_"}); err != nil {
		return Config{}, fmt.Errorf("heartbeat: parse config: %w", err)
	}
	return cfg, nil
}

// HTTP projects the shared fields into a libs/httpx Config: admin listener
// only (Addr empty), since a worker serves no API.
func (c Config) HTTP() httpx.Config {
	return httpx.Config{
		Service:             "heartbeat",
		Addr:                "",
		AdminAddr:           c.AdminAddr,
		ShutdownTimeout:     c.ShutdownTimeout,
		LogLevel:            c.LogLevel,
		LogFormat:           c.LogFormat,
		LogSampleInitial:    c.LogSampleInitial,
		LogSampleThereafter: c.LogSampleThereafter,
	}
}
