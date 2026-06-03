// Package config loads the heartbeat worker's settings from the environment.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// Config is the heartbeat worker configuration. It carries the shared HTTP
// fields (so the health/metrics server can be built from libs/httpx) plus the
// worker's own tick interval.
//
// Keys (all prefixed HEARTBEAT_):
//
//	HEARTBEAT_HTTP_ADDR              health/metrics listen address (default ":8081")
//	HEARTBEAT_HTTP_SHUTDOWN_TIMEOUT  graceful-shutdown budget       (default "10s")
//	HEARTBEAT_LOG_LEVEL              debug|info|warn|error          (default "info")
//	HEARTBEAT_LOG_FORMAT             json|text                      (default "json")
//	HEARTBEAT_INTERVAL              tick period                    (default "5s")
type Config struct {
	Addr            string        `env:"HTTP_ADDR" envDefault:":8081"`
	ShutdownTimeout time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json"`
	Interval        time.Duration `env:"INTERVAL" envDefault:"5s"`
}

// Load parses the configuration from HEARTBEAT_-prefixed environment vars.
func Load() (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: "HEARTBEAT_"}); err != nil {
		return Config{}, fmt.Errorf("heartbeat: parse config: %w", err)
	}
	return cfg, nil
}

// HTTP projects the shared HTTP fields into a libs/httpx Config.
func (c Config) HTTP() httpx.Config {
	return httpx.Config{
		Addr:            c.Addr,
		ShutdownTimeout: c.ShutdownTimeout,
		LogLevel:        c.LogLevel,
		LogFormat:       c.LogFormat,
	}
}
