package httpx

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the shared HTTP-server configuration, populated from the
// environment. Services load it with a prefix (e.g. "PING_") so several
// binaries can coexist in one process tree without key collisions; an empty
// prefix reads the bare keys.
//
// Two listeners: the API listener (Addr) carries the service's own routes and
// nothing else; the admin listener (AdminAddr) carries everything operational
// — /healthz, /readyz, /metrics, /version and /debug/pprof — so an ingress that
// only routes to the API port never exposes them. A pure worker sets Addr to ""
// and gets only the admin listener.
//
// Keys (with the "PING_" prefix as an example):
//
//	PING_SERVICE_NAME           service name for logs, build_info, spans (default: executable name)
//	PING_HTTP_ADDR              API listen address                      (default ":8080")
//	PING_ADMIN_ADDR             admin listen address                    (default ":9080")
//	PING_HTTP_SHUTDOWN_TIMEOUT  graceful-shutdown budget                (default "10s")
//	PING_HTTP_SLOW_REQUEST      log a request at warn above this latency (default "1s", 0 = off)
//	PING_HEALTH_CHECK_INTERVAL  how often readiness checks run in the background (default "10s")
//	PING_HEALTH_CHECK_TIMEOUT   per-check timeout                       (default "3s")
//	PING_LOG_LEVEL              debug|info|warn|error                   (default "info")
//	PING_LOG_FORMAT             json|text                               (default "json")
//	PING_LOG_SAMPLE_INITIAL     per second, per message: pass the first N (default 100, 0 = no sampling)
//	PING_LOG_SAMPLE_THEREAFTER  … then every M-th                       (default 100)
type Config struct {
	Service             string        `env:"SERVICE_NAME"`
	Addr                string        `env:"HTTP_ADDR" envDefault:":8080"`
	AdminAddr           string        `env:"ADMIN_ADDR" envDefault:":9080"`
	ShutdownTimeout     time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s"`
	SlowRequest         time.Duration `env:"HTTP_SLOW_REQUEST" envDefault:"1s"`
	HealthInterval      time.Duration `env:"HEALTH_CHECK_INTERVAL" envDefault:"10s"`
	HealthTimeout       time.Duration `env:"HEALTH_CHECK_TIMEOUT" envDefault:"3s"`
	LogLevel            string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat           string        `env:"LOG_FORMAT" envDefault:"json"`
	LogSampleInitial    int           `env:"LOG_SAMPLE_INITIAL" envDefault:"100"`
	LogSampleThereafter int           `env:"LOG_SAMPLE_THEREAFTER" envDefault:"100"`
}

// LoadConfig parses a [Config] from the environment using the given key
// prefix (use "" for no prefix). It returns the fully-defaulted config even
// on success, so callers can rely on every field being set.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("httpx: parse config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}

// LogConfig projects the logging fields for [NewLogger].
func (c Config) LogConfig() LogConfig {
	return LogConfig{
		Service:          c.Service,
		Level:            c.LogLevel,
		Format:           c.LogFormat,
		SampleInitial:    c.LogSampleInitial,
		SampleThereafter: c.LogSampleThereafter,
	}
}
