// Package config loads the heartbeat worker's settings from the environment.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// Prefix is the environment-variable prefix of every heartbeat key.
const Prefix = "HEARTBEAT_"

// defaultAdminAddr keeps heartbeat's admin listener off ping's default port
// when both run on one host.
const defaultAdminAddr = ":9081"

// Config is the heartbeat worker configuration: the shared server config
// (embedded, so every libs/httpx key — admin listener, token, timeouts, log
// level and sampling — is available with the HEARTBEAT_ prefix and the same
// defaults) plus the worker's own tick interval. A worker has no API
// listener, only the admin one: HTTP_ADDR is ignored.
//
// Keys (all prefixed HEARTBEAT_; see libs/httpx for the full list):
//
//	HEARTBEAT_ADMIN_ADDR             health/metrics/pprof listen address       (default ":9081")
//	HEARTBEAT_ADMIN_TOKEN            bearer token for /admin/* and /debug/*    (default "": see ADMIN_INSECURE)
//	HEARTBEAT_ADMIN_INSECURE         allow no token on a non-loopback address  (default false)
//	HEARTBEAT_HTTP_SHUTDOWN_DELAY    wait after readiness flips                (default "2s")
//	HEARTBEAT_HTTP_SHUTDOWN_TIMEOUT  graceful-shutdown budget                  (default "10s")
//	HEARTBEAT_LOG_LEVEL              debug|info|warn|error                     (default "info")
//	HEARTBEAT_LOG_FORMAT             json|text                                 (default "json")
//	HEARTBEAT_LOG_SAMPLE_INITIAL     log sampling: first N per msg per second  (default 100, 0 = off)
//	HEARTBEAT_LOG_SAMPLE_THEREAFTER  … then every M-th                         (default 100)
//	HEARTBEAT_INTERVAL               tick period                               (default "5s")
type Config struct {
	httpx.Config `yaml:",inline"`

	Interval time.Duration `env:"INTERVAL" envDefault:"5s" yaml:"interval"`
}

// Load parses the configuration from HEARTBEAT_-prefixed environment
// variables and validates it.
func Load() (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: Prefix}); err != nil {
		return Config{}, fmt.Errorf("heartbeat: parse config: %w", err)
	}
	// Our own default for the admin address; an explicitly empty value stays
	// empty (and is rejected: a worker without its admin listener is blind)
	// instead of falling back to the envDefault as the env parser would.
	if v, set := os.LookupEnv(Prefix + "ADMIN_ADDR"); set {
		cfg.AdminAddr = v
	} else {
		cfg.AdminAddr = defaultAdminAddr
	}
	cfg.Addr = "" // a worker serves no API
	cfg.Service = "heartbeat"
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("heartbeat: config: %w", err)
	}
	return cfg, nil
}

// Validate checks the shared server settings (see [httpx.Config.Validate])
// and the tick interval.
func (c *Config) Validate() error {
	if c.Interval <= 0 {
		return fmt.Errorf("%sINTERVAL must be positive, got %s", Prefix, c.Interval)
	}
	return c.Config.Validate()
}

// HTTP is the libs/httpx part of the configuration, what the admin server is
// built from.
func (c Config) HTTP() httpx.Config { return c.Config }
