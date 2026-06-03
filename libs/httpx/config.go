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
// Keys (with the "PING_" prefix as an example):
//
//	PING_HTTP_ADDR              listen address           (default ":8080")
//	PING_HTTP_SHUTDOWN_TIMEOUT  graceful-shutdown budget (default "10s")
//	PING_LOG_LEVEL              debug|info|warn|error    (default "info")
//	PING_LOG_FORMAT             json|text                (default "json")
type Config struct {
	Addr            string        `env:"HTTP_ADDR" envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json"`
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
