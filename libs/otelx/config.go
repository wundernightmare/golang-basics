package otelx

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

// Config is the tracing configuration, populated from the environment with a
// per-service prefix (e.g. "TASKS_"). The endpoint/insecure keys follow the
// OpenTelemetry environment-variable conventions so they can also be set the
// standard way in a deployment.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_OTEL_ENABLED                 turn tracing export on/off       (default false)
//	TASKS_OTEL_SERVICE_NAME            resource service.name            (default "service")
//	TASKS_OTEL_SERVICE_VERSION         resource service.version         (default "dev")
//	TASKS_OTEL_EXPORTER_OTLP_ENDPOINT  collector host:port (gRPC)       (default "localhost:4317")
//	TASKS_OTEL_EXPORTER_OTLP_INSECURE  use plaintext (no TLS) to it     (default true)
//	TASKS_OTEL_TRACES_SAMPLER_RATIO    head sampling ratio [0.0–1.0]    (default 1.0)
type Config struct {
	Enabled      bool    `env:"OTEL_ENABLED" envDefault:"false"`
	ServiceName  string  `env:"OTEL_SERVICE_NAME" envDefault:"service"`
	Version      string  `env:"OTEL_SERVICE_VERSION" envDefault:"dev"`
	Endpoint     string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	Insecure     bool    `env:"OTEL_EXPORTER_OTLP_INSECURE" envDefault:"true"`
	SamplerRatio float64 `env:"OTEL_TRACES_SAMPLER_RATIO" envDefault:"1.0"`
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field is defaulted; tracing export is off
// unless OTEL_ENABLED is set.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("otelx: parse config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}
