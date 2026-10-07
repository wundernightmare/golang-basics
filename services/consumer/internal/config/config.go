// Package config loads the consumer worker's settings: an optional YAML file
// (CONSUMER_CONFIG) overlaid with CONSUMER_-prefixed environment variables,
// through httpx.LoadYAML. The service config embeds the libs' own Config
// structs, so every tuning knob a lib grows is configurable here without
// copying fields.
package config

import (
	"errors"
	"fmt"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
)

// Prefix is the environment-variable prefix of every key.
const Prefix = "CONSUMER_"

// Config is the consumer worker configuration.
//
// Keys (all prefixed CONSUMER_; YAML keys in parentheses):
//
//   - the libs/httpx keys (top level, e.g. ADMIN_ADDR, ADMIN_TOKEN,
//     ADMIN_INSECURE, LOG_LEVEL, LOG_FORMAT); a worker has no API listener,
//     so HTTP_ADDR is ignored and the admin listener defaults to ":9083";
//   - the libs/kafka keys (under "kafka:"), e.g. KAFKA_BROKERS, KAFKA_TOPIC,
//     KAFKA_GROUP, KAFKA_TLS_*, KAFKA_SASL_*, KAFKA_CONSUMER_MAX_RETRIES,
//     KAFKA_DEAD_LETTER_TOPIC; the client id defaults to "consumer";
//   - the libs/otelx keys (under "otel:"), e.g. OTEL_ENABLED,
//     OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE;
//   - DEDUPE_SIZE (dedupe_size): event ids remembered for duplicate
//     suppression (default 10000).
//
// The lib keys already carry their KAFKA_ / OTEL_ namespace, so the nested
// structs take no extra envPrefix: CONSUMER_KAFKA_BROKERS, not
// CONSUMER_KAFKA_KAFKA_BROKERS.
type Config struct {
	httpx.Config `yaml:",inline"`
	Kafka        kafka.Config `yaml:"kafka"`
	OTel         otelx.Config `yaml:"otel"`
	DedupeSize   int          `env:"DEDUPE_SIZE" envDefault:"10000" yaml:"dedupe_size"`
}

// Load reads the YAML file at path (optional: a missing file means env-only),
// overlays the CONSUMER_ environment and validates the result. Service-level
// defaults (name, admin port, client id) are set before loading, so the file
// and the environment still override them.
func Load(path string) (Config, error) {
	var cfg Config
	cfg.Service = "consumer"
	cfg.AdminAddr = ":9083"
	cfg.Kafka.ClientID = "consumer"
	cfg.OTel.ServiceName = "consumer"
	if err := httpx.LoadYAML(path, Prefix, &cfg); err != nil {
		return Config{}, err
	}
	cfg.Addr = "" // a worker serves no API: admin listener only
	if cfg.OTel.Version == "" {
		cfg.OTel.Version = httpx.Version
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks every part and reports all problems at once.
func (c *Config) Validate() error {
	var errs []error
	if err := c.Config.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("http: %w", err))
	}
	if err := c.Kafka.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("kafka: %w", err))
	}
	if c.DedupeSize <= 0 {
		errs = append(errs, errors.New("DEDUPE_SIZE must be positive"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("consumer config: %w", err)
	}
	return nil
}
