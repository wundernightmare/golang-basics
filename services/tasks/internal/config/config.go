// Package config loads the tasks service's settings from an optional YAML file
// overlaid with environment variables (see [httpx.LoadYAML]) and projects them
// into the per-dependency configs of the shared libs. It is the worked example
// of the config-file story: one source of truth, env-overridable, defaulted in
// code (no envDefault tags, so env overlays YAML rather than resetting it).
package config

import (
	"time"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// Config is the tasks service configuration. Connection details are expressed
// as URLs so one line in config.yaml (or one env var) points each dependency at
// its server; the per-library tuning knobs keep their own defaults.
//
// Load from a file with TASKS_CONFIG=/path/config.yaml; every key is also an env
// var under the TASKS_ prefix (e.g. TASKS_DATABASE_URL, TASKS_KAFKA_BROKERS).
type Config struct {
	HTTPAddr        string        `yaml:"http_addr" env:"HTTP_ADDR"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" env:"HTTP_SHUTDOWN_TIMEOUT"`
	LogLevel        string        `yaml:"log_level" env:"LOG_LEVEL"`
	LogFormat       string        `yaml:"log_format" env:"LOG_FORMAT"`

	DatabaseURL string        `yaml:"database_url" env:"DATABASE_URL"`
	ValkeyURL   string        `yaml:"valkey_url" env:"VALKEY_URL"`
	CacheTTL    time.Duration `yaml:"cache_ttl" env:"CACHE_TTL"`

	KafkaBrokers []string `yaml:"kafka_brokers" env:"KAFKA_BROKERS" envSeparator:","`
	KafkaTopic   string   `yaml:"kafka_topic" env:"KAFKA_TOPIC"`

	OTelEnabled  bool    `yaml:"otel_enabled" env:"OTEL_ENABLED"`
	OTelEndpoint string  `yaml:"otel_endpoint" env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTelSampler  float64 `yaml:"otel_sampler_ratio" env:"OTEL_TRACES_SAMPLER_RATIO"`
}

// Load reads the config from the file named by TASKS_CONFIG (when set and
// present) and overlays TASKS_-prefixed environment variables, then fills in
// code defaults for anything still unset.
func Load(yamlPath string) (Config, error) {
	var cfg Config
	if err := httpx.LoadYAML(yamlPath, "TASKS_", &cfg); err != nil {
		return Config{}, err
	}
	cfg.withDefaults()
	return cfg, nil
}

// withDefaults fills sensible defaults for any field left unset by both YAML and
// env. Kept in code (not envDefault tags) so it runs last and never clobbers a
// value the operator supplied.
func (c *Config) withDefaults() {
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8082"
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogFormat == "" {
		c.LogFormat = "json"
	}
	if c.DatabaseURL == "" {
		c.DatabaseURL = "postgres://app:app@localhost:5432/app?sslmode=disable"
	}
	if c.ValkeyURL == "" {
		c.ValkeyURL = "valkey://localhost:6379"
	}
	if c.CacheTTL == 0 {
		c.CacheTTL = time.Minute
	}
	if len(c.KafkaBrokers) == 0 {
		c.KafkaBrokers = []string{"localhost:9092"}
	}
	if c.KafkaTopic == "" {
		c.KafkaTopic = "tasks.events"
	}
	if c.OTelEndpoint == "" {
		c.OTelEndpoint = "localhost:4317"
	}
	if c.OTelSampler == 0 {
		c.OTelSampler = 1.0
	}
}

// HTTP projects the shared HTTP-server fields into a libs/httpx Config.
func (c Config) HTTP() httpx.Config {
	return httpx.Config{
		Addr:            c.HTTPAddr,
		ShutdownTimeout: c.ShutdownTimeout,
		LogLevel:        c.LogLevel,
		LogFormat:       c.LogFormat,
	}
}

// Postgres projects the database settings, keeping libs/pgx's own pool/timeout
// defaults (zero values are left untouched by pgx.New).
func (c Config) Postgres() pgx.Config {
	return pgx.Config{URL: c.DatabaseURL, ConnectTimeout: 5 * time.Second, MaxConns: 10}
}

// Valkey projects the cache settings.
func (c Config) Valkey() valkey.Config {
	return valkey.Config{URL: c.ValkeyURL, DialTimeout: 5 * time.Second}
}

// Kafka projects the broker settings (producer shape — Topic is the publish
// target).
func (c Config) Kafka() kafka.Config {
	return kafka.Config{
		Brokers:     c.KafkaBrokers,
		Topic:       c.KafkaTopic,
		ClientID:    "tasks",
		DialTimeout: 10 * time.Second,
	}
}

// OTel projects the tracing settings, naming this service in every span.
func (c Config) OTel() otelx.Config {
	return otelx.Config{
		Enabled:      c.OTelEnabled,
		ServiceName:  "tasks",
		Version:      "dev",
		Endpoint:     c.OTelEndpoint,
		Insecure:     true,
		SamplerRatio: c.OTelSampler,
	}
}
