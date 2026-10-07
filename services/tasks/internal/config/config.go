// Package config loads the tasks service's settings from an optional YAML
// file overlaid with TASKS_-prefixed environment variables (see
// [httpx.LoadYAML]: env > YAML > default). It embeds the shared libs' own
// Config structs rather than copying their fields, so every tuning knob a lib
// offers — pool sizes, session timeouts, TLS, sampling — is configurable here
// under the key the lib documents, with the lib's default.
package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// EnvPrefix is the prefix of every environment key.
const EnvPrefix = "TASKS_"

// Config is the tasks service configuration.
//
// The lib sections are embedded without an envPrefix: their keys already
// carry a namespace (DATABASE_URL / DB_*, VALKEY_*, KAFKA_*, OTEL_*), so the
// environment keys are the ones each lib documents under TASKS_ —
// TASKS_DATABASE_URL, TASKS_DB_STATEMENT_TIMEOUT, TASKS_VALKEY_URL,
// TASKS_KAFKA_BROKERS, TASKS_OTEL_ENABLED… In YAML they are sections:
//
//	http_addr: ":8082"          # httpx.Config, inline at the top level
//	postgres: {url: …}          # pgx.Config
//	valkey:   {url: …}          # valkey.Config
//	kafka:    {brokers: […]}    # kafka.Config (topic = where events go)
//	otel:     {enabled: true}   # otelx.Config
//	cache:    {ttl: 1m}
//	outbox:   {poll_interval: 1s}
//	idempotency: {ttl: 24h}
type Config struct {
	httpx.Config `yaml:",inline"`

	Postgres pgx.Config    `yaml:"postgres"`
	Valkey   valkey.Config `yaml:"valkey"`
	Kafka    kafka.Config  `yaml:"kafka"`
	OTel     otelx.Config  `yaml:"otel"`

	Cache       CacheConfig       `yaml:"cache"`
	Outbox      OutboxConfig      `yaml:"outbox"`
	Idempotency IdempotencyConfig `yaml:"idempotency"`
}

// CacheConfig tunes the task read cache.
type CacheConfig struct {
	// TTL of a cached task (±10% jitter is applied).
	TTL time.Duration `env:"CACHE_TTL" envDefault:"1m" yaml:"ttl"`
	// TombstoneTTL is how long a PATCH/DELETE keeps the key from being
	// re-filled, so a read racing the write cannot cache the old task (see
	// libs/valkey). A few times the slowest store read.
	TombstoneTTL time.Duration `env:"CACHE_TOMBSTONE_TTL" envDefault:"5s" yaml:"tombstone_ttl"`
}

// OutboxConfig tunes the outbox relay.
type OutboxConfig struct {
	PollInterval time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"1s" yaml:"poll_interval"`
	BatchSize    int           `env:"OUTBOX_BATCH_SIZE" envDefault:"100" yaml:"batch_size"`
	MaxBackoff   time.Duration `env:"OUTBOX_MAX_BACKOFF" envDefault:"30s" yaml:"max_backoff"`
}

// IdempotencyConfig tunes Idempotency-Key retention.
type IdempotencyConfig struct {
	TTL           time.Duration `env:"IDEMPOTENCY_KEY_TTL" envDefault:"24h" yaml:"ttl"`
	PurgeInterval time.Duration `env:"IDEMPOTENCY_PURGE_INTERVAL" envDefault:"10m" yaml:"purge_interval"`
}

// Load reads the config from the YAML file at yamlPath (optional: a missing
// file means env-only), overlays TASKS_ environment variables and validates
// the result. This service's own defaults (name, ports, client id) are set
// before loading: [httpx.LoadYAML] fills the libs' tag defaults only into
// fields still at zero, so a preset survives, and the file and the
// environment override it.
func Load(yamlPath string) (Config, error) {
	cfg := Config{}
	cfg.Service = "tasks"
	cfg.Addr = ":8082"
	cfg.AdminAddr = ":9082"
	cfg.Kafka.ClientID = "tasks"
	cfg.OTel.ServiceName = "tasks"
	cfg.OTel.Version = httpx.Version
	if err := httpx.LoadYAML(yamlPath, EnvPrefix, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the whole config: the HTTP server's own rules (see
// [httpx.Config.Validate], which also applies its defaults) and this
// service's.
func (c *Config) Validate() error {
	var errs []error
	if err := c.Config.Validate(); err != nil {
		errs = append(errs, err)
	}
	if c.Postgres.URL == "" && c.Postgres.Host == "" {
		errs = append(errs, errors.New("DATABASE_URL or DB_HOST must be set"))
	}
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS must be set"))
	}
	if c.Kafka.Topic == "" {
		errs = append(errs, errors.New("KAFKA_TOPIC must be set: it is where the task events go"))
	}
	if c.Cache.TTL <= 0 || c.Cache.TombstoneTTL <= 0 {
		errs = append(errs, errors.New("CACHE_TTL and CACHE_TOMBSTONE_TTL must be positive"))
	}
	if c.Outbox.PollInterval <= 0 || c.Outbox.MaxBackoff < c.Outbox.PollInterval {
		errs = append(errs, errors.New("OUTBOX_POLL_INTERVAL must be positive and at most OUTBOX_MAX_BACKOFF"))
	}
	if c.Outbox.BatchSize < 1 || c.Outbox.BatchSize > 1000 {
		errs = append(errs, fmt.Errorf("OUTBOX_BATCH_SIZE must be 1..1000, got %d", c.Outbox.BatchSize))
	}
	if c.Idempotency.TTL <= 0 || c.Idempotency.PurgeInterval <= 0 {
		errs = append(errs, errors.New("IDEMPOTENCY_KEY_TTL and IDEMPOTENCY_PURGE_INTERVAL must be positive"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}
