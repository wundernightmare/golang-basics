// Package config loads the consumer worker's settings from the environment
// (CONSUMER_-prefixed) and projects them into the shared libs' configs. It is
// env-only, mirroring services/heartbeat — the YAML config-file story is shown
// in services/tasks instead.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
)

// Config is the consumer worker configuration. It carries the shared HTTP
// fields (for the health/metrics server) plus the broker subscription.
//
// Keys (all prefixed CONSUMER_):
//
//	CONSUMER_HTTP_ADDR              health/metrics listen address (default ":8083")
//	CONSUMER_HTTP_SHUTDOWN_TIMEOUT  graceful-shutdown budget       (default "10s")
//	CONSUMER_LOG_LEVEL              debug|info|warn|error          (default "info")
//	CONSUMER_LOG_FORMAT             json|text                      (default "json")
//	CONSUMER_KAFKA_BROKERS          comma-separated seeds          (default "localhost:9092")
//	CONSUMER_KAFKA_TOPIC            topic to drain                 (default "tasks.events")
//	CONSUMER_KAFKA_GROUP            consumer group id              (default "tasks-consumer")
//	CONSUMER_OTEL_ENABLED           export traces                  (default false)
//	CONSUMER_OTEL_EXPORTER_OTLP_ENDPOINT  collector host:port      (default "localhost:4317")
type Config struct {
	Addr            string        `env:"HTTP_ADDR" envDefault:":8083"`
	ShutdownTimeout time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s"`
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json"`

	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	KafkaTopic   string   `env:"KAFKA_TOPIC" envDefault:"tasks.events"`
	KafkaGroup   string   `env:"KAFKA_GROUP" envDefault:"tasks-consumer"`

	OTelEnabled  bool    `env:"OTEL_ENABLED" envDefault:"false"`
	OTelEndpoint string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	OTelSampler  float64 `env:"OTEL_TRACES_SAMPLER_RATIO" envDefault:"1.0"`
}

// Load parses the configuration from CONSUMER_-prefixed environment vars.
func Load() (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: "CONSUMER_"}); err != nil {
		return Config{}, fmt.Errorf("consumer: parse config: %w", err)
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

// Kafka projects the subscription into a libs/kafka Config (consumer shape).
func (c Config) Kafka() kafka.Config {
	return kafka.Config{
		Brokers:     c.KafkaBrokers,
		Topic:       c.KafkaTopic,
		Topics:      []string{c.KafkaTopic},
		Group:       c.KafkaGroup,
		ClientID:    "consumer",
		DialTimeout: 10 * time.Second,
	}
}

// OTel projects the tracing settings, naming this service in every span.
func (c Config) OTel() otelx.Config {
	return otelx.Config{
		Enabled:      c.OTelEnabled,
		ServiceName:  "consumer",
		Version:      "dev",
		Endpoint:     c.OTelEndpoint,
		Insecure:     true,
		SamplerRatio: c.OTelSampler,
	}
}
