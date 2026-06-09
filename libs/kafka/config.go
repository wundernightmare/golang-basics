package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the Kafka client configuration, populated from the environment with
// a per-service prefix (e.g. "TASKS_" for a producer, "CONSUMER_" for a
// consumer). The same struct serves both shapes; a [Producer] ignores Group and
// a [Consumer] ignores nothing — Topic is the produce target and Topics the
// subscribe set.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_KAFKA_BROKERS        comma-separated host:port seeds  (default "localhost:9092")
//	TASKS_KAFKA_TOPIC          default produce topic            (default "tasks.events")
//	TASKS_KAFKA_TOPICS         comma-separated subscribe topics (default: the Topic value)
//	TASKS_KAFKA_GROUP          consumer group id                (default "tasks-consumer")
//	TASKS_KAFKA_CLIENT_ID      client id advertised to brokers  (default "golang-basics")
//	TASKS_KAFKA_DIAL_TIMEOUT   broker dial timeout              (default "10s")
type Config struct {
	Brokers     []string      `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	Topic       string        `env:"KAFKA_TOPIC" envDefault:"tasks.events"`
	Topics      []string      `env:"KAFKA_TOPICS" envSeparator:","`
	Group       string        `env:"KAFKA_GROUP" envDefault:"tasks-consumer"`
	ClientID    string        `env:"KAFKA_CLIENT_ID" envDefault:"golang-basics"`
	DialTimeout time.Duration `env:"KAFKA_DIAL_TIMEOUT" envDefault:"10s"`
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field is defaulted; when KAFKA_TOPICS is unset
// the consumer subscribes to the single Topic.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("kafka: parse config (prefix %q): %w", prefix, err)
	}
	if len(cfg.Topics) == 0 && cfg.Topic != "" {
		cfg.Topics = []string{cfg.Topic}
	}
	return cfg, nil
}

func (c Config) brokersString() string { return strings.Join(c.Brokers, ",") }
