package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/consumer/internal/config"
)

func TestLoad_DefaultsAreSecure(t *testing.T) {
	_, err := config.Load("")
	require.ErrorContains(t, err, "ADMIN_TOKEN is empty", "an open admin listener on :9083 must be asked for")

	t.Setenv("CONSUMER_ADMIN_INSECURE", "true")
	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "consumer", cfg.Service)
	assert.Empty(t, cfg.Addr, "a worker has no API listener")
	assert.Equal(t, ":9083", cfg.AdminAddr)
	assert.Equal(t, "consumer", cfg.Kafka.ClientID)
	assert.Equal(t, []string{"tasks.events"}, cfg.Kafka.Topics)
	assert.Equal(t, "tasks-consumer", cfg.Kafka.Group)
	assert.Equal(t, 5, cfg.Kafka.MaxRetries)
	assert.Equal(t, "{topic}.dlq", cfg.Kafka.DeadLetterTopic)
	assert.False(t, cfg.OTel.Insecure, "OTLP export is TLS unless configured otherwise")
	assert.Equal(t, "consumer", cfg.OTel.ServiceName)
	assert.Equal(t, 10000, cfg.DedupeSize)
}

func TestLoad_YAMLThenEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
admin_addr: "127.0.0.1:9183"
http_addr: ":8083"
log_level: debug
dedupe_size: 50
kafka:
  brokers: [a:9092, b:9092]
  topic: from-yaml
  dead_letter_topic: none
  consumer_max_retries: 2
  tls_enabled: true
  sasl_mechanism: scram-sha-512
  sasl_username: svc
otel:
  enabled: true
  insecure: true
`), 0o600))
	t.Setenv("CONSUMER_KAFKA_SASL_PASSWORD", "s3cret")
	t.Setenv("CONSUMER_KAFKA_CONSUMER_RETRY_BACKOFF", "1s")

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9183", cfg.AdminAddr, "loopback: no token needed")
	assert.Empty(t, cfg.Addr, "http_addr is ignored for a worker")
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, 50, cfg.DedupeSize)
	assert.Equal(t, []string{"a:9092", "b:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, []string{"from-yaml"}, cfg.Kafka.Topics)
	assert.Equal(t, "none", cfg.Kafka.DeadLetterTopic)
	assert.Equal(t, 2, cfg.Kafka.MaxRetries)
	assert.Equal(t, time.Second, cfg.Kafka.RetryBackoff, "env overlays the file")
	assert.Equal(t, "s3cret", cfg.Kafka.SASLPassword)
	assert.True(t, cfg.OTel.Enabled)
	assert.True(t, cfg.OTel.Insecure, "insecure export comes from config, never from code")
}

func TestLoad_RejectsBadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("admin_insecure: true\nkafka:\n  topicz: typo\n"), 0o600))
	_, err := config.Load(path)
	require.ErrorContains(t, err, "topicz", "an unknown key is an error, not a silent default")

	t.Setenv("CONSUMER_ADMIN_INSECURE", "true")
	t.Setenv("CONSUMER_KAFKA_SASL_MECHANISM", "kerberos")
	t.Setenv("CONSUMER_DEDUPE_SIZE", "-1")
	_, err = config.Load("")
	require.ErrorContains(t, err, "KAFKA_SASL_MECHANISM")
	require.ErrorContains(t, err, "DEDUPE_SIZE", "all problems are reported at once")
}
