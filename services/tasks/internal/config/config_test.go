package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/tasks/internal/config"
)

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// The admin listener is on a routable address by default, so it needs a
// token (or ADMIN_INSECURE) to validate — as in any deployment.
func withAdminToken(t *testing.T) {
	t.Helper()
	t.Setenv("TASKS_ADMIN_TOKEN", "s3cret")
}

func TestLoadDefaults(t *testing.T) {
	withAdminToken(t)
	cfg, err := config.Load("")
	require.NoError(t, err)

	assert.Equal(t, "tasks", cfg.Service)
	assert.Equal(t, ":8082", cfg.Addr)
	assert.Equal(t, ":9082", cfg.AdminAddr)
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout, "httpx defaults apply")
	assert.Equal(t, 100, cfg.LogSampleInitial)

	assert.Equal(t, "localhost", cfg.Postgres.Host, "pgx defaults apply")
	assert.Equal(t, 30*time.Second, cfg.Postgres.StatementTimeout)
	assert.Equal(t, "localhost:6379", cfg.Valkey.Addr)
	assert.Equal(t, 500*time.Millisecond, cfg.Valkey.OpTimeout)
	assert.Equal(t, []string{"localhost:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "tasks.events", cfg.Kafka.Topic)
	assert.Equal(t, "tasks", cfg.Kafka.ClientID, "service default beats the lib's")
	assert.Equal(t, 5*time.Second, cfg.Kafka.PublishTimeout)

	assert.False(t, cfg.OTel.Enabled)
	assert.Equal(t, "tasks", cfg.OTel.ServiceName)
	assert.False(t, cfg.OTel.Insecure, "TLS to the collector unless configured otherwise")
	assert.Equal(t, "localhost:4318", cfg.OTel.Endpoint)

	assert.Equal(t, time.Minute, cfg.Cache.TTL)
	assert.Equal(t, 5*time.Second, cfg.Cache.TombstoneTTL)
	assert.Equal(t, time.Second, cfg.Outbox.PollInterval)
	assert.Equal(t, 100, cfg.Outbox.BatchSize)
	assert.Equal(t, 24*time.Hour, cfg.Idempotency.TTL)
}

// The keys each lib documents work unchanged under TASKS_.
func TestLibKeysKeepTheirDocumentedNames(t *testing.T) {
	withAdminToken(t)
	for k, v := range map[string]string{
		"TASKS_DATABASE_URL":                "postgres://u:p@db:5432/app",
		"TASKS_DB_MAX_CONNS":                "33",
		"TASKS_DB_STATEMENT_TIMEOUT":        "7s",
		"TASKS_VALKEY_URL":                  "valkey://cache:6379",
		"TASKS_VALKEY_OP_TIMEOUT":           "250ms",
		"TASKS_KAFKA_BROKERS":               "k1:9092,k2:9092",
		"TASKS_KAFKA_TOPIC":                 "custom.events",
		"TASKS_OTEL_ENABLED":                "true",
		"TASKS_OTEL_EXPORTER_OTLP_INSECURE": "true",
		"TASKS_OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4318",
		"TASKS_HTTP_REQUEST_TIMEOUT":        "3s",
		"TASKS_CACHE_TTL":                   "30s",
		"TASKS_OUTBOX_BATCH_SIZE":           "10",
		"TASKS_IDEMPOTENCY_KEY_TTL":         "1h",
		"TASKS_HTTP_ADDR":                   ":1234",
	} {
		t.Setenv(k, v)
	}
	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "postgres://u:p@db:5432/app", cfg.Postgres.URL)
	assert.Equal(t, int32(33), cfg.Postgres.MaxConns)
	assert.Equal(t, 7*time.Second, cfg.Postgres.StatementTimeout)
	assert.Equal(t, "valkey://cache:6379", cfg.Valkey.URL)
	assert.Equal(t, 250*time.Millisecond, cfg.Valkey.OpTimeout)
	assert.Equal(t, []string{"k1:9092", "k2:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "custom.events", cfg.Kafka.Topic)
	assert.True(t, cfg.OTel.Enabled)
	assert.True(t, cfg.OTel.Insecure)
	assert.Equal(t, "collector:4318", cfg.OTel.Endpoint)
	assert.Equal(t, 3*time.Second, cfg.RequestTimeout)
	assert.Equal(t, 30*time.Second, cfg.Cache.TTL)
	assert.Equal(t, 10, cfg.Outbox.BatchSize)
	assert.Equal(t, time.Hour, cfg.Idempotency.TTL)
	assert.Equal(t, ":1234", cfg.Addr)
}

func TestLoadYAMLThenEnvOverlay(t *testing.T) {
	withAdminToken(t)
	path := writeYAML(t, `
http_addr: ":9999"
postgres:
  url: postgres://app:app@yaml-db:5432/app
  lock_timeout: 2s
kafka:
  brokers: ["k1:9092", "k2:9092"]
  topic: yaml.events
cache:
  ttl: 30s
outbox:
  poll_interval: 250ms
`)
	t.Setenv("TASKS_KAFKA_TOPIC", "env.events")

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, ":9999", cfg.Addr, "from YAML")
	assert.Equal(t, ":9082", cfg.AdminAddr, "service default: not in YAML or env")
	assert.Equal(t, "postgres://app:app@yaml-db:5432/app", cfg.Postgres.URL)
	assert.Equal(t, 2*time.Second, cfg.Postgres.LockTimeout)
	assert.Equal(t, 30*time.Second, cfg.Postgres.StatementTimeout, "lib default for what YAML left out")
	assert.Equal(t, []string{"k1:9092", "k2:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "env.events", cfg.Kafka.Topic, "env wins over YAML")
	assert.Equal(t, 30*time.Second, cfg.Cache.TTL)
	assert.Equal(t, 250*time.Millisecond, cfg.Outbox.PollInterval)
}

func TestLoadRejectsUnknownYAMLKeysAndBadValues(t *testing.T) {
	withAdminToken(t)
	_, err := config.Load(writeYAML(t, "databse_url: postgres://typo\n"))
	require.ErrorContains(t, err, "databse_url", "a typo is an error, not a silent default")

	_, err = config.Load(writeYAML(t, "outbox:\n  batch_size: 5000\n"))
	require.ErrorContains(t, err, "OUTBOX_BATCH_SIZE")

	t.Setenv("TASKS_ADMIN_TOKEN", "")
	_, err = config.Load("")
	require.ErrorContains(t, err, "ADMIN_TOKEN", "an open admin surface on :9082 is refused")
	t.Setenv("TASKS_ADMIN_INSECURE", "true")
	_, err = config.Load("")
	require.NoError(t, err, "… unless explicitly allowed for local development")
}

// The service's own defaults (name, ports, client id) apply only where the
// operator set nothing; an explicit value — even the libs' default — wins.
func TestServiceDefaultsYieldToExplicitValues(t *testing.T) {
	withAdminToken(t)
	path := writeYAML(t, "admin_addr: \":9080\"\nkafka:\n  client_id: from-yaml\n")
	t.Setenv("TASKS_SERVICE_NAME", "tasks-canary")
	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, ":9080", cfg.AdminAddr, "set in YAML to the lib's default: kept")
	assert.Equal(t, "from-yaml", cfg.Kafka.ClientID)
	assert.Equal(t, "tasks-canary", cfg.Service)
	assert.Equal(t, ":8082", cfg.Addr, "unset: the service default")
	assert.Equal(t, "tasks", cfg.OTel.ServiceName)
}
