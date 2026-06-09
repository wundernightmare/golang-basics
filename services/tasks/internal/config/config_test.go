package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/tasks/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load("")
	require.NoError(t, err)
	require.Equal(t, ":8082", cfg.HTTPAddr)
	require.Equal(t, time.Minute, cfg.CacheTTL)
	require.Equal(t, []string{"localhost:9092"}, cfg.KafkaBrokers)
	require.False(t, cfg.OTelEnabled)

	// Projections inherit the right values.
	require.Equal(t, ":8082", cfg.HTTP().Addr)
	require.Equal(t, "tasks", cfg.OTel().ServiceName)
	require.Equal(t, "tasks.events", cfg.Kafka().Topic)
}

func TestLoadYAMLThenEnvOverlay(t *testing.T) {
	yaml := "" +
		"http_addr: \":9999\"\n" +
		"cache_ttl: 30s\n" +
		"kafka_brokers: [\"k1:9092\", \"k2:9092\"]\n" +
		"kafka_topic: custom.events\n"
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	// env overrides one field; the rest come from YAML.
	t.Setenv("TASKS_KAFKA_TOPIC", "env.events")

	cfg, err := config.Load(path)
	require.NoError(t, err)
	require.Equal(t, ":9999", cfg.HTTPAddr, "from YAML")
	require.Equal(t, 30*time.Second, cfg.CacheTTL, "from YAML")
	require.Equal(t, []string{"k1:9092", "k2:9092"}, cfg.KafkaBrokers, "from YAML")
	require.Equal(t, "env.events", cfg.KafkaTopic, "env wins over YAML")
}
