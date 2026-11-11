package config_test

import (
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/ingest/internal/config"
)

// base is the least a deployment sets: a bucket, credentials, an admin token.
func base(env map[string]string) map[string]string {
	out := map[string]string{
		"INGEST_ADMIN_TOKEN":          "s3cret",
		"INGEST_S3_BUCKET":            "events",
		"INGEST_S3_ENDPOINT":          "https://s3.internal",
		"INGEST_S3_ACCESS_KEY_ID":     "key",
		"INGEST_S3_SECRET_ACCESS_KEY": "secret",
	}
	maps.Copy(out, env)

	return out
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom("", base(nil))
	require.NoError(t, err)

	assert.Equal(t, config.ModeIngest, cfg.Mode)
	assert.Equal(t, "ingest", cfg.Service)
	assert.Equal(t, ":8084", cfg.Addr)
	assert.Equal(t, ":9084", cfg.AdminAddr)
	assert.False(t, cfg.KafkaEnabled(), "no brokers: the archive answers every batch")

	assert.Equal(t, 65536, cfg.Ingest.MaxEventBytes)
	assert.Equal(t, 1000, cfg.Ingest.MaxBatchEvents)
	assert.Equal(t, int64(128<<20), cfg.Ingest.MaxInFlightBytes)
	assert.Equal(t, time.Second, cfg.Ingest.KafkaAckGrace)
	assert.Equal(t, 15*time.Second, cfg.Ingest.DrainTimeout)

	assert.Equal(t, "events/", cfg.Archive.Prefix)
	assert.Equal(t, 8<<20, cfg.Archive.BatchBytes)
	assert.Equal(t, 10*time.Second, cfg.Archive.Linger)
	assert.Equal(t, time.Second, cfg.Archive.UrgentLinger)

	// The presets that differ from the libs' and the tags' defaults.
	assert.Equal(t, 5*time.Second, cfg.S3.OpTimeout, "a put an answer waits on fails in seconds")
	assert.Equal(t, "ingest.events", cfg.Kafka.Topic)
	assert.Equal(t, 10, cfg.Ingest.KafkaBreaker.MinRequests)
	assert.Equal(t, 2, cfg.Ingest.ArchiveBreaker.MinRequests, "an archive call is a whole batch")
	assert.Equal(t, time.Minute, cfg.Ingest.ArchiveBreaker.Window)
	assert.InDelta(t, 0.5, cfg.Ingest.ArchiveBreaker.FailureRatio, 0, "the tag default fills what the preset left")

	assert.Equal(t, 5*time.Minute, cfg.Compact.Interval)
	assert.Equal(t, 15*time.Minute, cfg.Compact.CloseDelay)
	assert.True(t, cfg.Compact.DeleteSources)
}

func TestLoadFromYAMLWithEnvOnTop(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
mode: ingest
admin_token: s3cret
s3:
  bucket: from-yaml
  endpoint: https://s3.internal
  access_key_id: key
  secret_access_key: secret
  op_timeout: 3s
kafka:
  brokers: [k1:9092, k2:9092]
  publish_timeout: 2s
ingest:
  max_event_bytes: 1024
  kafka_ack_grace: 250ms
  kafka_breaker: {open_timeout: 1s}
archive:
  linger: 30s
  urgent_linger: 500ms
compact:
  interval: 1m
`), 0o600))

	cfg, err := config.LoadFrom(path, map[string]string{
		"INGEST_S3_BUCKET":                      "from-env",
		"INGEST_ARCHIVE_URGENT_LINGER":          "2s",
		"INGEST_ARCHIVE_BREAKER_OPEN_TIMEOUT":   "42s",
		"INGEST_KAFKA_BREAKER_HALF_OPEN_PROBES": "3",
	})
	require.NoError(t, err)

	assert.Equal(t, "from-env", cfg.S3.Bucket, "env > yaml")
	assert.Equal(t, 3*time.Second, cfg.S3.OpTimeout, "yaml > preset")
	assert.True(t, cfg.KafkaEnabled())
	assert.Equal(t, 2*time.Second, cfg.Kafka.PublishTimeout)
	assert.Equal(t, 1024, cfg.Ingest.MaxEventBytes)
	assert.Equal(t, 250*time.Millisecond, cfg.Ingest.KafkaAckGrace)
	assert.Equal(t, time.Second, cfg.Ingest.KafkaBreaker.OpenTimeout)
	assert.Equal(t, 3, cfg.Ingest.KafkaBreaker.HalfOpenProbes)
	assert.Equal(t, 42*time.Second, cfg.Ingest.ArchiveBreaker.OpenTimeout)
	assert.Equal(t, 30*time.Second, cfg.Archive.Linger)
	assert.Equal(t, 2*time.Second, cfg.Archive.UrgentLinger)
	assert.Equal(t, time.Minute, cfg.Compact.Interval)

	_, err = config.LoadFrom(filepath.Join(t.TempDir(), "absent.yaml"), base(nil))
	require.Error(t, err, "a config path that is set must exist")
}

func TestCompactModeIsAWorker(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom("", base(map[string]string{"INGEST_MODE": "compact"}))
	require.NoError(t, err)
	assert.Empty(t, cfg.Addr, "no API listener")
	assert.Equal(t, ":9084", cfg.AdminAddr)

	// A config file shared with the ingest pods may carry Kafka settings
	// the compactor has no brokers for: they are not its business.
	_, err = config.LoadFrom("", base(map[string]string{"INGEST_MODE": "compact", "INGEST_KAFKA_PUBLISH_TIMEOUT": "9s"}))
	require.NoError(t, err)
}

func TestValidateRejects(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		env  map[string]string
		want string
	}{
		"unknown mode":            {map[string]string{"INGEST_MODE": "both"}, "MODE must be"},
		"no bucket":               {map[string]string{"INGEST_S3_BUCKET": ""}, "S3_BUCKET must be set"},
		"plaintext s3":            {map[string]string{"INGEST_S3_ENDPOINT": "http://s3.internal"}, "S3_INSECURE"},
		"kafka keys, no brokers":  {map[string]string{"INGEST_KAFKA_PUBLISH_TIMEOUT": "9s"}, "KAFKA_BROKERS is not"},
		"event over kafka limit":  {map[string]string{"INGEST_KAFKA_BROKERS": "k:9092", "INGEST_KAFKA_MESSAGE_MAX_BYTES": "65536"}, "does not fit KAFKA_MESSAGE_MAX_BYTES"},
		"event over body limit":   {map[string]string{"INGEST_MAX_EVENT_BYTES": "2097152"}, "MAX_EVENT_BYTES must be"},
		"batch over the contract": {map[string]string{"INGEST_MAX_BATCH_EVENTS": "1001"}, "MAX_BATCH_EVENTS must be"},
		"in-flight under a body":  {map[string]string{"INGEST_MAX_IN_FLIGHT_BYTES": "1024"}, "MAX_IN_FLIGHT_BYTES"},
		"bulkhead under a batch":  {map[string]string{"INGEST_KAFKA_MAX_IN_FLIGHT": "10"}, "KAFKA_MAX_IN_FLIGHT"},
		"retry-after under 1s":    {map[string]string{"INGEST_RETRY_AFTER": "100ms"}, "RETRY_AFTER"},
		"kafka breaker ratio":     {map[string]string{"INGEST_KAFKA_BREAKER_FAILURE_RATIO": "1.5"}, "KAFKA_BREAKER_"},
		"archive breaker probes":  {map[string]string{"INGEST_ARCHIVE_BREAKER_HALF_OPEN_PROBES": "0"}, "ARCHIVE_BREAKER_"},
		"urgent over linger":      {map[string]string{"INGEST_ARCHIVE_URGENT_LINGER": "20s"}, "ARCHIVE_URGENT_LINGER"},
		"archive compression":     {map[string]string{"INGEST_ARCHIVE_COMPRESSION": "gzip"}, "ARCHIVE_COMPRESSION"},
		"batch bytes":             {map[string]string{"INGEST_ARCHIVE_BATCH_BYTES": "0"}, "ARCHIVE_BATCH_BYTES"},
		"deadline under the wait": {map[string]string{"INGEST_HTTP_REQUEST_TIMEOUT": "6s"}, "HTTP_REQUEST_TIMEOUT"},
		"close delay under a put": {map[string]string{"INGEST_COMPACT_CLOSE_DELAY": "1s"}, "COMPACT_CLOSE_DELAY"},
		"compact interval":        {map[string]string{"INGEST_COMPACT_INTERVAL": "0s"}, "COMPACT_INTERVAL"},
		"compact sizes":           {map[string]string{"INGEST_COMPACT_FETCH_CONCURRENCY": "0"}, "COMPACT_PART_BYTES"},
		"compact compression":     {map[string]string{"INGEST_COMPACT_COMPRESSION": "lz4"}, "COMPACT_COMPRESSION"},
		"no admin token":          {map[string]string{"INGEST_ADMIN_TOKEN": ""}, "ADMIN_TOKEN"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := config.LoadFrom("", base(tc.env))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDefaultsAreTheReference(t *testing.T) {
	t.Parallel()

	def, err := config.Defaults()
	require.NoError(t, err)
	assert.Equal(t, "ingest", def.Kafka.ClientID)
	assert.Equal(t, "ingest", def.OTel.ServiceName)
	assert.Equal(t, 10*time.Second, def.Archive.Linger)
}

// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
//
//nolint:paralleltest // t.Setenv
func TestLoad_ReadsTheProcessEnvironment(t *testing.T) {
	for k, v := range base(map[string]string{"INGEST_ARCHIVE_PREFIX": "from-env/"}) {
		t.Setenv(k, v)
	}

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "from-env/", cfg.Archive.Prefix)
}
