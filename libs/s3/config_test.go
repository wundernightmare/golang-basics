package s3_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
)

// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
func TestLoadConfig_ReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv("INGEST_S3_BUCKET", "from-env")

	cfg, err := s3.LoadConfig("INGEST_")
	require.NoError(t, err)
	assert.Equal(t, "from-env", cfg.Bucket)
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := s3.LoadConfigFrom("INGEST_", map[string]string{})
	require.NoError(t, err)
	assert.False(t, cfg.Configured(), "no default bucket: unset means no object storage")
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.True(t, cfg.UsePathStyle)
	assert.False(t, cfg.Insecure)
	assert.Equal(t, 10*time.Second, cfg.OpTimeout)
	assert.Equal(t, time.Minute, cfg.TransferTimeout)
	assert.Equal(t, 3, cfg.MaxAttempts)
	assert.Equal(t, int64(16<<20), cfg.UploadPartBytes)
	assert.Equal(t, s3.ChecksumWhenRequired, cfg.RequestChecksum)

	_, err = s3.New(context.Background(), cfg, nil)
	require.ErrorContains(t, err, "S3_BUCKET must be set")

	_, err = s3.LoadConfigFrom("INGEST_", map[string]string{"INGEST_S3_MAX_ATTEMPTS": "many"})
	require.Error(t, err)
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := func() s3.Config {
		return s3.Config{Bucket: "b", Endpoint: "https://s3.internal:9000", AccessKeyID: "k", SecretAccessKey: "s"}
	}

	cfg := valid()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 10*time.Second, cfg.OpTimeout, "a hand-built config is defaulted")
	assert.Equal(t, int64(16<<20), cfg.UploadPartBytes)

	aws := s3.Config{Bucket: "b"}
	require.NoError(t, aws.Validate(), "no endpoint and no keys: AWS with the default credential chain")

	missingCA := filepath.Join(t.TempDir(), "absent.pem")

	for name, tc := range map[string]struct {
		mutate func(*s3.Config)
		want   string
	}{
		"half a credential pair": {func(c *s3.Config) { c.SecretAccessKey = "" }, "go together"},
		"plaintext endpoint":     {func(c *s3.Config) { c.Endpoint = "http://s3.internal:9000" }, "S3_INSECURE=true"},
		"endpoint without host":  {func(c *s3.Config) { c.Endpoint = "s3.internal" }, "must be a URL"},
		"endpoint scheme":        {func(c *s3.Config) { c.Endpoint = "ftp://s3.internal" }, "scheme must be https"},
		"missing CA file":        {func(c *s3.Config) { c.CAFile = missingCA }, "S3_CA_FILE"},
		"negative timeout":       {func(c *s3.Config) { c.OpTimeout = -time.Second }, "must be positive"},
		"no attempts":            {func(c *s3.Config) { c.MaxAttempts = -1 }, "S3_MAX_ATTEMPTS"},
		"no connections":         {func(c *s3.Config) { c.MaxConnsPerHost = -1 }, "S3_MAX_CONNS_PER_HOST"},
		"part below S3 minimum":  {func(c *s3.Config) { c.UploadPartBytes = 1 << 20 }, "S3_UPLOAD_PART_BYTES"},
		"unknown checksum mode":  {func(c *s3.Config) { c.RequestChecksum = "always" }, "S3_REQUEST_CHECKSUM"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := valid()
			tc.mutate(&cfg)
			require.ErrorContains(t, cfg.Validate(), tc.want)
		})
	}

	insecure := valid()
	insecure.Endpoint = "http://localhost:9000"
	insecure.Insecure = true
	require.NoError(t, insecure.Validate())
}

func TestNewRejectsABadCAFile(t *testing.T) {
	t.Parallel()

	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))

	_, err := s3.New(context.Background(), s3.Config{
		Bucket: "b", Endpoint: "https://s3.internal", AccessKeyID: "k", SecretAccessKey: "s", CAFile: notPEM,
	}, nil)
	require.ErrorContains(t, err, "holds no PEM certificate")
}
