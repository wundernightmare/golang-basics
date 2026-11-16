package s3_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/s3"
	"github.com/wundernightmare/golang-basics/libs/secret"
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
	assert.False(t, cfg.VirtualHostStyle)
	assert.False(t, cfg.Insecure)
	assert.Equal(t, 10*time.Second, cfg.OpTimeout)
	assert.Equal(t, time.Minute, cfg.TransferTimeout)
	assert.Equal(t, 3, cfg.MaxAttempts)
	assert.Equal(t, int64(16<<20), cfg.UploadPartBytes)
	assert.Equal(t, s3.ChecksumWhenRequired, cfg.RequestChecksum)

	_, err = s3.New(context.Background(), cfg, nil)
	require.ErrorContains(t, err, "S3_BUCKET must be set")

	_, err = s3.LoadConfigFrom("INGEST_", map[string]string{"INGEST_S3_MAX_ATTEMPTS": "many"})
	require.ErrorIs(t, err, s3.ErrConfig, "a value that does not parse")

	_, err = s3.LoadConfigFrom("INGEST_", map[string]string{"INGEST_S3_BUCKET": "b", "INGEST_S3_ENDPOINT": "http://s3.internal:9000"})
	require.ErrorIs(t, err, s3.ErrConfig, "a configured store is validated when it is loaded")

	_, err = s3.LoadConfigFrom("INGEST_", map[string]string{"INGEST_S3_USE_PATH_STYLE": "false"})
	require.ErrorContains(t, err, "INGEST_S3_USE_PATH_STYLE is no longer read", "the retired key is refused instead of being ignored")
	require.ErrorContains(t, err, "S3_VIRTUAL_HOST_STYLE")
	require.ErrorIs(t, err, s3.ErrConfig)
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := func() s3.Config {
		return s3.Config{Bucket: "b", Endpoint: "https://s3.internal:9000", AccessKeyID: "k", SecretAccessKey: secret.New("s")}
	}

	cfg := valid()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 10*time.Second, cfg.OpTimeout, "a hand-built config is defaulted")
	assert.Equal(t, int64(16<<20), cfg.UploadPartBytes)

	aws := s3.Config{Bucket: "b"}
	require.NoError(t, aws.Validate(), "no endpoint and no keys: AWS with the default credential chain")

	missingCA := filepath.Join(t.TempDir(), "absent.pem")
	notPEM := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))

	for name, tc := range map[string]struct {
		mutate func(*s3.Config)
		want   string
	}{
		"key id without secret":  {func(c *s3.Config) { c.SecretAccessKey = secret.New("") }, "go together"},
		"secret without key id":  {func(c *s3.Config) { c.AccessKeyID = "" }, "go together"},
		"plaintext endpoint":     {func(c *s3.Config) { c.Endpoint = "http://s3.internal:9000" }, "S3_INSECURE=true"},
		"endpoint without host":  {func(c *s3.Config) { c.Endpoint = "s3.internal" }, "must be a URL"},
		"endpoint scheme":        {func(c *s3.Config) { c.Endpoint = "ftp://s3.internal" }, "scheme must be https"},
		"missing CA file":        {func(c *s3.Config) { c.CAFile = missingCA }, "S3_CA_FILE"},
		"CA file is a directory": {func(c *s3.Config) { c.CAFile = filepath.Dir(missingCA) }, "S3_CA_FILE"},
		"CA file without a cert": {func(c *s3.Config) { c.CAFile = notPEM }, "S3_CA_FILE: " + notPEM + " holds no PEM certificate"},
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
			err := cfg.Validate()
			require.ErrorContains(t, err, tc.want)
			require.ErrorIs(t, err, s3.ErrConfig)
		})
	}

	insecure := valid()
	insecure.Endpoint = "http://localhost:9000"
	insecure.Insecure = true
	require.NoError(t, insecure.Validate())
}
