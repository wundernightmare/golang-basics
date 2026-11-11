package s3

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the S3 client configuration, populated from the environment with
// a per-service prefix (e.g. "INGEST_"), or from YAML when a service embeds
// it in its own config (field `S3 s3.Config` tagged yaml:"s3", no envPrefix:
// the keys already carry S3_).
//
// There is no default bucket, so a service can treat object storage as
// optional — on when S3_BUCKET is set (see [Config.Configured]).
//
// Keys (with the "INGEST_" prefix as an example):
//
//	INGEST_S3_BUCKET              [bucket]              bucket every call addresses                    (default "": off)
//	INGEST_S3_ENDPOINT            [endpoint]            base URL of an S3-compatible store; "" = AWS   (default "")
//	INGEST_S3_REGION              [region]              signing region                                 (default "us-east-1")
//	INGEST_S3_ACCESS_KEY_ID       [access_key_id]       static credentials; "" = the AWS default chain (default "")
//	INGEST_S3_SECRET_ACCESS_KEY   [secret_access_key]   …                                              (default "")
//	INGEST_S3_USE_PATH_STYLE      [use_path_style]      endpoint/bucket/key instead of bucket.endpoint (default true)
//	INGEST_S3_INSECURE            [insecure]            allow an http:// endpoint                      (default false)
//	INGEST_S3_CA_FILE             [ca_file]             PEM bundle that signs the endpoint's cert      (default "": system roots)
//	INGEST_S3_AUTO_CREATE_BUCKET  [auto_create_bucket]  create the bucket at boot when it is missing   (default false)
//
// Deadlines and retries — every call carries its own deadline, a request
// context is not one:
//
//	INGEST_S3_DIAL_TIMEOUT        [dial_timeout]        TCP + TLS connect                              (default "5s")
//	INGEST_S3_OP_TIMEOUT          [op_timeout]          one put/head/list page/delete, retries included (default "10s")
//	INGEST_S3_TRANSFER_TIMEOUT    [transfer_timeout]    one get (body included) or one multipart part  (default "60s")
//	INGEST_S3_MAX_ATTEMPTS        [max_attempts]        tries per call inside its deadline (1 = none)  (default 3)
//	INGEST_S3_RETRY_MAX_BACKOFF   [retry_max_backoff]   cap of the jittered backoff between tries      (default "2s")
//
// Memory and connections — bounded by named keys, never a client default:
//
//	INGEST_S3_MAX_CONNS_PER_HOST  [max_conns_per_host]  connections to the endpoint                    (default 32)
//	INGEST_S3_UPLOAD_PART_BYTES   [upload_part_bytes]   buffer of one multipart part (min 5 MiB)       (default 16777216)
//
// Compatibility:
//
//	INGEST_S3_REQUEST_CHECKSUM    [request_checksum]    when_required|when_supported                   (default "when_required")
//
// when_supported is the AWS SDK's own default (a CRC32 trailer on every
// upload); several S3-compatible stores reject it, so the default here is the
// conservative one. Uploaded bytes are still covered by the request signature
// over TLS and by the store's ETag.
type Config struct {
	Bucket           string `env:"S3_BUCKET" yaml:"bucket"`
	Endpoint         string `env:"S3_ENDPOINT" yaml:"endpoint"`
	Region           string `env:"S3_REGION" envDefault:"us-east-1" yaml:"region"`
	AccessKeyID      string `env:"S3_ACCESS_KEY_ID" yaml:"access_key_id"`
	SecretAccessKey  string `env:"S3_SECRET_ACCESS_KEY" secret:"true" yaml:"secret_access_key"`
	UsePathStyle     bool   `env:"S3_USE_PATH_STYLE" envDefault:"true" yaml:"use_path_style"`
	Insecure         bool   `env:"S3_INSECURE" envDefault:"false" yaml:"insecure"`
	CAFile           string `env:"S3_CA_FILE" yaml:"ca_file"`
	AutoCreateBucket bool   `env:"S3_AUTO_CREATE_BUCKET" envDefault:"false" yaml:"auto_create_bucket"`

	DialTimeout time.Duration `env:"S3_DIAL_TIMEOUT" envDefault:"5s" yaml:"dial_timeout"`
	// OpTimeout bounds one small call — put, head, one list page, one delete
	// batch — with the SDK's retries inside it. It is what an acknowledgement
	// that waits on a put can take at worst.
	OpTimeout time.Duration `env:"S3_OP_TIMEOUT" envDefault:"10s" yaml:"op_timeout"`
	// TransferTimeout bounds a call that moves a large body: one get (the
	// body is read inside it) and one multipart part.
	TransferTimeout time.Duration `env:"S3_TRANSFER_TIMEOUT" envDefault:"60s" yaml:"transfer_timeout"`
	MaxAttempts     int           `env:"S3_MAX_ATTEMPTS" envDefault:"3" yaml:"max_attempts"`
	RetryMaxBackoff time.Duration `env:"S3_RETRY_MAX_BACKOFF" envDefault:"2s" yaml:"retry_max_backoff"`

	MaxConnsPerHost int   `env:"S3_MAX_CONNS_PER_HOST" envDefault:"32" yaml:"max_conns_per_host"`
	UploadPartBytes int64 `env:"S3_UPLOAD_PART_BYTES" envDefault:"16777216" yaml:"upload_part_bytes"`

	RequestChecksum string `env:"S3_REQUEST_CHECKSUM" envDefault:"when_required" yaml:"request_checksum"`
}

// Defaults applied by [Config.Validate] (and [New]) to zero fields, so a
// hand-built Config needs only what differs.
const (
	defaultRegion          = "us-east-1"
	defaultDialTimeout     = 5 * time.Second
	defaultOpTimeout       = 10 * time.Second
	defaultTransferTimeout = 60 * time.Second
	defaultMaxAttempts     = 3
	defaultRetryMaxBackoff = 2 * time.Second
	defaultMaxConnsPerHost = 32
	defaultUploadPartBytes = 16 << 20

	// MinUploadPartBytes is the smallest multipart part S3 accepts (the last
	// part of an upload excepted).
	MinUploadPartBytes = 5 << 20

	// ChecksumWhenRequired / ChecksumWhenSupported are the S3_REQUEST_CHECKSUM values.
	ChecksumWhenRequired  = "when_required"
	ChecksumWhenSupported = "when_supported"
)

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field but the bucket, the endpoint and the
// credentials is defaulted.
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("s3: parse config (prefix %q): %w", prefix, err)
	}

	return cfg, nil
}

// Configured reports whether a bucket is set: the switch for a service that
// runs without object storage when there is none.
func (c Config) Configured() bool { return c.Bucket != "" }

func (c *Config) withDefaults() {
	if c.Region == "" {
		c.Region = defaultRegion
	}

	if c.RequestChecksum == "" {
		c.RequestChecksum = ChecksumWhenRequired
	}

	setDuration(&c.DialTimeout, defaultDialTimeout)
	setDuration(&c.OpTimeout, defaultOpTimeout)
	setDuration(&c.TransferTimeout, defaultTransferTimeout)
	setDuration(&c.RetryMaxBackoff, defaultRetryMaxBackoff)

	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultMaxAttempts
	}

	if c.MaxConnsPerHost == 0 {
		c.MaxConnsPerHost = defaultMaxConnsPerHost
	}

	if c.UploadPartBytes == 0 {
		c.UploadPartBytes = defaultUploadPartBytes
	}
}

func setDuration(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	}
}

// Validate applies the defaults and rejects a config the client could not
// work with, or could only work with unsafely: no bucket, half a credential
// pair, a plaintext endpoint that was not asked for, an unreadable CA bundle,
// non-positive budgets, a part size S3 refuses.
func (c *Config) Validate() error {
	c.withDefaults()

	var errs []error
	if c.Bucket == "" {
		errs = append(errs, errors.New("S3_BUCKET must be set"))
	}

	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") {
		errs = append(errs, errors.New("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY go together: set both, or neither for the AWS default credential chain"))
	}

	errs = append(errs, c.validateEndpoint()...)

	if c.CAFile != "" {
		if _, err := os.Stat(c.CAFile); err != nil {
			errs = append(errs, fmt.Errorf("S3_CA_FILE: %w", err))
		}
	}

	if c.DialTimeout < 0 || c.OpTimeout < 0 || c.TransferTimeout < 0 || c.RetryMaxBackoff < 0 {
		errs = append(errs, errors.New("S3_DIAL_TIMEOUT, S3_OP_TIMEOUT, S3_TRANSFER_TIMEOUT and S3_RETRY_MAX_BACKOFF must be positive"))
	}

	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("S3_MAX_ATTEMPTS must be at least 1, got %d", c.MaxAttempts))
	}

	if c.MaxConnsPerHost < 1 {
		errs = append(errs, fmt.Errorf("S3_MAX_CONNS_PER_HOST must be at least 1, got %d", c.MaxConnsPerHost))
	}

	if c.UploadPartBytes < MinUploadPartBytes {
		errs = append(errs, fmt.Errorf("S3_UPLOAD_PART_BYTES must be at least %d (S3's minimum part), got %d", MinUploadPartBytes, c.UploadPartBytes))
	}

	if c.RequestChecksum != ChecksumWhenRequired && c.RequestChecksum != ChecksumWhenSupported {
		errs = append(errs, fmt.Errorf("S3_REQUEST_CHECKSUM must be %s or %s, got %q", ChecksumWhenRequired, ChecksumWhenSupported, c.RequestChecksum))
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("s3 config: %w", err)
	}

	return nil
}

func (c *Config) validateEndpoint() []error {
	if c.Endpoint == "" {
		return nil // AWS: the SDK resolves the regional HTTPS endpoint
	}

	endpoint, err := url.Parse(c.Endpoint)
	if err != nil || endpoint.Host == "" {
		return []error{fmt.Errorf("S3_ENDPOINT must be a URL like https://s3.internal:9000, got %q", c.Endpoint)}
	}

	switch endpoint.Scheme {
	case "https":
		return nil
	case "http":
		if !c.Insecure {
			return []error{errors.New("S3_ENDPOINT is plaintext http://: credentials and data would cross the network unencrypted — use https://, or set S3_INSECURE=true for a local store")}
		}

		return nil
	default:
		return []error{fmt.Errorf("S3_ENDPOINT scheme must be https (or http with S3_INSECURE=true), got %q", endpoint.Scheme)}
	}
}
