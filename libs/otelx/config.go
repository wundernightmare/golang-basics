package otelx

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tracehubmmp/golang-basics/libs/otelx/retro"
)

// Config is the tracing configuration, populated from the environment with a
// per-service prefix (e.g. "TASKS_"). The keys follow the OpenTelemetry
// environment-variable conventions (with the prefix in front) so they read
// the same way as the standard ones in a deployment.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_OTEL_ENABLED                        turn tracing export on/off                 (default false)
//	TASKS_OTEL_SERVICE_NAME                   resource service.name                      (default: the service sets it)
//	TASKS_OTEL_SERVICE_VERSION                resource service.version                   (default: the service sets it)
//	TASKS_OTEL_DEPLOYMENT_ENVIRONMENT         resource deployment.environment.name       (default "")
//	TASKS_OTEL_EXPORTER_OTLP_ENDPOINT         collector host:port, OTLP over HTTP        (default "localhost:4318")
//	TASKS_OTEL_EXPORTER_OTLP_INSECURE         plaintext HTTP to it instead of TLS        (default false)
//	TASKS_OTEL_EXPORTER_OTLP_HEADERS          k=v,k=v headers (auth tokens, tenant ids)  (default none)
//	TASKS_OTEL_EXPORTER_OTLP_CERTIFICATE      CA bundle (PEM) that signs the collector   (default: system roots)
//	TASKS_OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE  client certificate (PEM) for mTLS       (default none)
//	TASKS_OTEL_EXPORTER_OTLP_CLIENT_KEY       client key (PEM) for mTLS                  (default none)
//	TASKS_OTEL_EXPORTER_OTLP_TIMEOUT          per-export request timeout                 (default "10s")
//	TASKS_OTEL_TRACES_SAMPLER_RATIO           head sampling ratio (0,1]; negative = never sample; 0 = not set (default 1.0)
//	TASKS_OTEL_TRACES_REDRAW_REMOTE_PARENT    ignore a caller's sampled flag: the edge   (default false)
//	TASKS_OTEL_RETRO_BUFFER_MIB               retroactive buffer, MiB; negative = off    (default 16)
//	TASKS_OTEL_RETRO_TRIGGERS_PER_SECOND      traces a warn/error line may export a second (default 50)
//	TASKS_OTEL_BSP_SCHEDULE_DELAY             batch flush interval                       (default "5s")
//	TASKS_OTEL_BSP_MAX_QUEUE_SIZE             spans buffered before dropping             (default 2048)
//	TASKS_OTEL_BSP_MAX_EXPORT_BATCH_SIZE      spans per export request                   (default 512)
//
// Export is TLS by default: the collector is one hop away in a cluster and
// still on a network you do not own. Set INSECURE only for a local collector.
//
// The zero Config is a valid one: [Init] reads every zero field as "not set"
// and applies the default above, whether the Config was built by hand or
// loaded. [Config.Validate] says what Init refuses.
type Config struct {
	Enabled     bool   `env:"OTEL_ENABLED" envDefault:"false" yaml:"enabled"`
	ServiceName string `env:"OTEL_SERVICE_NAME" yaml:"service_name"`
	Version     string `env:"OTEL_SERVICE_VERSION" yaml:"service_version"`
	Environment string `env:"OTEL_DEPLOYMENT_ENVIRONMENT" yaml:"deployment_environment"`

	Endpoint      string            `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4318" yaml:"endpoint"`
	Insecure      bool              `env:"OTEL_EXPORTER_OTLP_INSECURE" envDefault:"false" yaml:"insecure"`
	Headers       map[string]string `env:"OTEL_EXPORTER_OTLP_HEADERS" envKeyValSeparator:"=" yaml:"headers" secret:"true"`
	CACert        string            `env:"OTEL_EXPORTER_OTLP_CERTIFICATE" yaml:"ca_certificate"`
	ClientCert    string            `env:"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE" yaml:"client_certificate"`
	ClientKey     string            `env:"OTEL_EXPORTER_OTLP_CLIENT_KEY" yaml:"client_key"`
	ExportTimeout time.Duration     `env:"OTEL_EXPORTER_OTLP_TIMEOUT" envDefault:"10s" yaml:"export_timeout"`

	// SamplerRatio is the share of root traces sampled, in (0,1]. A negative
	// value samples no root. Zero is "not set" and means 1.0 — written in
	// the environment or in YAML too: Init replaces it, so the loaded Config
	// (and /admin/config) shows 0 where the "tracing enabled" line shows 1.
	SamplerRatio float64 `env:"OTEL_TRACES_SAMPLER_RATIO" envDefault:"1.0" yaml:"sampler_ratio"`
	// RedrawRemoteParent is for the internet-facing edge, where the caller
	// is a client that could otherwise have every request of its own traced
	// through the whole pipeline: the sampled flag of a remote parent is
	// ignored — the decision is drawn at SamplerRatio — and its tracestate
	// is dropped. Off, a remote parent is followed like a local one, which
	// is what every service behind the edge needs.
	RedrawRemoteParent bool `env:"OTEL_TRACES_REDRAW_REMOTE_PARENT" envDefault:"false" yaml:"redraw_remote_parent"`

	// Retro is the retroactive sampling buffer (libs/otelx/retro): the spans
	// of unsampled traces wait in it, and a warn / error log line exports
	// its trace. On by default; BufferMiB < 0 turns it off.
	Retro retro.Config `envPrefix:"OTEL_RETRO_" yaml:"retro"`

	BatchTimeout       time.Duration `env:"OTEL_BSP_SCHEDULE_DELAY" envDefault:"5s" yaml:"batch_timeout"`
	MaxQueueSize       int           `env:"OTEL_BSP_MAX_QUEUE_SIZE" envDefault:"2048" yaml:"max_queue_size"`
	MaxExportBatchSize int           `env:"OTEL_BSP_MAX_EXPORT_BATCH_SIZE" envDefault:"512" yaml:"max_export_batch_size"`
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix) and validates it. Every field is defaulted; tracing
// export is off unless OTEL_ENABLED is set. An environment that still sets
// OTEL_TRACES_TRUST_REMOTE_PARENT is refused: the key is no longer read, and
// its replacement, OTEL_TRACES_REDRAW_REMOTE_PARENT, means the opposite.
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	if err := refuseRetiredKey(prefix, environ); err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("otelx: parse config (prefix %q): %w", prefix, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// retiredTrustRemoteParent had the opposite sense of
// OTEL_TRACES_REDRAW_REMOTE_PARENT and the opposite default: an edge that
// still sets it to false would, were the key merely unknown, follow every
// client's sampled flag.
const retiredTrustRemoteParent = "OTEL_TRACES_TRUST_REMOTE_PARENT"

// refuseRetiredKey fails when the environment still sets
// [retiredTrustRemoteParent], whatever its value.
func refuseRetiredKey(prefix string, environ map[string]string) error {
	key := prefix + retiredTrustRemoteParent

	_, set := environ[key]
	if environ == nil {
		_, set = os.LookupEnv(key)
	}

	if !set {
		return nil
	}

	return fmt.Errorf("otelx: %s is no longer read: set %sOTEL_TRACES_REDRAW_REMOTE_PARENT to the opposite value "+
		"(true on an internet-facing edge) or remove the key", key, prefix)
}

// Validate reports every value [Init] refuses, by the name of its
// environment key. With export on over TLS it reads the certificate files,
// as Init will. A zero field is "not set" and passes.
func (c Config) Validate() error {
	var errs []error

	if !validEndpoint(c.Endpoint) {
		// The value is not quoted: a URL may carry credentials.
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT must be host:port, without a scheme or a path "+
			"(OTEL_EXPORTER_OTLP_INSECURE chooses plaintext or TLS, the path is /v1/traces)"))
	}

	for _, d := range []struct {
		key   string
		value time.Duration
	}{
		{"OTEL_EXPORTER_OTLP_TIMEOUT", c.ExportTimeout},
		{"OTEL_BSP_SCHEDULE_DELAY", c.BatchTimeout},
	} {
		if d.value < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative, got %s", d.key, d.value))
		}
	}

	for _, n := range []struct {
		key   string
		value int
	}{
		{"OTEL_BSP_MAX_QUEUE_SIZE", c.MaxQueueSize},
		{"OTEL_BSP_MAX_EXPORT_BATCH_SIZE", c.MaxExportBatchSize},
	} {
		if n.value < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative, got %d", n.key, n.value))
		}
	}

	if math.IsNaN(c.SamplerRatio) {
		errs = append(errs, errors.New("OTEL_TRACES_SAMPLER_RATIO must be a number"))
	}

	if c.Enabled && !c.Insecure {
		if _, err := tlsConfig(c); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("otelx: config: %w", err)
	}

	return nil
}

// validEndpoint reports whether endpoint is a host with an optional port and
// nothing else: the exporter puts a scheme in front of it and a path behind.
func validEndpoint(endpoint string) bool {
	if endpoint == "" {
		return true
	}

	u, err := url.Parse("//" + endpoint)

	return err == nil && u.Host == endpoint && !strings.Contains(endpoint, "://")
}

// withDefaults fills the zero fields of a Config.
func (c *Config) withDefaults() {
	if c.Endpoint == "" {
		c.Endpoint = "localhost:4318"
	}

	if c.ExportTimeout == 0 {
		c.ExportTimeout = 10 * time.Second
	}

	if c.SamplerRatio == 0 {
		c.SamplerRatio = 1.0
	}

	if c.BatchTimeout == 0 {
		c.BatchTimeout = 5 * time.Second
	}

	if c.MaxQueueSize == 0 {
		c.MaxQueueSize = 2048
	}

	if c.MaxExportBatchSize == 0 {
		c.MaxExportBatchSize = 512
	}
}
