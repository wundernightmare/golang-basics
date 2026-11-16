package otelx

import (
	"errors"
	"fmt"
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
//	TASKS_OTEL_TRACES_SAMPLER_RATIO           head sampling ratio [0,1]; 0 samples no root (default 1.0)
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
// The zero Config is a valid one: [Init] reads a zero field as "not set" and
// applies the default above, whether the Config was built by hand or loaded.
// SamplerRatio is the exception — its zero is a setting, and its default is
// applied by [LoadConfig] and httpx.LoadYAML, not by Init: a Config built by
// hand names its ratio. [Config.Validate] says what Init refuses.
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

	// SamplerRatio is the share of root traces sampled, in [0,1]. Zero
	// samples no root — also in a Config built by hand, which gets no
	// default here: with the retroactive buffer on, a trace is then
	// exported only by a warn / error line or the debug token.
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

// ErrConfig is what every error about a configuration matches with
// [errors.Is]: a value [Config.Validate] refuses, an environment that does
// not parse, a retired key.
var ErrConfig = errors.New("otelx: config")

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
	if err := refuseRetiredKeys(prefix, environ); err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("%w: parse (prefix %q): %w", ErrConfig, prefix, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// RetiredKeys names the environment keys this config once read, each with
// what to set instead. [LoadConfig] refuses an environment that sets one
// under its prefix, and so does httpx.LoadYAML for a service that has this
// config as a section — it reads the keys of the struct, and cannot see one
// that is gone.
//
// OTEL_TRACES_TRUST_REMOTE_PARENT had the opposite sense of
// OTEL_TRACES_REDRAW_REMOTE_PARENT and the opposite default: an edge that
// still sets it to false would, were the key merely unknown, follow every
// client's sampled flag.
func (Config) RetiredKeys() map[string]string {
	return map[string]string{
		"OTEL_TRACES_TRUST_REMOTE_PARENT": "set OTEL_TRACES_REDRAW_REMOTE_PARENT to the opposite value " +
			"(true on an internet-facing edge) or remove the key",
	}
}

// refuseRetiredKeys fails when the environment (environ, or the process
// environment when it is nil) sets a key of [Config.RetiredKeys] under
// prefix, whatever its value.
func refuseRetiredKeys(prefix string, environ map[string]string) error {
	for name, instead := range (Config{}).RetiredKeys() {
		key := prefix + name

		_, set := environ[key]
		if environ == nil {
			_, set = os.LookupEnv(key)
		}

		if set {
			return fmt.Errorf("%w: %s is no longer read: %s", ErrConfig, key, instead)
		}
	}

	return nil
}

// Validate reports every value [Init] refuses, by the name of its
// environment key, in an error that matches [ErrConfig]. With export on over
// TLS it reads the certificate files, as Init will. A zero field passes.
func (c Config) Validate() error {
	var errs []error

	if !validEndpoint(c.Endpoint) {
		// The value is not quoted: a URL may carry credentials.
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT must be host:port, with the port and without a scheme or a path "+
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

	// Written so that NaN, which compares false with everything, is refused.
	if !(c.SamplerRatio >= 0 && c.SamplerRatio <= 1) {
		errs = append(errs, fmt.Errorf("OTEL_TRACES_SAMPLER_RATIO must be within [0, 1], got %v", c.SamplerRatio))
	}

	if c.Retro.TriggersPerSecond < 0 {
		errs = append(errs, fmt.Errorf("OTEL_RETRO_TRIGGERS_PER_SECOND must not be negative, got %d "+
			"(OTEL_RETRO_BUFFER_MIB < 0 turns the buffer off)", c.Retro.TriggersPerSecond))
	}

	if c.Enabled && !c.Insecure {
		if _, err := tlsConfig(c); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}

	return nil
}

// validEndpoint reports whether endpoint is a host with a port and nothing
// else: the exporter puts a scheme in front of it and a path behind, and
// without a port would dial 80 or 443, where no OTLP receiver listens by
// default.
func validEndpoint(endpoint string) bool {
	if endpoint == "" {
		return true
	}

	u, err := url.Parse("//" + endpoint)

	return err == nil && u.Host == endpoint && u.Port() != "" && !strings.Contains(endpoint, "://")
}

// withDefaults fills the zero fields of a Config whose zero is "not set".
func (c *Config) withDefaults() {
	if c.Endpoint == "" {
		c.Endpoint = "localhost:4318"
	}

	if c.ExportTimeout == 0 {
		c.ExportTimeout = 10 * time.Second
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
