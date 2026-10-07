package otelx

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
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
//	TASKS_OTEL_SERVICE_VERSION                resource service.version                   (default: httpx.Version)
//	TASKS_OTEL_DEPLOYMENT_ENVIRONMENT         resource deployment.environment.name       (default "")
//	TASKS_OTEL_EXPORTER_OTLP_ENDPOINT         collector host:port, OTLP over HTTP        (default "localhost:4318")
//	TASKS_OTEL_EXPORTER_OTLP_INSECURE         plaintext HTTP to it instead of TLS        (default false)
//	TASKS_OTEL_EXPORTER_OTLP_HEADERS          k=v,k=v headers (auth tokens, tenant ids)  (default none)
//	TASKS_OTEL_EXPORTER_OTLP_CERTIFICATE      CA bundle (PEM) that signs the collector   (default: system roots)
//	TASKS_OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE  client certificate (PEM) for mTLS       (default none)
//	TASKS_OTEL_EXPORTER_OTLP_CLIENT_KEY       client key (PEM) for mTLS                  (default none)
//	TASKS_OTEL_EXPORTER_OTLP_TIMEOUT          per-export request timeout                 (default "10s")
//	TASKS_OTEL_TRACES_SAMPLER_RATIO           head sampling ratio (0,1]; negative = never sample (default 1.0)
//	TASKS_OTEL_TRACES_TRUST_REMOTE_PARENT     honour a caller's sampled flag             (default true)
//	TASKS_OTEL_BSP_SCHEDULE_DELAY             batch flush interval                       (default "5s")
//	TASKS_OTEL_BSP_MAX_QUEUE_SIZE             spans buffered before dropping             (default 2048)
//	TASKS_OTEL_BSP_MAX_EXPORT_BATCH_SIZE      spans per export request                   (default 512)
//
// Export is TLS by default: the collector is one hop away in a cluster and
// still on a network you do not own. Set INSECURE only for a local collector.
// TrustRemoteParent is the OpenTelemetry default (a sampled caller keeps its
// trace intact through this service); turn it off on the internet-facing
// edge, where an untrusted client could otherwise force sampling.
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

	SamplerRatio      float64 `env:"OTEL_TRACES_SAMPLER_RATIO" envDefault:"1.0" yaml:"sampler_ratio"`
	TrustRemoteParent bool    `env:"OTEL_TRACES_TRUST_REMOTE_PARENT" envDefault:"true" yaml:"trust_remote_parent"`

	BatchTimeout       time.Duration `env:"OTEL_BSP_SCHEDULE_DELAY" envDefault:"5s" yaml:"batch_timeout"`
	MaxQueueSize       int           `env:"OTEL_BSP_MAX_QUEUE_SIZE" envDefault:"2048" yaml:"max_queue_size"`
	MaxExportBatchSize int           `env:"OTEL_BSP_MAX_EXPORT_BATCH_SIZE" envDefault:"512" yaml:"max_export_batch_size"`
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field is defaulted; tracing export is off
// unless OTEL_ENABLED is set.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("otelx: parse config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}

// withDefaults fills the zero fields of a hand-built Config.
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
