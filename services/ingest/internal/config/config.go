// Package config loads the ingest service's settings from an optional YAML
// file overlaid with INGEST_-prefixed environment variables (see
// [httpx.LoadYAML]: env > YAML > default). It embeds the shared libs' own
// Config structs rather than copying their fields, so every knob a lib offers
// is configurable here under the key the lib documents. Every timeout, batch
// size and limit of the write path is a key — nothing is a constant in code.
package config

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

// EnvPrefix is the prefix of every environment key.
const EnvPrefix = "INGEST_"

// The two roles one binary runs in (INGEST_MODE).
const (
	// ModeIngest serves POST /events: the stateless pods behind the ingress.
	ModeIngest = "ingest"
	// ModeCompact merges closed hours of the archive: one replica, no API
	// listener.
	ModeCompact = "compact"
)

// ContractMaxBatchEvents is maxItems of IngestRequest.events in
// api/tsp/ingest.tsp: MAX_BATCH_EVENTS may lower it, never raise it.
const ContractMaxBatchEvents = 1000

// Config is the ingest service configuration.
//
// S3 is required in both modes (INGEST_S3_BUCKET). Kafka is optional and on
// when INGEST_KAFKA_BROKERS is set: without it every batch is answered by the
// archive — the footprint for bringing ingestion back before the brokers.
//
//	mode: ingest                 # or compact
//	http_addr: ":8084"           # httpx.Config, inline at the top level
//	s3:      {bucket: …}         # s3.Config
//	kafka:   {brokers: […]}      # kafka.Config (topic = where events go)
//	otel:    {enabled: true}     # otelx.Config
//	ingest:  {max_event_bytes: 65536}
//	archive: {linger: 10s}
//	compact: {interval: 5m}
type Config struct {
	httpx.Config `yaml:",inline"`

	// Mode is the role of this process.
	Mode string `env:"MODE" envDefault:"ingest" yaml:"mode"`

	S3    s3.Config    `yaml:"s3"`
	Kafka kafka.Config `yaml:"kafka"`
	OTel  otelx.Config `yaml:"otel"`

	Ingest  IngestConfig  `yaml:"ingest"`
	Archive ArchiveConfig `yaml:"archive"`
	Compact CompactConfig `yaml:"compact"`
}

// IngestConfig tunes the request path.
type IngestConfig struct {
	// MaxEventBytes is the largest single event (its JSON object) accepted.
	MaxEventBytes int `env:"MAX_EVENT_BYTES" envDefault:"65536" yaml:"max_event_bytes"`
	// MaxBatchEvents is the most events one request may carry (at most the
	// contract's 1000).
	MaxBatchEvents int `env:"MAX_BATCH_EVENTS" envDefault:"1000" yaml:"max_batch_events"`
	// MaxInFlightBytes bounds the event bytes held in memory — accepted, not
	// yet settled by both sinks. Past it requests get 429. Size the pod's
	// memory limit at about 3× this plus the Kafka send queue: a batch is
	// held raw, framed and compressed at the moment it is flushed.
	MaxInFlightBytes int64 `env:"MAX_IN_FLIGHT_BYTES" envDefault:"134217728" yaml:"max_in_flight_bytes"`
	// RetryAfter is the Retry-After a 429 / 503 carries (whole seconds).
	RetryAfter time.Duration `env:"RETRY_AFTER" envDefault:"1s" yaml:"retry_after"`
	// KafkaAckGrace is how long a request waits for Kafka alone before the
	// archive batch holding its events is flushed early. It bounds the
	// latency at the onset of a Kafka outage, before the circuit opens.
	KafkaAckGrace time.Duration `env:"KAFKA_ACK_GRACE" envDefault:"1s" yaml:"kafka_ack_grace"`
	// KafkaMaxInFlight bounds the events being sent to Kafka at once.
	KafkaMaxInFlight int `env:"KAFKA_MAX_IN_FLIGHT" envDefault:"10000" yaml:"kafka_max_in_flight"`
	// DrainTimeout is how long shutdown waits, after the HTTP server has
	// drained, for the second copies still in flight: the last archive
	// batch and the Kafka sends. terminationGracePeriodSeconds must exceed
	// HTTP_SHUTDOWN_DELAY + HTTP_SHUTDOWN_TIMEOUT + this.
	DrainTimeout time.Duration `env:"DRAIN_TIMEOUT" envDefault:"15s" yaml:"drain_timeout"`

	KafkaBreaker   BreakerConfig `envPrefix:"KAFKA_BREAKER_" yaml:"kafka_breaker"`
	ArchiveBreaker BreakerConfig `envPrefix:"ARCHIVE_BREAKER_" yaml:"archive_breaker"`
}

// BreakerConfig is one sink's circuit breaker (libs/resilience).
// The two sinks see very different call rates — Kafka one call per request,
// the archive one per batch — so each has its own; the presets differ.
type BreakerConfig struct {
	// FailureRatio opens the circuit when failures / calls in Window reach it.
	FailureRatio float64 `env:"FAILURE_RATIO" envDefault:"0.5" yaml:"failure_ratio"`
	// MinRequests is the least calls in Window before the ratio counts.
	MinRequests int `env:"MIN_REQUESTS" envDefault:"10" yaml:"min_requests"`
	// Window is the sliding window the ratio is measured over.
	Window time.Duration `env:"WINDOW" envDefault:"10s" yaml:"window"`
	// OpenTimeout is how long the circuit stays open before a probe.
	OpenTimeout time.Duration `env:"OPEN_TIMEOUT" envDefault:"5s" yaml:"open_timeout"`
	// HalfOpenProbes is how many probes must succeed to close it.
	HalfOpenProbes int `env:"HALF_OPEN_PROBES" envDefault:"1" yaml:"half_open_probes"`
}

// Resilient converts to the breaker's own config.
func (b BreakerConfig) Resilient() resilience.BreakerConfig {
	return resilience.BreakerConfig{
		FailureRatio: b.FailureRatio, MinRequests: b.MinRequests, Window: b.Window,
		OpenTimeout: b.OpenTimeout, HalfOpenProbes: b.HalfOpenProbes,
	}
}

// validate requires a breaker: a sink without one would be retried by every
// request for its full timeout while it is down.
func (b BreakerConfig) validate() error {
	if b.FailureRatio <= 0 {
		return errors.New("failure_ratio must be in (0, 1]")
	}

	return errors.Join(b.Resilient().Validate()...)
}

// ArchiveConfig tunes how events become S3 objects.
type ArchiveConfig struct {
	// Prefix is the key prefix of everything the service writes.
	Prefix string `env:"ARCHIVE_PREFIX" envDefault:"events/" yaml:"prefix"`
	// BatchBytes flushes a batch once its uncompressed size reaches it.
	BatchBytes int `env:"ARCHIVE_BATCH_BYTES" envDefault:"8388608" yaml:"batch_bytes"`
	// Linger is how long a batch waits for more events while Kafka is
	// answering the requests: it sets the object count of normal operation
	// (one object per pod per Linger).
	Linger time.Duration `env:"ARCHIVE_LINGER" envDefault:"10s" yaml:"linger"`
	// UrgentLinger is the wait once a request depends on the archive for its
	// answer: the latency a client pays, and the object rate (one per pod
	// per UrgentLinger), while Kafka is away.
	UrgentLinger time.Duration `env:"ARCHIVE_URGENT_LINGER" envDefault:"1s" yaml:"urgent_linger"`
	// MaxConcurrentFlushes bounds the puts in flight per pod.
	MaxConcurrentFlushes int `env:"ARCHIVE_MAX_CONCURRENT_FLUSHES" envDefault:"4" yaml:"max_concurrent_flushes"`
	// Compression of the raw objects: fastest|default|better|best (zstd).
	Compression string `env:"ARCHIVE_COMPRESSION" envDefault:"default" yaml:"compression"`
}

// CompactConfig tunes compaction (MODE=compact).
type CompactConfig struct {
	// Interval between passes over the archive.
	Interval time.Duration `env:"COMPACT_INTERVAL" envDefault:"5m" yaml:"interval"`
	// RunOnce makes the process exit after one pass (a Kubernetes CronJob)
	// instead of looping (a one-replica Deployment that is scraped).
	RunOnce bool `env:"COMPACT_RUN_ONCE" envDefault:"false" yaml:"run_once"`
	// CloseDelay is how long after an hour ends it is considered closed:
	// longer than any put that started inside the hour can take
	// (S3_OP_TIMEOUT) plus the clock skew between pods.
	CloseDelay time.Duration `env:"COMPACT_CLOSE_DELAY" envDefault:"15m" yaml:"close_delay"`
	// PartBytes is the target compressed size of a compacted object.
	PartBytes int64 `env:"COMPACT_PART_BYTES" envDefault:"134217728" yaml:"part_bytes"`
	// FetchConcurrency is how many raw objects are downloaded and verified
	// at once; memory grows with it × MaxObjectBytes at worst.
	FetchConcurrency int `env:"COMPACT_FETCH_CONCURRENCY" envDefault:"4" yaml:"fetch_concurrency"`
	// MaxObjectBytes bounds one raw object, stored and decompressed; a
	// larger one is quarantined rather than read.
	MaxObjectBytes int64 `env:"COMPACT_MAX_OBJECT_BYTES" envDefault:"268435456" yaml:"max_object_bytes"`
	// Compression of the compacted objects: fastest|default|better|best.
	Compression string `env:"COMPACT_COMPRESSION" envDefault:"better" yaml:"compression"`
	// DeleteSources removes the raw objects of an hour once its manifest is
	// written. Off, they stay (twice the storage) until a lifecycle rule or
	// an operator removes them.
	DeleteSources bool `env:"COMPACT_DELETE_SOURCES" envDefault:"true" yaml:"delete_sources"`
	// HourTimeout bounds the compaction of one hour.
	HourTimeout time.Duration `env:"COMPACT_HOUR_TIMEOUT" envDefault:"30m" yaml:"hour_timeout"`
}

// preset is this service's own defaults (name, ports, client id, and the
// lib settings the write path's latency depends on).
func preset() Config {
	var cfg Config

	cfg.Service = "ingest"
	cfg.Addr = ":8084"
	cfg.AdminAddr = ":9084"
	cfg.Kafka.ClientID = "ingest"
	cfg.Kafka.Topic = "ingest.events"
	cfg.OTel.ServiceName = "ingest"
	cfg.OTel.Version = httpx.Version
	// A put is what an acknowledgement waits on while Kafka is away: fail it
	// in seconds and let the client retry rather than hold the request.
	cfg.S3.OpTimeout = 5 * time.Second
	// One archive call is a whole batch: two failed batches in a minute are
	// an outage, where ten failed Kafka requests in ten seconds are.
	cfg.Ingest.ArchiveBreaker = BreakerConfig{MinRequests: 2, Window: time.Minute, OpenTimeout: 10 * time.Second}

	return cfg
}

// Defaults is the configuration with nothing set: what --config-reference
// prints, and what [Config.Validate] compares with to tell a key that was
// set from one that was not.
func Defaults() (Config, error) {
	cfg := preset()
	err := httpx.Defaults(EnvPrefix, &cfg)

	return cfg, err
}

// Load reads the config from the YAML file at yamlPath (optional: a missing
// path means env-only), overlays INGEST_ environment variables and validates
// the result.
func Load(yamlPath string) (Config, error) { return LoadFrom(yamlPath, nil) }

// LoadFrom is [Load] reading environ instead of the process environment
// (nil means the process environment).
func LoadFrom(yamlPath string, environ map[string]string) (Config, error) {
	cfg := preset()
	if err := httpx.LoadYAMLFrom(yamlPath, EnvPrefix, environ, &cfg); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// KafkaEnabled reports whether events are published: brokers are set.
func (c Config) KafkaEnabled() bool { return len(c.Kafka.Brokers) > 0 }

// Validate checks the whole config: the HTTP server's rules, the libs', and
// the relations between this service's keys that would otherwise fail at
// three in the morning — an event the body limit cannot carry, a request
// deadline shorter than the wait it is supposed to cover.
func (c *Config) Validate() error {
	def, err := Defaults()
	if err != nil {
		return err
	}

	var errs []error

	switch c.Mode {
	case ModeIngest:
	case ModeCompact:
		c.Addr = "" // a worker: the admin listener only
	default:
		errs = append(errs, fmt.Errorf("MODE must be %s or %s, got %q", ModeIngest, ModeCompact, c.Mode))
	}

	if err := c.Config.Validate(); err != nil {
		errs = append(errs, err)
	}

	if err := c.S3.Validate(); err != nil {
		errs = append(errs, err)
	}

	errs = append(errs, c.validateKafka(def)...)
	errs = append(errs, c.validateIngest()...)
	errs = append(errs, c.validateArchive()...)
	errs = append(errs, c.validateCompact()...)

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}

	return nil
}

func (c *Config) validateKafka(def Config) []error {
	if c.Mode == ModeCompact {
		return nil // the compactor never talks to Kafka: a shared config file may still name it
	}

	if !c.KafkaEnabled() {
		k := c.Kafka

		k.Brokers = def.Kafka.Brokers // nil or [] alike
		if !reflect.DeepEqual(k, def.Kafka) {
			return []error{errors.New("KAFKA_* keys are set but KAFKA_BROKERS is not: set the brokers to publish events, or drop the keys")}
		}

		return nil
	}

	var errs []error
	if err := c.Kafka.Validate(); err != nil {
		errs = append(errs, err)
	}

	// Headers and the record framing take a little on top of the event.
	const recordOverhead = 512
	if c.Ingest.MaxEventBytes+recordOverhead > c.Kafka.MessageMaxBytes {
		errs = append(errs, fmt.Errorf("MAX_EVENT_BYTES (%d) does not fit KAFKA_MESSAGE_MAX_BYTES (%d): the brokers would reject the largest events",
			c.Ingest.MaxEventBytes, c.Kafka.MessageMaxBytes))
	}

	return errs
}

func (c *Config) validateIngest() []error {
	var errs []error

	ing := c.Ingest
	if ing.MaxEventBytes < 1 || int64(ing.MaxEventBytes) > c.MaxBodyBytes {
		errs = append(errs, fmt.Errorf("MAX_EVENT_BYTES must be 1..HTTP_MAX_BODY_BYTES (%d), got %d", c.MaxBodyBytes, ing.MaxEventBytes))
	}

	if ing.MaxBatchEvents < 1 || ing.MaxBatchEvents > ContractMaxBatchEvents {
		errs = append(errs, fmt.Errorf("MAX_BATCH_EVENTS must be 1..%d (the contract's limit), got %d", ContractMaxBatchEvents, ing.MaxBatchEvents))
	}

	if ing.MaxInFlightBytes < c.MaxBodyBytes {
		errs = append(errs, fmt.Errorf("MAX_IN_FLIGHT_BYTES (%d) must be at least HTTP_MAX_BODY_BYTES (%d): one request must fit", ing.MaxInFlightBytes, c.MaxBodyBytes))
	}

	if ing.KafkaMaxInFlight < ing.MaxBatchEvents {
		errs = append(errs, fmt.Errorf("KAFKA_MAX_IN_FLIGHT (%d) must be at least MAX_BATCH_EVENTS (%d): one request must fit", ing.KafkaMaxInFlight, ing.MaxBatchEvents))
	}

	if ing.RetryAfter < time.Second || ing.KafkaAckGrace <= 0 || ing.DrainTimeout <= 0 {
		errs = append(errs, errors.New("RETRY_AFTER must be at least 1s; KAFKA_ACK_GRACE and DRAIN_TIMEOUT must be positive"))
	}

	if err := ing.KafkaBreaker.validate(); err != nil {
		errs = append(errs, fmt.Errorf("KAFKA_BREAKER_*: %w", err))
	}

	if err := ing.ArchiveBreaker.validate(); err != nil {
		errs = append(errs, fmt.Errorf("ARCHIVE_BREAKER_*: %w", err))
	}

	return errs
}

func (c *Config) validateArchive() []error {
	var errs []error

	arc := c.Archive
	if arc.BatchBytes < 1 || arc.MaxConcurrentFlushes < 1 {
		errs = append(errs, errors.New("ARCHIVE_BATCH_BYTES and ARCHIVE_MAX_CONCURRENT_FLUSHES must be positive"))
	}

	if arc.UrgentLinger <= 0 || arc.Linger < arc.UrgentLinger {
		errs = append(errs, errors.New("ARCHIVE_URGENT_LINGER must be positive and at most ARCHIVE_LINGER"))
	}

	if _, err := shard.ParseLevel(arc.Compression); err != nil {
		errs = append(errs, fmt.Errorf("ARCHIVE_COMPRESSION: %w", err))
	}

	// The slowest answer the archive alone can give: Kafka's grace, the
	// urgent linger, then one put. A request deadline below that turns every
	// request of a Kafka outage into a 504 that the archive was about to
	// answer.
	if worst := c.Ingest.KafkaAckGrace + arc.UrgentLinger + c.S3.OpTimeout; c.Mode == ModeIngest && c.RequestTimeout > 0 && c.RequestTimeout <= worst {
		errs = append(errs, fmt.Errorf("HTTP_REQUEST_TIMEOUT (%s) must exceed KAFKA_ACK_GRACE + ARCHIVE_URGENT_LINGER + S3_OP_TIMEOUT (%s)", c.RequestTimeout, worst))
	}

	return errs
}

func (c *Config) validateCompact() []error {
	var errs []error

	co := c.Compact
	if co.Interval <= 0 || co.HourTimeout <= 0 {
		errs = append(errs, errors.New("COMPACT_INTERVAL and COMPACT_HOUR_TIMEOUT must be positive"))
	}

	if co.CloseDelay <= c.S3.OpTimeout {
		errs = append(errs, fmt.Errorf("COMPACT_CLOSE_DELAY (%s) must exceed S3_OP_TIMEOUT (%s): a put that started inside the hour must have ended", co.CloseDelay, c.S3.OpTimeout))
	}

	if co.PartBytes < 1 || co.FetchConcurrency < 1 || co.MaxObjectBytes < 1 {
		errs = append(errs, errors.New("COMPACT_PART_BYTES, COMPACT_FETCH_CONCURRENCY and COMPACT_MAX_OBJECT_BYTES must be positive"))
	}

	if _, err := shard.ParseLevel(co.Compression); err != nil {
		errs = append(errs, fmt.Errorf("COMPACT_COMPRESSION: %w", err))
	}

	return errs
}
