package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config is the Kafka client configuration, populated from the environment
// (and optionally YAML, via httpx.LoadYAML) with a per-service prefix (e.g.
// "TASKS_" for a producer, "CONSUMER_" for a consumer). The same struct
// serves both shapes: a [Producer] ignores the group and consumer fields;
// Topic is the default produce target and Topics the subscribe set.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_KAFKA_BROKERS                  comma-separated host:port seeds            (default "localhost:9092")
//	TASKS_KAFKA_TOPIC                    default produce topic                      (default "tasks.events")
//	TASKS_KAFKA_TOPICS                   comma-separated subscribe topics           (default: the Topic value)
//	TASKS_KAFKA_GROUP                    consumer group id                          (default "tasks-consumer")
//	TASKS_KAFKA_CLIENT_ID                client id advertised to brokers            (default "golang-basics")
//	TASKS_KAFKA_DIAL_TIMEOUT             broker dial timeout                        (default "10s")
//	TASKS_KAFKA_PUBLISH_TIMEOUT          deadline for one Publish                   (default "5s")
//	TASKS_KAFKA_ALLOW_AUTO_TOPIC_CREATION ask the broker to create a missing topic  (default false)
//
// Security (TLS and SASL are independent: SASL over TLS is SASL_SSL, SASL
// alone is SASL_PLAINTEXT, which sends a PLAIN password in the clear):
//
//	TASKS_KAFKA_TLS_ENABLED              dial the brokers over TLS                  (default false)
//	TASKS_KAFKA_TLS_CA_CERT              CA bundle file (PEM) for the brokers       (default: system roots)
//	TASKS_KAFKA_TLS_CLIENT_CERT          client certificate file (PEM) for mTLS     (default none)
//	TASKS_KAFKA_TLS_CLIENT_KEY           client key file (PEM) for mTLS             (default none)
//	TASKS_KAFKA_TLS_INSECURE_SKIP_VERIFY skip broker certificate verification       (default false; never in production)
//	TASKS_KAFKA_SASL_MECHANISM           ""|plain|scram-sha-256|scram-sha-512       (default "": no SASL)
//	TASKS_KAFKA_SASL_USERNAME            SASL user                                  (default "")
//	TASKS_KAFKA_SASL_PASSWORD            SASL password (secret)                     (default "")
//
// Consumer:
//
//	TASKS_KAFKA_LAG_INTERVAL             group-lag poll period, negative = off      (default "15s")
//	TASKS_KAFKA_CONSUMER_MAX_POLL_RECORDS records processed per batch              (default 100)
//	TASKS_KAFKA_CONSUMER_HANDLER_TIMEOUT deadline of one handler attempt           (default "30s")
//	TASKS_KAFKA_CONSUMER_MAX_RETRIES     retries after the first attempt, negative = none (default 5)
//	TASKS_KAFKA_CONSUMER_RETRY_BACKOFF   first retry delay, doubled per retry, jittered (default "200ms")
//	TASKS_KAFKA_CONSUMER_RETRY_BACKOFF_MAX cap on one retry delay                   (default "30s")
//	TASKS_KAFKA_DEAD_LETTER_TOPIC        where exhausted records go; "{topic}" is the
//	                                     source topic; "none" disables the DLQ     (default "{topic}.dlq")
//	TASKS_KAFKA_CONSUMER_DRAIN_TIMEOUT   budget to finish the in-flight batch on shutdown (default "10s")
//
// Zero means "unset" throughout (the defaults fill it), which is why the DLQ
// is disabled with the word "none" rather than an empty string and "no
// retries" is a negative number: see httpx.LoadYAML on explicit zeros.
type Config struct {
	Brokers                []string      `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092" yaml:"brokers"`
	Topic                  string        `env:"KAFKA_TOPIC" envDefault:"tasks.events" yaml:"topic"`
	Topics                 []string      `env:"KAFKA_TOPICS" envSeparator:"," yaml:"topics"`
	Group                  string        `env:"KAFKA_GROUP" envDefault:"tasks-consumer" yaml:"group"`
	ClientID               string        `env:"KAFKA_CLIENT_ID" envDefault:"golang-basics" yaml:"client_id"`
	DialTimeout            time.Duration `env:"KAFKA_DIAL_TIMEOUT" envDefault:"10s" yaml:"dial_timeout"`
	AllowAutoTopicCreation bool          `env:"KAFKA_ALLOW_AUTO_TOPIC_CREATION" envDefault:"false" yaml:"allow_auto_topic_creation"`
	// PublishTimeout bounds one Publish. A broker that stops answering (not
	// refusing — hanging) would otherwise hold the request for as long as the
	// caller's context lives, which for an HTTP handler is the whole request:
	// found by the chaos suite.
	PublishTimeout time.Duration `env:"KAFKA_PUBLISH_TIMEOUT" envDefault:"5s" yaml:"publish_timeout"`

	TLSEnabled            bool   `env:"KAFKA_TLS_ENABLED" envDefault:"false" yaml:"tls_enabled"`
	TLSCACert             string `env:"KAFKA_TLS_CA_CERT" yaml:"tls_ca_cert"`
	TLSClientCert         string `env:"KAFKA_TLS_CLIENT_CERT" yaml:"tls_client_cert"`
	TLSClientKey          string `env:"KAFKA_TLS_CLIENT_KEY" yaml:"tls_client_key"`
	TLSInsecureSkipVerify bool   `env:"KAFKA_TLS_INSECURE_SKIP_VERIFY" envDefault:"false" yaml:"tls_insecure_skip_verify"`

	SASLMechanism string `env:"KAFKA_SASL_MECHANISM" yaml:"sasl_mechanism"`
	SASLUsername  string `env:"KAFKA_SASL_USERNAME" yaml:"sasl_username"`
	SASLPassword  string `env:"KAFKA_SASL_PASSWORD" secret:"true" yaml:"sasl_password"`

	LagInterval     time.Duration `env:"KAFKA_LAG_INTERVAL" envDefault:"15s" yaml:"lag_interval"`
	MaxPollRecords  int           `env:"KAFKA_CONSUMER_MAX_POLL_RECORDS" envDefault:"100" yaml:"consumer_max_poll_records"`
	HandlerTimeout  time.Duration `env:"KAFKA_CONSUMER_HANDLER_TIMEOUT" envDefault:"30s" yaml:"consumer_handler_timeout"`
	MaxRetries      int           `env:"KAFKA_CONSUMER_MAX_RETRIES" envDefault:"5" yaml:"consumer_max_retries"`
	RetryBackoff    time.Duration `env:"KAFKA_CONSUMER_RETRY_BACKOFF" envDefault:"200ms" yaml:"consumer_retry_backoff"`
	RetryBackoffMax time.Duration `env:"KAFKA_CONSUMER_RETRY_BACKOFF_MAX" envDefault:"30s" yaml:"consumer_retry_backoff_max"`
	DeadLetterTopic string        `env:"KAFKA_DEAD_LETTER_TOPIC" envDefault:"{topic}.dlq" yaml:"dead_letter_topic"`
	DrainTimeout    time.Duration `env:"KAFKA_CONSUMER_DRAIN_TIMEOUT" envDefault:"10s" yaml:"consumer_drain_timeout"`
}

// DeadLetterDisabled is the [Config.DeadLetterTopic] value that turns the
// dead-letter topic off: a record that exhausts its retries then stops the
// consumer instead of being parked (see [Consumer.Run]).
const DeadLetterDisabled = "none"

// Defaults applied by [Config.Validate] (and the constructors) to zero
// fields of a hand-built Config. They mirror the envDefault tags.
const (
	defaultBroker          = "localhost:9092"
	defaultTopic           = "tasks.events"
	defaultGroup           = "tasks-consumer"
	defaultClientID        = "golang-basics"
	defaultDialTimeout     = 10 * time.Second
	defaultPublishTimeout  = 5 * time.Second
	defaultLagInterval     = 15 * time.Second
	defaultMaxPollRecords  = 100
	defaultHandlerTimeout  = 30 * time.Second
	defaultMaxRetries      = 5
	defaultRetryBackoff    = 200 * time.Millisecond
	defaultRetryBackoffMax = 30 * time.Second
	defaultDeadLetterTopic = "{topic}.dlq"
	defaultDrainTimeout    = 10 * time.Second
)

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix) and validates it. Every field is defaulted; when
// KAFKA_TOPICS is unset the consumer subscribes to the single Topic.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("kafka: parse config (prefix %q): %w", prefix, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("kafka: config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}

// withDefaults fills the zero fields of a hand-built Config (and of one
// loaded without envDefault tags), so every constructor sees a complete one.
func (c *Config) withDefaults() {
	if len(c.Brokers) == 0 {
		c.Brokers = []string{defaultBroker}
	}
	if c.Topic == "" {
		c.Topic = defaultTopic
	}
	if len(c.Topics) == 0 && c.Topic != "" {
		c.Topics = []string{c.Topic}
	}
	if c.Group == "" {
		c.Group = defaultGroup
	}
	if c.ClientID == "" {
		c.ClientID = defaultClientID
	}
	setDuration(&c.DialTimeout, defaultDialTimeout)
	setDuration(&c.PublishTimeout, defaultPublishTimeout)
	setDuration(&c.LagInterval, defaultLagInterval)
	setDuration(&c.HandlerTimeout, defaultHandlerTimeout)
	setDuration(&c.RetryBackoff, defaultRetryBackoff)
	setDuration(&c.RetryBackoffMax, defaultRetryBackoffMax)
	setDuration(&c.DrainTimeout, defaultDrainTimeout)
	if c.MaxPollRecords == 0 {
		c.MaxPollRecords = defaultMaxPollRecords
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.DeadLetterTopic == "" {
		c.DeadLetterTopic = defaultDeadLetterTopic
	}
	c.SASLMechanism = strings.ToLower(strings.TrimSpace(c.SASLMechanism))
}

func setDuration(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	}
}

// Validate applies the defaults and rejects a config the clients could not
// run with: no brokers, an unknown SASL mechanism or one without
// credentials, half a client key pair, TLS files without TLS, and
// non-positive budgets.
func (c *Config) Validate() error {
	c.withDefaults()
	var errs []error
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			errs = append(errs, errors.New("KAFKA_BROKERS: empty broker address"))
			break
		}
	}
	switch c.SASLMechanism {
	case "":
		if c.SASLUsername != "" || c.SASLPassword != "" {
			errs = append(errs, errors.New("KAFKA_SASL_USERNAME/PASSWORD set without KAFKA_SASL_MECHANISM"))
		}
	case "plain", "scram-sha-256", "scram-sha-512":
		if c.SASLUsername == "" || c.SASLPassword == "" {
			errs = append(errs, fmt.Errorf("KAFKA_SASL_MECHANISM %q needs KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD", c.SASLMechanism))
		}
	default:
		errs = append(errs, fmt.Errorf("KAFKA_SASL_MECHANISM: unknown mechanism %q (want plain|scram-sha-256|scram-sha-512)", c.SASLMechanism))
	}
	if (c.TLSClientCert == "") != (c.TLSClientKey == "") {
		errs = append(errs, errors.New("KAFKA_TLS_CLIENT_CERT and KAFKA_TLS_CLIENT_KEY must be set together"))
	}
	if !c.TLSEnabled && (c.TLSCACert != "" || c.TLSClientCert != "" || c.TLSInsecureSkipVerify) {
		errs = append(errs, errors.New("KAFKA_TLS_* options set but KAFKA_TLS_ENABLED is false"))
	}
	for name, d := range map[string]time.Duration{
		"KAFKA_DIAL_TIMEOUT":               c.DialTimeout,
		"KAFKA_PUBLISH_TIMEOUT":            c.PublishTimeout,
		"KAFKA_CONSUMER_HANDLER_TIMEOUT":   c.HandlerTimeout,
		"KAFKA_CONSUMER_RETRY_BACKOFF":     c.RetryBackoff,
		"KAFKA_CONSUMER_RETRY_BACKOFF_MAX": c.RetryBackoffMax,
		"KAFKA_CONSUMER_DRAIN_TIMEOUT":     c.DrainTimeout,
	} {
		if d < 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.RetryBackoffMax < c.RetryBackoff {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_RETRY_BACKOFF_MAX (%s) is below KAFKA_CONSUMER_RETRY_BACKOFF (%s)", c.RetryBackoffMax, c.RetryBackoff))
	}
	if c.MaxPollRecords < 0 {
		errs = append(errs, errors.New("KAFKA_CONSUMER_MAX_POLL_RECORDS must be positive"))
	}
	if strings.TrimSpace(c.DeadLetterTopic) != c.DeadLetterTopic {
		errs = append(errs, fmt.Errorf("KAFKA_DEAD_LETTER_TOPIC %q has surrounding spaces", c.DeadLetterTopic))
	}
	return errors.Join(errs...)
}

// maxRetries is the number of retries after the first attempt.
func (c Config) maxRetries() int { return max(c.MaxRetries, 0) }

// deadLetterTopic resolves the DLQ topic for a record consumed from source,
// or "" when the DLQ is disabled.
func (c Config) deadLetterTopic(source string) string {
	if c.DeadLetterTopic == DeadLetterDisabled || c.DeadLetterTopic == "-" {
		return ""
	}
	return strings.ReplaceAll(c.DeadLetterTopic, "{topic}", source)
}

func (c Config) brokersString() string { return strings.Join(c.Brokers, ",") }

// commonOpts are the client options every client shares: seeds, identity,
// dial timeout, TLS and SASL.
func (c Config) commonOpts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.DialTimeout(c.DialTimeout),
	}
	tlsCfg, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	mech, err := c.saslMechanism()
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

// producerOpts are the produce-side options, shared by [Producer] and the
// consumer's dead-letter publishing: every in-sync replica acknowledges
// (which the idempotent producer, on by default, requires), no linger (the
// synchronous shape), and topic auto-creation only when configured.
func (c Config) producerOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	}
	if c.AllowAutoTopicCreation {
		// Ask the broker to create a missing topic (it still decides, via
		// auto.create.topics.enable). A dev convenience; production
		// pre-provisions topics so a typo cannot create one.
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	return opts
}

// tlsConfig maps the TLS fields to a *tls.Config, or nil when TLS is off.
// The CA file is added to the system roots; ServerName is left to franz-go,
// which sets it per broker from the dialed host.
func (c Config) tlsConfig() (*tls.Config, error) {
	if !c.TLSEnabled {
		return nil, nil //nolint:nilnil // nil config means "plaintext", not an error
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.TLSInsecureSkipVerify, //nolint:gosec // opt-in for a self-signed dev broker; Validate requires TLS on, docs say never in production
	}
	if c.TLSCACert != "" {
		pem, err := os.ReadFile(c.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("kafka: read KAFKA_TLS_CA_CERT: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: KAFKA_TLS_CA_CERT %q holds no PEM certificate", c.TLSCACert)
		}
		cfg.RootCAs = pool
	}
	if c.TLSClientCert != "" || c.TLSClientKey != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSClientCert, c.TLSClientKey)
		if err != nil {
			return nil, fmt.Errorf("kafka: load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// saslMechanism maps the SASL fields to a franz-go mechanism, or nil when
// SASL is off.
func (c Config) saslMechanism() (sasl.Mechanism, error) {
	switch strings.ToLower(c.SASLMechanism) {
	case "":
		return nil, nil //nolint:nilnil // nil mechanism means "no SASL", not an error
	case "plain":
		return plain.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unknown SASL mechanism %q", c.SASLMechanism)
	}
}
