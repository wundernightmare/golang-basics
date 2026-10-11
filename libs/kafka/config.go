package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the Kafka client configuration, populated from the environment
// (and optionally YAML, via httpx.LoadYAML) with a per-service prefix (e.g.
// "TASKS_" for a producer, "CONSUMER_" for a consumer). The same struct
// serves both shapes and both providers: a [Producer] ignores the group and
// consumer fields; Topic is the default produce target and Topics the
// subscribe set.
//
// Every key is mapped explicitly onto the linked provider's client — a
// librdkafka property (libs/kafka-rdkafka) or a franz-go option
// (libs/kafka-franz); README.md has the full key → property → option table
// and why each default is safe.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_KAFKA_BROKERS                   comma-separated host:port seeds             (required; no default)
//	TASKS_KAFKA_TOPIC                     default produce topic                       (default "tasks.events")
//	TASKS_KAFKA_TOPICS                    comma-separated subscribe topics            (default: the Topic value)
//	TASKS_KAFKA_GROUP                     consumer group id                           (default "tasks-consumer")
//	TASKS_KAFKA_CLIENT_ID                 client id advertised to brokers             (default "golang-basics")
//	TASKS_KAFKA_DIAL_TIMEOUT              broker connection setup; also the boot check (default "10s")
//	TASKS_KAFKA_PUBLISH_TIMEOUT           delivery deadline of one Send, retries included (default "5s")
//	TASKS_KAFKA_ALLOW_AUTO_TOPIC_CREATION ask the broker to create a missing topic    (default false)
//	TASKS_KAFKA_STATISTICS_INTERVAL_MS    client metrics refresh period, negative = off (default 15000)
//
// Security (librdkafka's model, which franz-go is mapped onto: the protocol
// picks TLS and/or SASL):
//
//	TASKS_KAFKA_SECURITY_PROTOCOL         plaintext|ssl|sasl_plaintext|sasl_ssl       (default plaintext)
//	TASKS_KAFKA_SASL_MECHANISM            PLAIN|SCRAM-SHA-256|SCRAM-SHA-512           (sasl_* only)
//	TASKS_KAFKA_SASL_USERNAME             SASL user
//	TASKS_KAFKA_SASL_PASSWORD             SASL password (secret)
//	TASKS_KAFKA_SASL_PASSWORD_FILE        file holding the SASL password (a mounted Secret),
//	                                      read once when a client is built; not with SASL_PASSWORD
//	TASKS_KAFKA_SSL_CA_LOCATION           CA bundle (PEM) that verifies the brokers   (default: system roots)
//	TASKS_KAFKA_SSL_CERTIFICATE_LOCATION  client certificate (PEM), mTLS
//	TASKS_KAFKA_SSL_KEY_LOCATION          client key (PEM), mTLS
//	TASKS_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM https (verify the broker host name) | none (default https)
//
// Memory: every buffer a client keeps is bounded by a named key, always
// written by the provider — never left to the client's defaults (librdkafka
// would hold up to 1 GiB of unsent records while the brokers are away,
// franz-go has no byte bound at all by default). See README "Memory".
//
//	TASKS_KAFKA_PRODUCER_QUEUE_MAX_KBYTES    unsent records, KiB                  (default 65536 = 64 MiB)
//	TASKS_KAFKA_PRODUCER_QUEUE_MAX_MESSAGES  unsent records, count                (default 100000)
//	TASKS_KAFKA_MESSAGE_MAX_BYTES            largest record (batch) sent          (default 1000000)
//	TASKS_KAFKA_CONSUMER_QUEUE_MAX_KBYTES    prefetched records, KiB              (default 4096 = 4 MiB)
//	TASKS_KAFKA_CONSUMER_FETCH_MAX_BYTES     one fetch response                   (default 4194304)
//	TASKS_KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES one partition in a fetch       (default 1048576)
//
// Producer (acks=all and idempotence are fixed: a retried send never
// duplicates or reorders a record within a partition):
//
//	TASKS_KAFKA_PRODUCER_COMPRESSION_TYPE  none|gzip|snappy|lz4|zstd             (default lz4)
//	TASKS_KAFKA_PRODUCER_LINGER_MS         batching window                       (default 5)
//
// Consumer:
//
//	TASKS_KAFKA_CONSUMER_ASSIGNMENT_STRATEGY cooperative-sticky|range|roundrobin (default cooperative-sticky)
//	TASKS_KAFKA_CONSUMER_SESSION_TIMEOUT_MS  member considered dead after        (default 45000)
//	TASKS_KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS longest gap between polls / a rebalance's budget (default 300000)
//	TASKS_KAFKA_CONSUMER_AUTO_OFFSET_RESET earliest|latest, for a group with no commit yet (default earliest)
//	TASKS_KAFKA_CONSUMER_MAX_POLL_RECORDS  records processed per batch          (default 100)
//	TASKS_KAFKA_CONSUMER_HANDLER_TIMEOUT   deadline of one handler attempt      (default "30s")
//	TASKS_KAFKA_CONSUMER_MAX_RETRIES       retries after the first attempt, negative = none (default 5)
//	TASKS_KAFKA_CONSUMER_RETRY_BACKOFF     first retry delay, doubled per retry, jittered (default "200ms")
//	TASKS_KAFKA_CONSUMER_RETRY_BACKOFF_MAX cap on one retry delay               (default "30s")
//	TASKS_KAFKA_DEAD_LETTER_TOPIC          where exhausted records go; "{topic}" is the
//	                                       source topic; "none" disables the DLQ (default "{topic}.dlq")
//	TASKS_KAFKA_CONSUMER_DRAIN_TIMEOUT     budget to finish the in-flight batch on shutdown (default "10s")
//
// Any other librdkafka property (librdkafka provider only — franz-go has no
// string-keyed configuration, so the franz provider rejects them), applied
// last:
//
//	TASKS_KAFKA_EXTRA_PROPERTIES           every client       env: JSON object, YAML: a map
//	TASKS_KAFKA_PRODUCER_EXTRA_PROPERTIES  producers only     e.g. {"batch.size": 262144}
//	TASKS_KAFKA_CONSUMER_EXTRA_PROPERTIES  consumers only     e.g. {"fetch.wait.max.ms": 100}
//
// A property that has a named key above, or that the library manages
// (acks, enable.idempotence, enable.auto.commit, go.*), is rejected there:
// one source of truth per setting. Secret-looking properties (sasl.password,
// ssl.key.password, …) are redacted in /admin/config like any other secret.
//
// Zero means "unset" throughout (the defaults fill it), which is why the DLQ
// is disabled with the word "none" rather than an empty string and "no
// retries" or "no statistics" is a negative number: see httpx.LoadYAML on
// explicit zeros.
type Config struct {
	Brokers                []string      `env:"KAFKA_BROKERS" envSeparator:"," yaml:"brokers"`
	Topic                  string        `env:"KAFKA_TOPIC" envDefault:"tasks.events" yaml:"topic"`
	Topics                 []string      `env:"KAFKA_TOPICS" envSeparator:"," yaml:"topics"`
	Group                  string        `env:"KAFKA_GROUP" envDefault:"tasks-consumer" yaml:"group"`
	ClientID               string        `env:"KAFKA_CLIENT_ID" envDefault:"golang-basics" yaml:"client_id"`
	DialTimeout            time.Duration `env:"KAFKA_DIAL_TIMEOUT" envDefault:"10s" yaml:"dial_timeout"`
	AllowAutoTopicCreation bool          `env:"KAFKA_ALLOW_AUTO_TOPIC_CREATION" envDefault:"false" yaml:"allow_auto_topic_creation"`
	// PublishTimeout bounds one Send, the client's own retries included
	// (librdkafka message.timeout.ms, franz-go RecordDeliveryTimeout). A
	// broker that stops answering (not refusing — hanging) would otherwise
	// hold an outbox pass for minutes: found by the chaos suite.
	PublishTimeout     time.Duration `env:"KAFKA_PUBLISH_TIMEOUT" envDefault:"5s" yaml:"publish_timeout"`
	StatisticsInterval int           `env:"KAFKA_STATISTICS_INTERVAL_MS" envDefault:"15000" yaml:"statistics_interval_ms"`

	SecurityProtocol string `env:"KAFKA_SECURITY_PROTOCOL" yaml:"security_protocol"`
	SASLMechanism    string `env:"KAFKA_SASL_MECHANISM" yaml:"sasl_mechanism"`
	SASLUsername     string `env:"KAFKA_SASL_USERNAME" yaml:"sasl_username"`
	SASLPassword     string `env:"KAFKA_SASL_PASSWORD" secret:"true" yaml:"sasl_password"`
	// SASLPasswordFile keeps the secret out of the environment (and
	// /admin/config): a Kubernetes/Docker secret mount.
	SASLPasswordFile                   string `env:"KAFKA_SASL_PASSWORD_FILE" secret:"false" yaml:"sasl_password_file"`
	SSLCALocation                      string `env:"KAFKA_SSL_CA_LOCATION" yaml:"ssl_ca_location"`
	SSLCertificateLocation             string `env:"KAFKA_SSL_CERTIFICATE_LOCATION" secret:"false" yaml:"ssl_certificate_location"`
	SSLKeyLocation                     string `env:"KAFKA_SSL_KEY_LOCATION" secret:"false" yaml:"ssl_key_location"`
	SSLEndpointIdentificationAlgorithm string `env:"KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM" yaml:"ssl_endpoint_identification_algorithm"`

	ProducerQueueMaxKBytes   int `env:"KAFKA_PRODUCER_QUEUE_MAX_KBYTES" envDefault:"65536" yaml:"producer_queue_max_kbytes"`
	ProducerQueueMaxMessages int `env:"KAFKA_PRODUCER_QUEUE_MAX_MESSAGES" envDefault:"100000" yaml:"producer_queue_max_messages"`
	MessageMaxBytes          int `env:"KAFKA_MESSAGE_MAX_BYTES" envDefault:"1000000" yaml:"message_max_bytes"`
	ConsumerQueueMaxKBytes   int `env:"KAFKA_CONSUMER_QUEUE_MAX_KBYTES" envDefault:"4096" yaml:"consumer_queue_max_kbytes"`
	ConsumerFetchMaxBytes    int `env:"KAFKA_CONSUMER_FETCH_MAX_BYTES" envDefault:"4194304" yaml:"consumer_fetch_max_bytes"`
	// ConsumerPartitionFetchMaxBytes is the per-partition share of one fetch.
	ConsumerPartitionFetchMaxBytes int `env:"KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES" envDefault:"1048576" yaml:"consumer_partition_fetch_max_bytes"`

	CompressionType string `env:"KAFKA_PRODUCER_COMPRESSION_TYPE" envDefault:"lz4" yaml:"producer_compression_type"`
	LingerMs        int    `env:"KAFKA_PRODUCER_LINGER_MS" envDefault:"5" yaml:"producer_linger_ms"`

	AssignmentStrategy string        `env:"KAFKA_CONSUMER_ASSIGNMENT_STRATEGY" envDefault:"cooperative-sticky" yaml:"consumer_assignment_strategy"`
	SessionTimeoutMs   int           `env:"KAFKA_CONSUMER_SESSION_TIMEOUT_MS" envDefault:"45000" yaml:"consumer_session_timeout_ms"`
	MaxPollIntervalMs  int           `env:"KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS" envDefault:"300000" yaml:"consumer_max_poll_interval_ms"`
	AutoOffsetReset    string        `env:"KAFKA_CONSUMER_AUTO_OFFSET_RESET" envDefault:"earliest" yaml:"consumer_auto_offset_reset"`
	MaxPollRecords     int           `env:"KAFKA_CONSUMER_MAX_POLL_RECORDS" envDefault:"100" yaml:"consumer_max_poll_records"`
	HandlerTimeout     time.Duration `env:"KAFKA_CONSUMER_HANDLER_TIMEOUT" envDefault:"30s" yaml:"consumer_handler_timeout"`
	MaxRetries         int           `env:"KAFKA_CONSUMER_MAX_RETRIES" envDefault:"5" yaml:"consumer_max_retries"`
	RetryBackoff       time.Duration `env:"KAFKA_CONSUMER_RETRY_BACKOFF" envDefault:"200ms" yaml:"consumer_retry_backoff"`
	RetryBackoffMax    time.Duration `env:"KAFKA_CONSUMER_RETRY_BACKOFF_MAX" envDefault:"30s" yaml:"consumer_retry_backoff_max"`
	DeadLetterTopic    string        `env:"KAFKA_DEAD_LETTER_TOPIC" envDefault:"{topic}.dlq" yaml:"dead_letter_topic"`
	DrainTimeout       time.Duration `env:"KAFKA_CONSUMER_DRAIN_TIMEOUT" envDefault:"10s" yaml:"consumer_drain_timeout"`

	ExtraProperties         Properties `env:"KAFKA_EXTRA_PROPERTIES" yaml:"extra_properties"`
	ProducerExtraProperties Properties `env:"KAFKA_PRODUCER_EXTRA_PROPERTIES" yaml:"producer_extra_properties"`
	ConsumerExtraProperties Properties `env:"KAFKA_CONSUMER_EXTRA_PROPERTIES" yaml:"consumer_extra_properties"`
}

// Properties are raw librdkafka properties (name → value). In the
// environment they are a JSON object whose values may be strings, numbers or
// booleans ({"debug":"broker","batch.size":262144}); in YAML a plain map.
type Properties map[string]string

// UnmarshalText parses the JSON object form. An empty value is no
// properties (empty = unset).
func (p *Properties) UnmarshalText(b []byte) error {
	if strings.TrimSpace(string(b)) == "" {
		*p = nil
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("want a JSON object of librdkafka properties: %w", err)
	}
	out := make(Properties, len(raw))
	for k, v := range raw {
		switch v := v.(type) {
		case string:
			out[k] = v
		case bool:
			out[k] = strconv.FormatBool(v)
		case float64:
			out[k] = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			return fmt.Errorf("property %q: want a string, number or boolean, got %T", k, v)
		}
	}
	*p = out
	return nil
}

// DeadLetterDisabled is the [Config.DeadLetterTopic] value that turns the
// dead-letter topic off: a record that exhausts its retries then stops the
// consumer instead of being parked (see [Consumer.Run]).
const DeadLetterDisabled = "none"

// Defaults applied by [Config.Validate] (and the constructors) to zero
// fields of a hand-built Config. They mirror the envDefault tags.
const (
	defaultTopic                  = "tasks.events"
	defaultGroup                  = "tasks-consumer"
	defaultClientID               = "golang-basics"
	defaultDialTimeout            = 10 * time.Second
	defaultPublishTimeout         = 5 * time.Second
	defaultStatisticsInterval     = 15000
	defaultProducerQueueMaxKBytes = 64 * 1024
	defaultProducerQueueMaxMsgs   = 100_000
	defaultMessageMaxBytes        = 1_000_000
	defaultConsumerQueueMaxKBytes = 4 * 1024
	defaultConsumerFetchMaxBytes  = 4 << 20
	defaultPartitionFetchMaxBytes = 1 << 20
	defaultCompressionType        = "lz4"
	defaultLingerMs               = 5
	defaultAutoOffsetReset        = "earliest"
	defaultMaxPollRecords         = 100
	defaultHandlerTimeout         = 30 * time.Second
	defaultMaxRetries             = 5
	defaultRetryBackoff           = 200 * time.Millisecond
	defaultRetryBackoffMax        = 30 * time.Second
	defaultDeadLetterTopic        = "{topic}.dlq"
	defaultDrainTimeout           = 10 * time.Second
	defaultAssignmentStrategy     = "cooperative-sticky"
	defaultSessionTimeoutMs       = 45_000
	defaultMaxPollIntervalMs      = 300_000
)

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix) and validates it. Every field but KAFKA_BROKERS is
// defaulted; when KAFKA_TOPICS is unset the consumer subscribes to the single
// Topic.
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
	if c.Topic == "" {
		c.Topic = defaultTopic
	}
	if len(c.Topics) == 0 && c.Topic != "" {
		c.Topics = []string{c.Topic}
	}
	setString(&c.Group, defaultGroup)
	setString(&c.ClientID, defaultClientID)
	setString(&c.CompressionType, defaultCompressionType)
	setString(&c.AutoOffsetReset, defaultAutoOffsetReset)
	setString(&c.DeadLetterTopic, defaultDeadLetterTopic)
	setString(&c.AssignmentStrategy, defaultAssignmentStrategy)
	setInt(&c.SessionTimeoutMs, defaultSessionTimeoutMs)
	setInt(&c.MaxPollIntervalMs, defaultMaxPollIntervalMs)
	setDuration(&c.DialTimeout, defaultDialTimeout)
	setDuration(&c.PublishTimeout, defaultPublishTimeout)
	setDuration(&c.HandlerTimeout, defaultHandlerTimeout)
	setDuration(&c.RetryBackoff, defaultRetryBackoff)
	setDuration(&c.RetryBackoffMax, defaultRetryBackoffMax)
	setDuration(&c.DrainTimeout, defaultDrainTimeout)
	setInt(&c.StatisticsInterval, defaultStatisticsInterval)
	setInt(&c.ProducerQueueMaxKBytes, defaultProducerQueueMaxKBytes)
	setInt(&c.ProducerQueueMaxMessages, defaultProducerQueueMaxMsgs)
	setInt(&c.MessageMaxBytes, defaultMessageMaxBytes)
	setInt(&c.ConsumerQueueMaxKBytes, defaultConsumerQueueMaxKBytes)
	setInt(&c.ConsumerFetchMaxBytes, defaultConsumerFetchMaxBytes)
	setInt(&c.ConsumerPartitionFetchMaxBytes, defaultPartitionFetchMaxBytes)
	setInt(&c.LingerMs, defaultLingerMs)
	setInt(&c.MaxPollRecords, defaultMaxPollRecords)
	setInt(&c.MaxRetries, defaultMaxRetries)
	c.SecurityProtocol = strings.ToLower(strings.TrimSpace(c.SecurityProtocol))
	c.SASLMechanism = strings.ToUpper(strings.TrimSpace(c.SASLMechanism))
}

func setString(s *string, def string) {
	if *s == "" {
		*s = def
	}
}

func setInt(n *int, def int) {
	if *n == 0 {
		*n = def
	}
}

func setDuration(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	}
}

// Validate applies the defaults and rejects a config the clients could not
// run with: no brokers (there is no default address — a service where Kafka
// is optional checks KAFKA_BROKERS before calling this), an unknown protocol,
// mechanism, codec or strategy, SASL or TLS keys the protocol does not use,
// half a client key pair, inconsistent buffer bounds — and whatever the
// linked provider refuses (see [Driver.Validate]: extra properties).
func (c *Config) Validate() error {
	c.withDefaults()
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS must be set"))
	}
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			errs = append(errs, errors.New("KAFKA_BROKERS: empty broker address"))
			break
		}
	}
	errs = append(errs, c.validateSecurity()...)
	errs = append(errs, c.validateBuffers()...)
	if !slices.Contains([]string{"none", "gzip", "snappy", "lz4", "zstd"}, c.CompressionType) {
		errs = append(errs, fmt.Errorf("KAFKA_PRODUCER_COMPRESSION_TYPE: unknown codec %q (want none|gzip|snappy|lz4|zstd)", c.CompressionType))
	}
	if !slices.Contains([]string{"earliest", "latest"}, c.AutoOffsetReset) {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_AUTO_OFFSET_RESET: want earliest|latest, got %q", c.AutoOffsetReset))
	}
	if !slices.Contains([]string{"cooperative-sticky", "range", "roundrobin"}, c.AssignmentStrategy) {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_ASSIGNMENT_STRATEGY: want cooperative-sticky|range|roundrobin, got %q", c.AssignmentStrategy))
	}
	if c.SessionTimeoutMs < 0 || c.MaxPollIntervalMs < 0 {
		errs = append(errs, errors.New("KAFKA_CONSUMER_SESSION_TIMEOUT_MS and KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS must be positive"))
	} else if c.MaxPollIntervalMs < c.SessionTimeoutMs {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS (%d) is below KAFKA_CONSUMER_SESSION_TIMEOUT_MS (%d)", c.MaxPollIntervalMs, c.SessionTimeoutMs))
	}
	if c.LingerMs < 0 {
		errs = append(errs, errors.New("KAFKA_PRODUCER_LINGER_MS must not be negative"))
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
	if d := registered(); d != nil {
		errs = append(errs, d.Validate(*c)...)
	}
	return errors.Join(errs...)
}

// validateSecurity checks the protocol and that every SASL / TLS key is one
// the protocol uses — a CA file next to "plaintext" is a misconfiguration
// that would otherwise connect in the clear without a word.
func (c *Config) validateSecurity() []error {
	var errs []error
	sasl, tls := false, false
	switch c.SecurityProtocol {
	case "", "plaintext":
	case "ssl":
		tls = true
	case "sasl_plaintext":
		sasl = true
	case "sasl_ssl":
		sasl, tls = true, true
	default:
		errs = append(errs, fmt.Errorf("KAFKA_SECURITY_PROTOCOL: unknown protocol %q (want plaintext|ssl|sasl_plaintext|sasl_ssl)", c.SecurityProtocol))
		return errs
	}
	hasPassword := c.SASLPassword != "" || c.SASLPasswordFile != ""
	if c.SASLPassword != "" && c.SASLPasswordFile != "" {
		errs = append(errs, errors.New("KAFKA_SASL_PASSWORD and KAFKA_SASL_PASSWORD_FILE are both set: pick one"))
	}
	if sasl {
		switch c.SASLMechanism {
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
			if c.SASLUsername == "" || !hasPassword {
				errs = append(errs, fmt.Errorf("KAFKA_SASL_MECHANISM %s needs KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD(_FILE)", c.SASLMechanism))
			}
		case "":
			errs = append(errs, fmt.Errorf("KAFKA_SECURITY_PROTOCOL %s needs KAFKA_SASL_MECHANISM (PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)", c.SecurityProtocol))
		default:
			errs = append(errs, fmt.Errorf("KAFKA_SASL_MECHANISM: unknown mechanism %q (want PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)", c.SASLMechanism))
		}
	} else if c.SASLMechanism != "" || c.SASLUsername != "" || hasPassword {
		errs = append(errs, errors.New("KAFKA_SASL_* set but KAFKA_SECURITY_PROTOCOL is not sasl_plaintext or sasl_ssl"))
	}
	if !tls && (c.SSLCALocation != "" || c.SSLCertificateLocation != "" || c.SSLKeyLocation != "" || c.SSLEndpointIdentificationAlgorithm != "") {
		errs = append(errs, errors.New("KAFKA_SSL_* set but KAFKA_SECURITY_PROTOCOL is not ssl or sasl_ssl"))
	}
	if (c.SSLCertificateLocation == "") != (c.SSLKeyLocation == "") {
		errs = append(errs, errors.New("KAFKA_SSL_CERTIFICATE_LOCATION and KAFKA_SSL_KEY_LOCATION must be set together"))
	}
	errs = append(errs, c.validateFiles()...)
	switch c.SSLEndpointIdentificationAlgorithm {
	case "", "https", "none":
	default:
		errs = append(errs, fmt.Errorf("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: want https|none, got %q", c.SSLEndpointIdentificationAlgorithm))
	}
	return errs
}

// validateFiles checks that every file the security keys name is there and
// readable (the password file also not empty), so a missing secret or
// certificate mount fails the boot — and --check-config — with the key's
// name instead of a TLS or SASL error from the client later.
// KAFKA_SSL_CA_LOCATION may also be a directory of certificates, or
// librdkafka's "probe" (find the system's).
func (c *Config) validateFiles() []error {
	var errs []error
	if c.SASLPasswordFile != "" {
		b, err := os.ReadFile(c.SASLPasswordFile)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("KAFKA_SASL_PASSWORD_FILE: %w", err))
		case strings.TrimRight(string(b), "\r\n") == "":
			errs = append(errs, fmt.Errorf("KAFKA_SASL_PASSWORD_FILE: %s is empty", c.SASLPasswordFile))
		}
	}
	for key, path := range map[string]string{
		"KAFKA_SSL_CA_LOCATION":          c.SSLCALocation,
		"KAFKA_SSL_CERTIFICATE_LOCATION": c.SSLCertificateLocation,
		"KAFKA_SSL_KEY_LOCATION":         c.SSLKeyLocation,
	} {
		if path == "" || (key == "KAFKA_SSL_CA_LOCATION" && path == "probe") {
			continue
		}
		if err := readable(path, key == "KAFKA_SSL_CA_LOCATION"); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
	}
	slices.SortFunc(errs, func(a, b error) int { return strings.Compare(a.Error(), b.Error()) })
	return errs
}

// readable reports why path cannot be read: missing, unreadable, or a
// directory where a file is wanted.
func readable(path string, dirOK bool) error {
	f, err := os.Open(path) //nolint:gosec // operator-supplied config path
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.IsDir() && !dirOK {
		return fmt.Errorf("%s is a directory", path)
	}
	return nil
}

// validateBuffers checks the memory bounds against each other the way
// librdkafka would at client creation — but at boot, with the key names.
func (c *Config) validateBuffers() []error {
	var errs []error
	for name, n := range map[string]int{
		"KAFKA_PRODUCER_QUEUE_MAX_KBYTES":          c.ProducerQueueMaxKBytes,
		"KAFKA_PRODUCER_QUEUE_MAX_MESSAGES":        c.ProducerQueueMaxMessages,
		"KAFKA_MESSAGE_MAX_BYTES":                  c.MessageMaxBytes,
		"KAFKA_CONSUMER_QUEUE_MAX_KBYTES":          c.ConsumerQueueMaxKBytes,
		"KAFKA_CONSUMER_FETCH_MAX_BYTES":           c.ConsumerFetchMaxBytes,
		"KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES": c.ConsumerPartitionFetchMaxBytes,
	} {
		if n < 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.MessageMaxBytes > c.ProducerQueueMaxKBytes*1024 {
		errs = append(errs, fmt.Errorf("KAFKA_MESSAGE_MAX_BYTES (%d) does not fit in KAFKA_PRODUCER_QUEUE_MAX_KBYTES (%d KiB)", c.MessageMaxBytes, c.ProducerQueueMaxKBytes))
	}
	if c.ConsumerFetchMaxBytes < c.MessageMaxBytes {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_FETCH_MAX_BYTES (%d) is below KAFKA_MESSAGE_MAX_BYTES (%d): the largest record could not be fetched", c.ConsumerFetchMaxBytes, c.MessageMaxBytes))
	}
	if c.ConsumerPartitionFetchMaxBytes > c.ConsumerFetchMaxBytes {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES (%d) is above KAFKA_CONSUMER_FETCH_MAX_BYTES (%d)", c.ConsumerPartitionFetchMaxBytes, c.ConsumerFetchMaxBytes))
	}
	return errs
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
