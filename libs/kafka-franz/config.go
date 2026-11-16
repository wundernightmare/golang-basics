package franz

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/wundernightmare/golang-basics/libs/kafka"
	"github.com/wundernightmare/golang-basics/libs/secret"
)

// minBrokerReadBytes is the floor of BrokerMaxReadBytes: besides fetches,
// one response holds the cluster metadata, which grows with the topic count.
const minBrokerReadBytes = 16 << 20

// fetchFraming is the room a fetch response gets on top of
// KAFKA_CONSUMER_FETCH_MAX_BYTES in BrokerMaxReadBytes.
const fetchFraming = 1 << 20

// maxBrokerReadBytes is the largest BrokerMaxReadBytes franz-go accepts
// (its option validation, kgo/config.go); a fetch may not be larger than the
// read bound, so it is the largest KAFKA_CONSUMER_FETCH_MAX_BYTES as well.
const maxBrokerReadBytes = 1 << 30

// brokerMaxReadBytes is one fetch plus framing, at least minBrokerReadBytes
// and at most maxBrokerReadBytes: a fetch size within fetchFraming of that
// limit keeps a bound franz-go accepts.
func brokerMaxReadBytes(cfg kafka.Config) int32 {
	n := max(int64(cfg.ConsumerFetchMaxBytes)+fetchFraming, minBrokerReadBytes)

	return int32(min(n, maxBrokerReadBytes))
}

// Ranges franz-go's option validation (kgo/config.go) puts on the named
// keys; validate reports them with the key's name.
const (
	minRecordTimeout  = time.Second
	maxLinger         = time.Minute
	minGroupTimeoutMs = 100
	minBatchBytes     = 512
	maxBatchBytes     = 1 << 30
)

// validate rejects what franz-go cannot express: everything NewProducer and
// NewConsumer would refuse. The keys with a known range are named; whatever
// else the option validation or the TLS and SASL setup refuses comes last, in
// franz-go's own words.
func validate(cfg kafka.Config) []error {
	var errs []error

	if cfg.ConsumerFetchMaxBytes > maxBrokerReadBytes {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_FETCH_MAX_BYTES (%d) is above %d, the most franz-go reads from a broker in one response",
			cfg.ConsumerFetchMaxBytes, maxBrokerReadBytes))
	}

	if cfg.PublishTimeout < minRecordTimeout {
		errs = append(errs, fmt.Errorf("KAFKA_PUBLISH_TIMEOUT (%s) is below %s, the shortest delivery timeout franz-go takes", cfg.PublishTimeout, minRecordTimeout))
	}

	if msDuration(cfg.LingerMs) > maxLinger {
		errs = append(errs, fmt.Errorf("KAFKA_PRODUCER_LINGER_MS (%d) is above %d, the longest batching window franz-go takes", cfg.LingerMs, maxLinger.Milliseconds()))
	}

	if cfg.SessionTimeoutMs < minGroupTimeoutMs || cfg.MaxPollIntervalMs < minGroupTimeoutMs {
		errs = append(errs, fmt.Errorf("KAFKA_CONSUMER_SESSION_TIMEOUT_MS (%d) and KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS (%d) may not be below %d with franz-go",
			cfg.SessionTimeoutMs, cfg.MaxPollIntervalMs, minGroupTimeoutMs))
	}

	if cfg.MessageMaxBytes < minBatchBytes || cfg.MessageMaxBytes > maxBatchBytes {
		errs = append(errs, fmt.Errorf("KAFKA_MESSAGE_MAX_BYTES (%d) is outside %d..%d, the batch sizes franz-go takes", cfg.MessageMaxBytes, minBatchBytes, maxBatchBytes))
	}

	if cfg.SSLCALocation == "probe" {
		errs = append(errs, errors.New(`KAFKA_SSL_CA_LOCATION "probe" is a librdkafka keyword: franz-go reads a PEM file, and uses the system roots when the key is empty`))
	}

	for name, props := range map[string]kafka.Properties{
		"KAFKA_EXTRA_PROPERTIES":          cfg.ExtraProperties,
		"KAFKA_PRODUCER_EXTRA_PROPERTIES": cfg.ProducerExtraProperties,
		"KAFKA_CONSUMER_EXTRA_PROPERTIES": cfg.ConsumerExtraProperties,
	} {
		if len(props) > 0 {
			errs = append(errs, fmt.Errorf("%s is set, but these are librdkafka properties and this binary links franz-go, "+
				"which has no string-keyed configuration (every setting is a typed option): use the named KAFKA_* keys, "+
				"or build with KAFKA_PROVIDER=rdkafka", name))
		}
	}

	if len(errs) > 0 {
		return errs
	}

	opts, err := consumerClientOpts(cfg, newStats(cfg))
	if err == nil {
		err = kgo.ValidateOpts(opts...)
	}

	if err != nil {
		errs = append(errs, fmt.Errorf("franz-go refuses this configuration: %w", err))
	}

	return errs
}

// consumerClientOpts are the options of a consumer's client, which also
// produces (the dead-letter topic): the superset of what a producer's has.
func consumerClientOpts(cfg kafka.Config, hooks kgo.Hook) ([]kgo.Opt, error) {
	opts, err := commonOpts(cfg, hooks)
	if err != nil {
		return nil, err
	}

	opts = append(opts, producerOpts(cfg)...)

	return append(opts, consumerOpts(cfg)...), nil
}

// commonOpts are the options every client shares: seeds, identity, dial
// timeout, TLS, SASL, the read bound and the statistics hooks.
func commonOpts(cfg kafka.Config, hooks kgo.Hook) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
		// The largest response the client accepts — and so allocates — from
		// a broker: one fetch (KAFKA_CONSUMER_FETCH_MAX_BYTES) plus framing,
		// not below what a large cluster's metadata needs. franz-go's own
		// default is 100 MiB.
		kgo.BrokerMaxReadBytes(brokerMaxReadBytes(cfg)),
		kgo.WithHooks(hooks),
		// Set on every client, consumers or not: franz-go checks the read
		// bound against them.
		kgo.FetchMaxBytes(int32(cfg.ConsumerFetchMaxBytes)),                   //nolint:gosec // G115: kafka.Config.Validate refuses a value above math.MaxInt32 (validateBuffers)
		kgo.FetchMaxPartitionBytes(int32(cfg.ConsumerPartitionFetchMaxBytes)), //nolint:gosec // G115: kafka.Config.Validate refuses a value above math.MaxInt32 (validateBuffers)
	}
	if cfg.AllowAutoTopicCreation {
		// Ask the broker to create a missing topic (it still decides, via
		// auto.create.topics.enable). A dev convenience; production
		// pre-provisions topics so a typo cannot create one.
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	tlsCfg, err := tlsConfig(cfg)
	if err != nil {
		return nil, err
	}

	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	mech, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}

	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}

	return opts, nil
}

// producerOpts are the produce-side options, shared by the producer and the
// consumer's dead-letter publishing: every in-sync replica acknowledges
// (idempotence, on by default, requires it), batches compressed with a
// fallback to none when compression does not shrink them, delivery bounded
// by KAFKA_PUBLISH_TIMEOUT, and the buffer bounded in records AND bytes
// (franz-go bounds only records by default).
func producerOpts(cfg kafka.Config) []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(msDuration(cfg.LingerMs)),
		kgo.ProducerBatchCompression(codec(cfg.CompressionType), kgo.NoCompression()),
		kgo.RecordDeliveryTimeout(cfg.PublishTimeout),
		kgo.MaxBufferedRecords(cfg.ProducerQueueMaxMessages),
		kgo.MaxBufferedBytes(cfg.ProducerQueueMaxKBytes * 1024),
		kgo.ProducerBatchMaxBytes(int32(cfg.MessageMaxBytes)), //nolint:gosec // G115: kafka.Config.Validate refuses a value above math.MaxInt32 (validateBuffers)
	}
}

// consumerOpts are the group options: manual commits, rebalances held until
// the loop committed its batch, the assignment strategy and timeouts, and
// the prefetch bounded by bytes.
func consumerOpts(cfg kafka.Config) []kgo.Opt {
	reset := kgo.NewOffset().AtStart()
	if cfg.AutoOffsetReset == "latest" {
		reset = kgo.NewOffset().AtEnd()
	}
	// franz-go buffers up to one fetch response per concurrent fetch: the
	// prefetch bound is KAFKA_CONSUMER_QUEUE_MAX_KBYTES worth of fetches (at
	// least one).
	fetches := max(cfg.ConsumerQueueMaxKBytes*1024/cfg.ConsumerFetchMaxBytes, 1)

	opts := []kgo.Opt{
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(subscription(cfg.Topics)...),
		kgo.DisableAutoCommit(),
		// Rebalances wait until the loop has processed and committed the
		// batch it polled (AllowRebalance), so a partition is never handed
		// to another member with processed-but-uncommitted records on it.
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(balancer(cfg.AssignmentStrategy)),
		kgo.SessionTimeout(msDuration(cfg.SessionTimeoutMs)),
		// franz-go has no max.poll.interval: the closest is how long a
		// member may take to rejoin during a rebalance, which with
		// BlockRebalanceOnPoll includes finishing the batch.
		kgo.RebalanceTimeout(msDuration(cfg.MaxPollIntervalMs)),
		kgo.ConsumeResetOffset(reset),
		kgo.MaxConcurrentFetches(fetches),
	}
	if slices.ContainsFunc(cfg.Topics, isPattern) {
		opts = append(opts, kgo.ConsumeRegex())
	}

	return opts
}

// isPattern: a subscribed topic that starts with "^" is a regular expression
// (librdkafka's rule, which libs/kafka documents for both providers).
func isPattern(topic string) bool { return strings.HasPrefix(topic, "^") }

// subscription is what ConsumeTopics gets. franz-go reads either every topic
// as a regular expression (ConsumeRegex) or none, so next to a pattern a
// plain name becomes the expression that matches that name and nothing else.
func subscription(topics []string) []string {
	if !slices.ContainsFunc(topics, isPattern) {
		return topics
	}

	out := make([]string, len(topics))
	for i, topic := range topics {
		out[i] = topic
		if !isPattern(topic) {
			out[i] = "^" + regexp.QuoteMeta(topic) + "$"
		}
	}

	return out
}

func codec(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}

func balancer(strategy string) kgo.GroupBalancer {
	switch strategy {
	case "range":
		return kgo.RangeBalancer()
	case "roundrobin":
		return kgo.RoundRobinBalancer()
	default:
		return kgo.CooperativeStickyBalancer()
	}
}

// tlsConfig maps the SSL keys to a *tls.Config, or nil without TLS. The CA
// file is the trust store, as it is in librdkafka (without one: the system
// roots); ServerName is left to franz-go, which sets it per broker from the
// dialed host. Endpoint identification "none" keeps verifying the chain
// against the CA — only the host name check is skipped, as in librdkafka.
func tlsConfig(cfg kafka.Config) (*tls.Config, error) {
	if cfg.SecurityProtocol != "ssl" && cfg.SecurityProtocol != "sasl_ssl" {
		return nil, nil //nolint:nilnil // nil config means "plaintext", not an error
	}

	var pool *x509.CertPool // nil: the system roots

	if cfg.SSLCALocation != "" {
		var err error
		if pool, err = secret.CertPool(cfg.SSLCALocation); err != nil {
			return nil, fmt.Errorf("kafka: KAFKA_SSL_CA_LOCATION: %w", err)
		}
	}

	tc := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	if cfg.SSLEndpointIdentificationAlgorithm == "none" {
		tc.InsecureSkipVerify = true // the chain is still verified below; only the host name is not
		tc.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("kafka: broker sent no certificate")
			}

			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}

			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter})

			return err
		}
	}

	if cfg.SSLCertificateLocation != "" {
		cert, err := tls.LoadX509KeyPair(cfg.SSLCertificateLocation, cfg.SSLKeyLocation)
		if err != nil {
			return nil, fmt.Errorf("kafka: load client key pair: %w", err)
		}

		tc.Certificates = []tls.Certificate{cert}
	}

	return tc, nil
}

// saslMechanism maps the SASL keys to a franz-go mechanism, or nil without
// SASL. The password comes from [kafka.Config.SASLSecret], which says when a
// password file is read.
func saslMechanism(cfg kafka.Config) (sasl.Mechanism, error) {
	if cfg.SecurityProtocol != "sasl_plaintext" && cfg.SecurityProtocol != "sasl_ssl" {
		return nil, nil //nolint:nilnil // nil mechanism means "no SASL", not an error
	}

	held, err := cfg.SASLSecret()
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}

	password := held.Expose()

	switch cfg.SASLMechanism {
	case "PLAIN":
		return plain.Auth{User: cfg.SASLUsername, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.SASLUsername, Pass: password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.SASLUsername, Pass: password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unknown SASL mechanism %q", cfg.SASLMechanism)
	}
}

func msDuration(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }
