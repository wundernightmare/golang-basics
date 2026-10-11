package franz

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// minBrokerReadBytes is the floor of BrokerMaxReadBytes: besides fetches,
// one response holds the cluster metadata, which grows with the topic count.
const minBrokerReadBytes = 16 << 20

// validate rejects what franz-go cannot express.
func validate(cfg kafka.Config) []error {
	var errs []error
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
	return errs
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
		// never below what a large cluster's metadata needs. franz-go's own
		// default is 100 MiB.
		kgo.BrokerMaxReadBytes(int32(max(cfg.ConsumerFetchMaxBytes+1<<20, minBrokerReadBytes))), //nolint:gosec // bounded by Validate
		kgo.WithHooks(hooks),
		// Set on every client, consumers or not: franz-go checks the read
		// bound against them.
		kgo.FetchMaxBytes(int32(cfg.ConsumerFetchMaxBytes)),                   //nolint:gosec // bounded by Validate
		kgo.FetchMaxPartitionBytes(int32(cfg.ConsumerPartitionFetchMaxBytes)), //nolint:gosec // bounded by Validate
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
		kgo.ProducerBatchMaxBytes(int32(cfg.MessageMaxBytes)), //nolint:gosec // bounded by Validate
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
	return []kgo.Opt{
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
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
// file is added to the system roots; ServerName is left to franz-go, which
// sets it per broker from the dialed host. Endpoint identification "none"
// keeps verifying the chain against the CA — only the host name check is
// skipped, as in librdkafka.
func tlsConfig(cfg kafka.Config) (*tls.Config, error) {
	if cfg.SecurityProtocol != "ssl" && cfg.SecurityProtocol != "sasl_ssl" {
		return nil, nil //nolint:nilnil // nil config means "plaintext", not an error
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if cfg.SSLCALocation != "" {
		pem, err := os.ReadFile(cfg.SSLCALocation)
		if err != nil {
			return nil, fmt.Errorf("kafka: read KAFKA_SSL_CA_LOCATION: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: KAFKA_SSL_CA_LOCATION %q holds no PEM certificate", cfg.SSLCALocation)
		}
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	if cfg.SSLEndpointIdentificationAlgorithm == "none" {
		tc.InsecureSkipVerify = true //nolint:gosec // the chain is still verified below; only the host name is not
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
// SASL. A password file is read here, once per client built.
func saslMechanism(cfg kafka.Config) (sasl.Mechanism, error) {
	if cfg.SecurityProtocol != "sasl_plaintext" && cfg.SecurityProtocol != "sasl_ssl" {
		return nil, nil //nolint:nilnil // nil mechanism means "no SASL", not an error
	}
	password := cfg.SASLPassword
	if cfg.SASLPasswordFile != "" {
		b, err := os.ReadFile(cfg.SASLPasswordFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: KAFKA_SASL_PASSWORD_FILE: %w", err)
		}
		if password = strings.TrimRight(string(b), "\r\n"); password == "" {
			return nil, fmt.Errorf("kafka: KAFKA_SASL_PASSWORD_FILE: %s is empty", cfg.SASLPasswordFile)
		}
	}
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
