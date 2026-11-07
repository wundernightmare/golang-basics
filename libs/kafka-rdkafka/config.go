package rdkafka

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// receiveMessageMaxBytesOverhead: librdkafka requires
// receive.message.max.bytes ≥ fetch.max.bytes + 512 (response framing).
const receiveMessageMaxBytesOverhead = 512

// ownedProperties maps each librdkafka property a named key (or the
// library) sets to what to use instead; *_EXTRA_PROPERTIES may not set them
// — one source of truth per setting.
var ownedProperties = map[string]string{ //nolint:gosec,gochecknoglobals // G101: property names mapped to key names, no credentials; read-only lookup table
	"bootstrap.servers":                     "KAFKA_BROKERS",
	"metadata.broker.list":                  "KAFKA_BROKERS",
	"client.id":                             "KAFKA_CLIENT_ID",
	"group.id":                              "KAFKA_GROUP",
	"socket.connection.setup.timeout.ms":    "KAFKA_DIAL_TIMEOUT",
	"message.timeout.ms":                    "KAFKA_PUBLISH_TIMEOUT",
	"delivery.timeout.ms":                   "KAFKA_PUBLISH_TIMEOUT",
	"allow.auto.create.topics":              "KAFKA_ALLOW_AUTO_TOPIC_CREATION",
	"statistics.interval.ms":                "KAFKA_STATISTICS_INTERVAL_MS",
	"security.protocol":                     "KAFKA_SECURITY_PROTOCOL",
	"sasl.mechanism":                        "KAFKA_SASL_MECHANISM",
	"sasl.mechanisms":                       "KAFKA_SASL_MECHANISM",
	"sasl.username":                         "KAFKA_SASL_USERNAME",
	"sasl.password":                         "KAFKA_SASL_PASSWORD(_FILE)",
	"ssl.ca.location":                       "KAFKA_SSL_CA_LOCATION",
	"ssl.certificate.location":              "KAFKA_SSL_CERTIFICATE_LOCATION",
	"ssl.key.location":                      "KAFKA_SSL_KEY_LOCATION",
	"ssl.endpoint.identification.algorithm": "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM",
	"queue.buffering.max.kbytes":            "KAFKA_PRODUCER_QUEUE_MAX_KBYTES",
	"queue.buffering.max.messages":          "KAFKA_PRODUCER_QUEUE_MAX_MESSAGES",
	"message.max.bytes":                     "KAFKA_MESSAGE_MAX_BYTES",
	"queued.max.messages.kbytes":            "KAFKA_CONSUMER_QUEUE_MAX_KBYTES",
	"fetch.max.bytes":                       "KAFKA_CONSUMER_FETCH_MAX_BYTES",
	"receive.message.max.bytes":             "KAFKA_CONSUMER_FETCH_MAX_BYTES (set from it)",
	"max.partition.fetch.bytes":             "KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES",
	"fetch.message.max.bytes":               "KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES",
	"compression.type":                      "KAFKA_PRODUCER_COMPRESSION_TYPE",
	"compression.codec":                     "KAFKA_PRODUCER_COMPRESSION_TYPE",
	"linger.ms":                             "KAFKA_PRODUCER_LINGER_MS",
	"queue.buffering.max.ms":                "KAFKA_PRODUCER_LINGER_MS",
	"partition.assignment.strategy":         "KAFKA_CONSUMER_ASSIGNMENT_STRATEGY",
	"session.timeout.ms":                    "KAFKA_CONSUMER_SESSION_TIMEOUT_MS",
	"max.poll.interval.ms":                  "KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS",
	"auto.offset.reset":                     "KAFKA_CONSUMER_AUTO_OFFSET_RESET",
	"acks":                                  "nothing: fixed to all by libs/kafka",
	"request.required.acks":                 "nothing: fixed to all by libs/kafka",
	"enable.idempotence":                    "nothing: fixed to true by libs/kafka",
	"enable.auto.commit":                    "nothing: the consumer loop commits",
	"enable.auto.offset.store":              "nothing: the consumer loop commits",
}

// validate rejects extra properties a named key owns. Only the property
// names are reported, never the values (one may be a password).
func validate(cfg kafka.Config) []error {
	var errs []error

	for _, src := range []struct {
		name  string
		props kafka.Properties
	}{
		{"KAFKA_EXTRA_PROPERTIES", cfg.ExtraProperties},
		{"KAFKA_PRODUCER_EXTRA_PROPERTIES", cfg.ProducerExtraProperties},
		{"KAFKA_CONSUMER_EXTRA_PROPERTIES", cfg.ConsumerExtraProperties},
	} {
		for _, k := range slices.Sorted(maps.Keys(src.props)) {
			if use, ok := ownedProperties[k]; ok {
				errs = append(errs, fmt.Errorf("%s: %q is not set here — use %s", src.name, k, use))
			} else if strings.HasPrefix(k, "go.") {
				errs = append(errs, fmt.Errorf("%s: %q is managed by libs/kafka-rdkafka", src.name, k))
			}
		}
	}

	return errs
}

// commonProperties are the properties every client shares: brokers,
// identity, security, statistics, and the logs routed to slog. A password
// file is read here, once per client built.
func commonProperties(cfg kafka.Config) (ckafka.ConfigMap, error) {
	props := ckafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(cfg.Brokers, ","),
		"client.id":                          cfg.ClientID,
		"socket.connection.setup.timeout.ms": int(cfg.DialTimeout.Milliseconds()),
		// Cloud load balancers and NAT gateways drop idle connections
		// silently; keepalive finds out before a send pays a timeout.
		"socket.keepalive.enable":  true,
		"allow.auto.create.topics": cfg.AllowAutoTopicCreation,
		"message.max.bytes":        cfg.MessageMaxBytes,
		"go.logs.channel.enable":   true,
	}
	if cfg.StatisticsInterval > 0 {
		props["statistics.interval.ms"] = cfg.StatisticsInterval
	}

	if cfg.SecurityProtocol != "" {
		props["security.protocol"] = cfg.SecurityProtocol
	}

	if cfg.SASLMechanism != "" { //nolint:nestif // mirrors kafka-franz saslMechanism; moving the password-file read into libs/kafka is a separate design change
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

		props["sasl.mechanism"] = cfg.SASLMechanism
		props["sasl.username"] = cfg.SASLUsername
		props["sasl.password"] = password
	}

	for k, v := range map[string]string{
		"ssl.ca.location":                       cfg.SSLCALocation,
		"ssl.certificate.location":              cfg.SSLCertificateLocation,
		"ssl.key.location":                      cfg.SSLKeyLocation,
		"ssl.endpoint.identification.algorithm": cfg.SSLEndpointIdentificationAlgorithm,
	} {
		if v != "" {
			props[k] = v
		}
	}

	for k, v := range cfg.ExtraProperties {
		props[k] = v
	}

	return props, nil
}

// producerProperties are the produce-side properties, shared by the
// producer and the consumer's dead-letter publishing: every in-sync replica
// acknowledges, idempotent, compressed, with a bounded local queue.
func producerProperties(cfg kafka.Config) (ckafka.ConfigMap, error) {
	props, err := commonProperties(cfg)
	if err != nil {
		return nil, err
	}

	props["acks"] = "all"
	props["enable.idempotence"] = true
	props["compression.type"] = cfg.CompressionType
	props["linger.ms"] = cfg.LingerMs
	props["message.timeout.ms"] = int(cfg.PublishTimeout.Milliseconds())
	// The local send queue. librdkafka's default holds up to 1 GiB: with
	// the brokers away and acks=all, that is where the pod's memory goes.
	// Full → QUEUE_FULL, which Send waits out within its budget.
	props["queue.buffering.max.kbytes"] = cfg.ProducerQueueMaxKBytes

	props["queue.buffering.max.messages"] = cfg.ProducerQueueMaxMessages
	for k, v := range cfg.ProducerExtraProperties {
		props[k] = v
	}

	return props, nil
}

// consumerProperties are the group member's: explicit commits only (the
// loop commits what it finished), the assignment strategy and timeouts, a
// bounded prefetch, and the offset reset for a group with no commit yet.
func consumerProperties(cfg kafka.Config) (ckafka.ConfigMap, error) {
	props, err := commonProperties(cfg)
	if err != nil {
		return nil, err
	}

	props["group.id"] = cfg.Group
	props["enable.auto.commit"] = false
	props["enable.auto.offset.store"] = false
	props["auto.offset.reset"] = cfg.AutoOffsetReset
	props["partition.assignment.strategy"] = cfg.AssignmentStrategy
	props["session.timeout.ms"] = cfg.SessionTimeoutMs
	props["max.poll.interval.ms"] = cfg.MaxPollIntervalMs
	// Bounded prefetch. librdkafka sizes its local queue for raw throughput
	// (up to 64 MiB per partition): a consumer that handles one record at a
	// time and wakes up to a deep backlog would pull hundreds of MB in
	// faster than it drains them and be OOM-killed before its first commit.
	props["queued.max.messages.kbytes"] = cfg.ConsumerQueueMaxKBytes
	props["fetch.max.bytes"] = cfg.ConsumerFetchMaxBytes
	props["receive.message.max.bytes"] = cfg.ConsumerFetchMaxBytes + receiveMessageMaxBytesOverhead

	props["max.partition.fetch.bytes"] = cfg.ConsumerPartitionFetchMaxBytes
	for k, v := range cfg.ConsumerExtraProperties {
		props[k] = v
	}

	return props, nil
}
