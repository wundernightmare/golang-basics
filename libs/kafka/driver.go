package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// Driver is one Kafka client library behind the [Producer] / [Consumer]
// API. Two ship with the repo, each its own module so a service links
// exactly one:
//
//   - libs/kafka-rdkafka — librdkafka through confluent-kafka-go (cgo; a
//     static musl binary in the image). The default.
//   - libs/kafka-franz — franz-go, pure Go (CGO_ENABLED=0).
//
// A provider registers itself from its package init, the database/sql way:
// a service imports it for its side effect (services/*/internal/kafkaprovider
// picks one by build tag) and keeps calling [NewProducer] / [NewConsumer].
//
// Drivers supply primitives only. Everything a service relies on —
// commit-after-handle, retries, Permanent, dead-lettering, drain, trace
// propagation, spans and metric names — lives once in this package and is
// held to the same behaviour by the conformance suite (libs/kafka/kafkatest)
// every driver runs.
type Driver interface {
	// Name is "rdkafka" or "franz": the kafka_provider label of
	// kafka_client_info and the startup log.
	Name() string
	// Validate reports what this client cannot do with cfg (e.g. franz-go
	// has no librdkafka property passthrough). cfg is already defaulted and
	// passed the neutral checks.
	Validate(cfg Config) []error
	// NewProducer builds a producing client. It does not wait for the
	// brokers: [NewProducer] pings them.
	NewProducer(cfg Config, log *slog.Logger) (ProducerClient, error)
	// NewConsumer builds a group member subscribed to cfg.Topics, with
	// automatic commits off. rb is called on group changes, from within
	// Poll or Close (never concurrently with the loop's own processing).
	NewConsumer(cfg Config, log *slog.Logger, rb Rebalance) (ConsumerClient, error)
}

// ProducerClient sends records synchronously.
type ProducerClient interface {
	// Send enqueues msg (to msg.Topic) and waits until the brokers
	// acknowledge it, the client gives up (Config.PublishTimeout) or ctx
	// ends. The headers already carry the trace context. Every error is a
	// *[SendError].
	Send(ctx context.Context, msg Message) error
	// Ping checks the brokers answer (metadata), bounded by ctx.
	Ping(ctx context.Context) error
	// Stats is the latest client snapshot for the metrics.
	Stats() ClientStats
	// Close flushes what is queued (briefly) and releases the client.
	Close()
}

// ConsumerClient is a consumer-group member.
type ConsumerClient interface {
	// Poll returns up to max records, waiting while there are none until
	// ctx ends. Fetch errors that do not stop the client are returned next
	// to the records (errors.Join); [ErrClientClosed] once it is closed.
	// Records of a partition revoked during the call are not returned.
	Poll(ctx context.Context, max int) ([]Message, error)
	// Commit commits the given next offsets (last processed + 1)
	// synchronously.
	Commit(ctx context.Context, offsets []Offset) error
	// AllowRebalance lets a group change that waited for the batch proceed
	// (a no-op for clients that rebalance only within Poll).
	AllowRebalance()
	// DeadLetter is the producing side used for the dead-letter topic.
	DeadLetter() ProducerClient
	// Ping checks the brokers answer (metadata), bounded by ctx.
	Ping(ctx context.Context) error
	// Stats is the latest client snapshot for the metrics (lag included).
	Stats() ClientStats
	// Close leaves the group — calling Revoked for the partitions it held,
	// so their processed offsets are committed — and releases the client.
	Close()
}

// TopicPartition names one partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Offset is the next offset to consume from a partition: the committed
// position, one past the last processed record.
type Offset struct {
	TopicPartition
	Next int64
}

// Rebalance is how a driver reports group changes to the [Consumer].
type Rebalance struct {
	Assigned func(ctx context.Context, parts []TopicPartition)
	// Revoked: the partitions move to another member in an orderly way;
	// commit what was processed on them now.
	Revoked func(ctx context.Context, parts []TopicPartition)
	// Lost: the partitions already belong to someone else (session
	// expired); committing would at best fail.
	Lost func(ctx context.Context, parts []TopicPartition)
}

// ClientStats is a driver's view of its client for the metrics. Valid is
// false until the first snapshot, and when the last one is stale.
type ClientStats struct {
	Valid         bool
	BrokersUp     int64 // brokers with a live connection
	RequestErrors int64 // cumulative request timeouts + transport errors
	QueueBytes    int64 // producer: bytes waiting in the local send queue
	QueueMaxBytes int64 // producer: the queue's bound
	LagMax        int64 // consumer: worst assigned partition
	LagSum        int64 // consumer: total over assigned partitions
}

// ErrClientClosed is what [ConsumerClient.Poll] returns once the client is
// closed.
var ErrClientClosed = errors.New("kafka: client closed")

var (
	driverMu sync.Mutex
	driver   Driver
)

// Register makes d the driver [NewProducer] and [NewConsumer] use. It is
// called from a provider package's init; a second registration panics —
// a binary links exactly one provider.
func Register(d Driver) {
	driverMu.Lock()
	defer driverMu.Unlock()
	if driver != nil {
		panic(fmt.Sprintf("kafka: provider %q registered while %q already is: link exactly one of libs/kafka-rdkafka, libs/kafka-franz", d.Name(), driver.Name()))
	}
	driver = d
}

func registered() Driver {
	driverMu.Lock()
	defer driverMu.Unlock()
	return driver
}

// Provider names the linked driver ("" when none is).
func Provider() string {
	if d := registered(); d != nil {
		return d.Name()
	}
	return ""
}

func mustDriver() (Driver, error) {
	d := registered()
	if d == nil {
		return nil, errors.New("kafka: no provider linked: import libs/kafka-rdkafka or libs/kafka-franz for its side effect")
	}
	return d, nil
}
