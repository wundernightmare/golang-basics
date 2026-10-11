package rdkafka_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	_ "github.com/tracehubmmp/golang-basics/libs/kafka-rdkafka"
	"github.com/tracehubmmp/golang-basics/libs/kafka/kafkatest"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One Kafka (KRaft) per test binary; every test owns its topics and consumer
// groups by name (testx.Unique).
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

func admin(t testing.TB) *ckafka.AdminClient {
	t.Helper()
	a, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": strings.Join(containers.Kafka(t), ",")})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	return a
}

var harness = kafkatest.Harness{
	Brokers: containers.Kafka,
	CreateTopic: func(t testing.TB, partitions int32, names ...string) {
		t.Helper()
		a := admin(t)
		specs := make([]ckafka.TopicSpecification, len(names))
		for i, n := range names {
			specs[i] = ckafka.TopicSpecification{Topic: n, NumPartitions: int(partitions), ReplicationFactor: 1}
		}
		res, err := a.CreateTopics(t.Context(), specs)
		require.NoError(t, err)
		for _, r := range res {
			require.Equal(t, ckafka.ErrNoError, r.Error.Code(), "create %s: %v", r.Topic, r.Error)
		}
		// Created is not yet led: wait until the metadata names a leader for
		// every partition.
		require.Eventually(t, func() bool {
			for _, n := range names {
				md, err := a.GetMetadata(&n, false, 2000)
				if err != nil || len(md.Topics[n].Partitions) != int(partitions) {
					return false
				}
				for _, p := range md.Topics[n].Partitions {
					if p.Leader < 0 || p.Error.Code() != ckafka.ErrNoError {
						return false
					}
				}
			}
			return true
		}, 30*time.Second, 50*time.Millisecond, "topics %v get leaders", names)
	},
	Committed: func(t testing.TB, group, topic string) map[int32]int64 {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		res, err := admin(t).ListConsumerGroupOffsets(ctx, []ckafka.ConsumerGroupTopicPartitions{{Group: group}})
		require.NoError(t, err)
		out := map[int32]int64{}
		for _, g := range res.ConsumerGroupsTopicPartitions {
			for _, tp := range g.Partitions {
				if tp.Topic != nil && *tp.Topic == topic && tp.Error == nil && tp.Offset >= 0 {
					out[tp.Partition] = int64(tp.Offset)
				}
			}
		}
		return out
	},
}

func TestConformance(t *testing.T) { kafkatest.Run(t, harness) }

func TestSASLSSL(t *testing.T) { kafkatest.SASLSSL(t) }

func TestProvider(t *testing.T) {
	assert.Equal(t, "rdkafka", kafka.Provider())
}

// A property a named key owns cannot come in through the passthrough —
// one source of truth — and the error names the key, never the value.
func TestValidate_PassthroughCannotOverrideNamedKeys(t *testing.T) {
	cfg := kafka.Config{Brokers: []string{"b:9092"},
		ExtraProperties:         kafka.Properties{"queue.buffering.max.kbytes": "1048576", "debug": "broker"},
		ConsumerExtraProperties: kafka.Properties{"fetch.message.max.bytes": "1", "partition.assignment.strategy": "range", "go.events.channel.size": "1"},
		ProducerExtraProperties: kafka.Properties{"sasl.password": "hunter2", "batch.size": "262144"}}
	err := cfg.Validate()
	require.Error(t, err)
	for _, want := range []string{
		`KAFKA_EXTRA_PROPERTIES: "queue.buffering.max.kbytes" is not set here — use KAFKA_PRODUCER_QUEUE_MAX_KBYTES`,
		`"fetch.message.max.bytes" is not set here — use KAFKA_CONSUMER_PARTITION_FETCH_MAX_BYTES`,
		`"partition.assignment.strategy" is not set here — use KAFKA_CONSUMER_ASSIGNMENT_STRATEGY`,
		`"go.events.channel.size" is managed by libs/kafka-rdkafka`,
		`"sasl.password" is not set here`,
	} {
		assert.Contains(t, err.Error(), want)
	}
	assert.NotContains(t, err.Error(), "hunter2", "values are never echoed")
	assert.NotContains(t, err.Error(), "debug")
	assert.NotContains(t, err.Error(), "batch.size")
}

// A static member fenced by another instance with its group.instance.id is a
// fatal librdkafka error: Run ends with kafka.ErrClientFailed (the service
// exits and restarts) and readiness fails — the loop no longer polls a dead
// client while /readyz stays green.
func TestFencedStaticMemberStopsTheLoop(t *testing.T) {
	topic := testx.Unique("fenced")
	harness.CreateTopic(t, 1, topic)
	cfg := func() kafka.Config {
		return kafka.Config{
			Brokers: containers.Kafka(t), Topics: []string{topic}, Group: "g-" + topic,
			ClientID: testx.Unique("cons"), StatisticsInterval: -1, SessionTimeoutMs: 10_000,
			ConsumerExtraProperties: kafka.Properties{"group.instance.id": "member-1"},
		}
	}
	handle := func(context.Context, kafka.Message) error { return nil }

	first, err := kafka.NewConsumer(t.Context(), cfg(), nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- first.Run(context.Background(), handle) }()
	reg := prometheus.NewRegistry()
	reg.MustRegister(first.Collectors()...)
	require.Eventually(t, func() bool { return testx.Metric(t, reg, "kafka_consumer_assigned_partitions", nil) == 1 },
		30*time.Second, 100*time.Millisecond, "the first member joins")

	second, err := kafka.NewConsumer(t.Context(), cfg(), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = second.Run(ctx, handle) }()

	select {
	case err := <-done:
		require.ErrorIs(t, err, kafka.ErrClientFailed)
		t.Logf("first member stopped: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the fenced member kept polling")
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	require.Error(t, first.ReadyCheck()(rctx), "readiness fails on the dead client")
}
