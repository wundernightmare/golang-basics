package franz_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	_ "github.com/tracehubmmp/golang-basics/libs/kafka-franz"
	"github.com/tracehubmmp/golang-basics/libs/kafka/kafkatest"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One Kafka (KRaft) per test binary; every test owns its topics and consumer
// groups by name (testx.Unique).
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

func admin(t testing.TB) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(containers.Kafka(t)...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

var harness = kafkatest.Harness{
	Brokers: containers.Kafka,
	CreateTopic: func(t testing.TB, partitions int32, names ...string) {
		t.Helper()
		adm := admin(t)
		resp, err := adm.CreateTopics(t.Context(), partitions, 1, nil, names...)
		require.NoError(t, err)
		require.NoError(t, resp.Error())
		// Created is not yet led: wait until every partition answers a
		// ListOffsets, which the leader itself serves.
		require.Eventually(t, func() bool {
			offs, err := adm.ListEndOffsets(t.Context(), names...)
			if err != nil {
				return false
			}
			n, ok := 0, true
			offs.Each(func(o kadm.ListedOffset) { n++; ok = ok && o.Err == nil })
			return ok && n == len(names)*int(partitions)
		}, 30*time.Second, 50*time.Millisecond, "topics %v get leaders", names)
	},
	Committed: func(t testing.TB, group, topic string) map[int32]int64 {
		t.Helper()
		resp, err := admin(t).FetchOffsets(t.Context(), group)
		require.NoError(t, err)
		out := map[int32]int64{}
		resp.Each(func(o kadm.OffsetResponse) {
			if o.Topic == topic && o.Err == nil {
				out[o.Partition] = o.At
			}
		})
		return out
	},
}

func TestConformance(t *testing.T) { kafkatest.Run(t, harness) }

func TestSASLSSL(t *testing.T) { kafkatest.SASLSSL(t) }

func TestProvider(t *testing.T) {
	assert.Equal(t, "franz", kafka.Provider())
}

// franz-go has no string-keyed configuration: librdkafka properties are a
// config error naming the way out, not silently ignored.
func TestValidate_RejectsPassthrough(t *testing.T) {
	cfg := kafka.Config{Brokers: []string{"b:9092"}, ProducerExtraProperties: kafka.Properties{"batch.size": "1"}}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KAFKA_PRODUCER_EXTRA_PROPERTIES")
	assert.Contains(t, err.Error(), "KAFKA_PROVIDER=rdkafka")
	assert.NotContains(t, err.Error(), "batch.size=1", "values are never echoed")
}
