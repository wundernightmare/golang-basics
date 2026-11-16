package rdkafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	_ "github.com/tracehubmmp/golang-basics/libs/kafka-rdkafka"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestProvider(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "rdkafka", kafka.Provider())
}

// A property a named key owns cannot come in through the passthrough —
// one source of truth — and the error names the key, never the value.
func TestValidate_PassthroughCannotOverrideNamedKeys(t *testing.T) {
	t.Parallel()

	cfg := kafka.Config{
		Brokers:                 []string{"b:9092"},
		ExtraProperties:         kafka.Properties{"queue.buffering.max.kbytes": "1048576", "debug": "broker"},
		ConsumerExtraProperties: kafka.Properties{"fetch.message.max.bytes": "1", "partition.assignment.strategy": "range", "go.events.channel.size": "1"},
		ProducerExtraProperties: kafka.Properties{"sasl.password": "hunter2", "batch.size": "262144"},
	}
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
