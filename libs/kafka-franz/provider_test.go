package franz_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/wundernightmare/golang-basics/libs/kafka"
	_ "github.com/wundernightmare/golang-basics/libs/kafka-franz"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestProvider(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "franz", kafka.Provider())
}

// franz-go has no string-keyed configuration: librdkafka properties are a
// config error naming the way out, not silently ignored.
func TestValidate_RejectsPassthrough(t *testing.T) {
	t.Parallel()

	cfg := kafka.Config{Brokers: []string{"b:9092"}, ProducerExtraProperties: kafka.Properties{"batch.size": "1"}}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KAFKA_PRODUCER_EXTRA_PROPERTIES")
	assert.Contains(t, err.Error(), "KAFKA_PROVIDER=rdkafka")
	assert.NotContains(t, err.Error(), "batch.size=1", "values are never echoed")
}
