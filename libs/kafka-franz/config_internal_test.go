package franz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// Every named buffer key reaches its franz-go option: nothing is left to
// the client's defaults (unbounded bytes, unbounded concurrent fetches).
func TestOpts_MapEveryBufferKey(t *testing.T) {
	cfg := kafka.Config{Brokers: []string{"localhost:1"}, Topics: []string{"t"}, Group: "g",
		ProducerQueueMaxKBytes: 2048, ProducerQueueMaxMessages: 500, MessageMaxBytes: 600_000,
		ConsumerQueueMaxKBytes: 8192, ConsumerFetchMaxBytes: 2 << 20, ConsumerPartitionFetchMaxBytes: 512 << 10,
		CompressionType: "zstd", AssignmentStrategy: "range"}
	require.NoError(t, cfg.Validate())
	common, err := commonOpts(cfg, newStats(cfg))
	require.NoError(t, err)
	cl, err := kgo.NewClient(append(append(common, producerOpts(cfg)...), consumerOpts(cfg)...)...)
	require.NoError(t, err)
	defer cl.Close()

	opts := cl.OptValues
	assert.Equal(t, []any{int64(2048 * 1024)}, opts(kgo.MaxBufferedBytes))
	assert.Equal(t, []any{int64(500)}, opts(kgo.MaxBufferedRecords))
	assert.Equal(t, []any{int32(2 << 20)}, opts(kgo.FetchMaxBytes))
	assert.Equal(t, []any{int32(512 << 10)}, opts(kgo.FetchMaxPartitionBytes))
	assert.Equal(t, []any{4}, opts(kgo.MaxConcurrentFetches), "8 MiB of prefetch = 4 fetches of 2 MiB")
	assert.Equal(t, []any{int32(minBrokerReadBytes)}, opts(kgo.BrokerMaxReadBytes))
}

func TestTLSConfig(t *testing.T) {
	tc, err := tlsConfig(kafka.Config{SecurityProtocol: "sasl_plaintext"})
	require.NoError(t, err)
	assert.Nil(t, tc, "no TLS unless the protocol says so")

	tc, err = tlsConfig(kafka.Config{SecurityProtocol: "ssl", SSLEndpointIdentificationAlgorithm: "none"})
	require.NoError(t, err)
	require.NotNil(t, tc.VerifyConnection, "the chain is still verified without the host name check")

	_, err = tlsConfig(kafka.Config{SecurityProtocol: "ssl", SSLCALocation: "/nonexistent/ca.pem"})
	require.Error(t, err)
}

func TestSASLMechanism(t *testing.T) {
	for _, m := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		mech, err := saslMechanism(kafka.Config{SecurityProtocol: "sasl_ssl", SASLMechanism: m, SASLUsername: "u", SASLPassword: "p"})
		require.NoError(t, err)
		assert.Equal(t, m, mech.Name())
	}
	_, err := saslMechanism(kafka.Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPasswordFile: "/nonexistent"})
	require.Error(t, err)
}
