package rdkafka

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

func defaulted(t *testing.T, cfg kafka.Config) kafka.Config {
	t.Helper()
	require.NoError(t, cfg.Validate())
	return cfg
}

// Every buffer is bounded by a named key, never left to librdkafka's
// defaults (1 GiB send queue, 64 MiB prefetch per partition).
func TestProperties_EveryBufferIsNamed(t *testing.T) {
	cfg := defaulted(t, kafka.Config{Brokers: []string{"b1:9092", "b2:9092"}})
	p, err := producerProperties(cfg)
	require.NoError(t, err)
	assert.Equal(t, 65536, p["queue.buffering.max.kbytes"])
	assert.Equal(t, 100000, p["queue.buffering.max.messages"])
	assert.Equal(t, 1000000, p["message.max.bytes"])
	assert.Equal(t, "lz4", p["compression.type"])
	assert.Equal(t, "all", p["acks"])
	assert.Equal(t, true, p["enable.idempotence"])
	assert.Equal(t, 5000, p["message.timeout.ms"])
	assert.Equal(t, "b1:9092,b2:9092", p["bootstrap.servers"])

	c, err := consumerProperties(cfg)
	require.NoError(t, err)
	assert.Equal(t, 4096, c["queued.max.messages.kbytes"])
	assert.Equal(t, 4<<20, c["fetch.max.bytes"])
	assert.Equal(t, 4<<20+512, c["receive.message.max.bytes"])
	assert.Equal(t, 1<<20, c["max.partition.fetch.bytes"])
	assert.Equal(t, "cooperative-sticky", c["partition.assignment.strategy"])
	assert.Equal(t, false, c["enable.auto.commit"])
	assert.Equal(t, false, c["enable.auto.offset.store"])
	assert.Equal(t, "earliest", c["auto.offset.reset"])

	// librdkafka accepts the result: every property name and value is valid.
	pr, err := ckafka.NewProducer(&p)
	require.NoError(t, err)
	pr.Close()
	co, err := ckafka.NewConsumer(&c)
	require.NoError(t, err)
	require.NoError(t, co.Close())
}

func TestProperties_SecurityAndPassthrough(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(pw, []byte("s3cret\n"), 0o600))
	cfg := defaulted(t, kafka.Config{Brokers: []string{"b:9093"}, SecurityProtocol: "SASL_SSL", SASLMechanism: "scram-sha-512",
		SASLUsername: "app", SASLPasswordFile: pw, SSLCALocation: "/certs/ca.crt",
		ExtraProperties: kafka.Properties{"socket.timeout.ms": "30000"}, ConsumerExtraProperties: kafka.Properties{"fetch.wait.max.ms": "100"}})
	c, err := consumerProperties(cfg)
	require.NoError(t, err)
	assert.Equal(t, "sasl_ssl", c["security.protocol"])
	assert.Equal(t, "SCRAM-SHA-512", c["sasl.mechanism"])
	assert.Equal(t, "app", c["sasl.username"])
	assert.Equal(t, "s3cret", c["sasl.password"], "read from the file, trailing newline dropped")
	assert.Equal(t, "/certs/ca.crt", c["ssl.ca.location"])
	assert.Equal(t, "30000", c["socket.timeout.ms"])
	assert.Equal(t, "100", c["fetch.wait.max.ms"])
	p, err := producerProperties(cfg)
	require.NoError(t, err)
	assert.NotContains(t, p, "fetch.wait.max.ms", "consumer passthrough stays on consumers")
}

func TestSendError_Taxonomy(t *testing.T) {
	for code, want := range map[ckafka.ErrorCode]string{
		ckafka.ErrQueueFull:                kafka.KindQueueFull,
		ckafka.ErrMsgTimedOut:              kafka.KindUnavailable,
		ckafka.ErrUnknownTopicOrPart:       kafka.KindNotProvisioned,
		ckafka.ErrTopicAuthorizationFailed: kafka.KindNotProvisioned,
		ckafka.ErrMsgSizeTooLarge:          kafka.KindRejected,
		ckafka.ErrInvalidRecord:            kafka.KindRejected,
		ckafka.ErrUnknown:                  kafka.KindUnknown,
	} {
		err := sendError(ckafka.NewError(code, "x", false))
		var se *kafka.SendError
		require.ErrorAs(t, err, &se)
		assert.Equal(t, want, se.Kind, code.String())
		assert.Equal(t, want != kafka.KindRejected, se.Retryable)
	}
	assert.NoError(t, sendError(nil))
}

func TestParseStats(t *testing.T) {
	s, err := parseStats(`{"msg_size":10,"msg_size_max":67108864,
	  "brokers":{"b:9092/1":{"nodeid":1,"state":"UP","req_timeouts":1,"txerrs":2,"rxerrs":3},
	             "b:9092/bootstrap":{"nodeid":-1,"state":"UP"},
	             "c:9092/2":{"nodeid":2,"state":"DOWN"}},
	  "topics":{"t":{"partitions":{"0":{"consumer_lag":5},"1":{"consumer_lag":7},"-1":{"consumer_lag":100},"2":{"consumer_lag":-1}}}}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, s.brokersUp, "the bootstrap entry is not a broker")
	assert.Equal(t, int64(6), s.requestErrors)
	assert.Equal(t, int64(10), s.queueBytes)
	assert.Equal(t, int64(67108864), s.queueMaxBytes)
	assert.Equal(t, int64(7), s.lagMax)
	assert.Equal(t, int64(12), s.lagSum, "the UA partition and unknown lag are skipped")
	_, err = parseStats("{")
	require.Error(t, err)
}

// A broker connection failure is retried by librdkafka itself: a warning, not
// an error (localhost resolves to [::1] first, so it shows up on every boot).
// Every other level-3 line stays an error.
func TestLogLevel(t *testing.T) {
	cases := []struct {
		ev   ckafka.LogEvent
		want slog.Level
	}{
		{ckafka.LogEvent{Level: 3, Tag: "FAIL"}, slog.LevelWarn},
		{ckafka.LogEvent{Level: 3, Tag: "ERROR"}, slog.LevelError},
		{ckafka.LogEvent{Level: 0, Tag: "SSL"}, slog.LevelError},
		{ckafka.LogEvent{Level: 4, Tag: "MAXPOLL"}, slog.LevelWarn},
		{ckafka.LogEvent{Level: 6, Tag: "CONFWARN"}, slog.LevelInfo},
		{ckafka.LogEvent{Level: 7, Tag: "BROKER"}, slog.LevelDebug},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, logLevel(c.ev), "%d/%s", c.ev.Level, c.ev.Tag)
	}
}
