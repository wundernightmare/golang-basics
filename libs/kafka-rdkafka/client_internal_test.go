package rdkafka

import (
	"context"
	"log/slog"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// No broker needed: librdkafka builds clients without connecting, and a
// record to an unreachable broker just waits in the local queue.
func offline(t *testing.T) kafka.Config {
	t.Helper()
	cfg := kafka.Config{Brokers: []string{"127.0.0.1:1"}, Topics: []string{"t"}, Group: "g",
		PublishTimeout: 300 * time.Millisecond}
	require.NoError(t, cfg.Validate())
	return cfg
}

// A fatal error (lost idempotent producer state) replaces the client; the
// producer keeps working on the new one.
func TestProducer_FatalErrorReplacesClient(t *testing.T) {
	p, err := newProducer(offline(t), slog.New(slog.DiscardHandler), "producer")
	require.NoError(t, err)
	defer p.Close()

	failed := p.cl.Load()
	require.Equal(t, ckafka.ErrNoError, failed.TestFatalError(ckafka.ErrOutOfOrderSequenceNumber, "test"))
	require.Eventually(t, func() bool { return p.cl.Load() != failed }, 10*time.Second, 10*time.Millisecond)

	p.replace(failed) // a late second report about the same client is a no-op
	assert.NotSame(t, failed, p.cl.Load())
}

// Send gives up with a retryable error when ctx ends before the delivery
// report; a fatal librdkafka error is KindFatal.
func TestProducer_SendBoundedByContext(t *testing.T) {
	p, err := newProducer(offline(t), slog.New(slog.DiscardHandler), "producer")
	require.NoError(t, err)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = p.Send(ctx, kafka.Message{Topic: "t", Value: []byte("x"), Headers: []kafka.Header{{Key: "h", Value: []byte("v")}}})
	var se *kafka.SendError
	require.ErrorAs(t, err, &se)
	assert.True(t, se.Retryable)

	require.ErrorAs(t, sendError(ckafka.NewError(ckafka.ErrFatal, "fenced", true)), &se)
	assert.Equal(t, kafka.KindFatal, se.Kind)
}

// The dead-letter producer is built on first use; one that cannot be built
// fails every send with that reason instead of panicking the loop.
func TestConsumer_DeadLetterProducerThatCannotBeBuilt(t *testing.T) {
	cfg := offline(t)
	cfg.ProducerExtraProperties = kafka.Properties{"no.such.property": "1"} // producers only: the consumer still builds
	cl, err := Driver{}.NewConsumer(cfg, slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.NoError(t, err)
	defer cl.Close()

	cl.AllowRebalance() // librdkafka rebalances inside Poll: nothing to release
	dlq := cl.DeadLetter()
	err = dlq.Send(context.Background(), kafka.Message{Topic: "t.dlq"})
	var se *kafka.SendError
	require.ErrorAs(t, err, &se)
	require.ErrorContains(t, err, "no.such.property")
	require.Error(t, dlq.Ping(context.Background()))
	assert.False(t, dlq.Stats().Valid)
	dlq.Close()
}

func TestNewConsumer_Errors(t *testing.T) {
	cfg := offline(t)
	cfg.ConsumerExtraProperties = kafka.Properties{"no.such.property": "1"}
	_, err := Driver{}.NewConsumer(cfg, slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.ErrorContains(t, err, "no.such.property")

	cfg = offline(t)
	cfg.SASLPasswordFile = "/nonexistent/password"
	cfg.SecurityProtocol, cfg.SASLMechanism, cfg.SASLUsername = "sasl_plaintext", "SCRAM-SHA-512", "u"
	_, err = Driver{}.NewConsumer(cfg, slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.Error(t, err)
	_, err = Driver{}.NewProducer(cfg, slog.New(slog.DiscardHandler))
	require.Error(t, err)
}
