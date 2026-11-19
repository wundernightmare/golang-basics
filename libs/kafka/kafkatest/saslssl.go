package kafkatest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/wundernightmare/golang-basics/libs/kafka"
	"github.com/wundernightmare/golang-basics/libs/testx"
	"github.com/wundernightmare/golang-basics/libs/testx/containers"
)

func saslSSLConfig(t *testing.T) kafka.Config {
	t.Helper()

	brokers := os.Getenv("KAFKA_SASL_SSL_BROKERS")
	username := os.Getenv("KAFKA_SASL_SSL_USERNAME")
	passwordFile := os.Getenv("KAFKA_SASL_SSL_PASSWORD_FILE")
	caFile := os.Getenv("KAFKA_SASL_SSL_CA_LOCATION")
	mechanism := "SCRAM-SHA-512"

	if brokers == "" {
		pki, err := containers.NewPKI()
		require.NoError(t, err)

		brokers = containers.Fixture(t, "redpanda-sasl-ssl", func(ctx context.Context) (string, testcontainers.Container, error) {
			ctr, err := tcredpanda.Run(ctx, containers.RedpandaImage,
				tcredpanda.WithTLS(pki.ServerCert, pki.ServerKey),
				tcredpanda.WithEnableSASL(),
				tcredpanda.WithNewServiceAccount("app", "app-secret"),
				tcredpanda.WithSuperusers("app"),
				tcredpanda.WithAutoCreateTopics())
			if err != nil {
				return "", ctr, err
			}

			broker, err := ctr.KafkaSeedBroker(ctx)

			return broker, ctr, err
		})
		caFile, _, _ = pki.ClientFiles(t)
		username, mechanism = "app", "SCRAM-SHA-256"
		passwordFile = filepath.Join(t.TempDir(), "app.password")
		require.NoError(t, os.WriteFile(passwordFile, []byte("app-secret"), 0o600))
	}

	topic := os.Getenv("KAFKA_SASL_SSL_TOPIC")
	if topic == "" {
		topic = "tasks.events"
	}

	return kafka.Config{
		Brokers: strings.Split(brokers, ","), Topic: topic,
		Group: testx.Unique("sasl-ssl"), ClientID: testx.Unique("sasl-ssl"),
		SecurityProtocol: "sasl_ssl", SASLMechanism: mechanism,
		SASLUsername: username, SASLPasswordFile: passwordFile, SSLCALocation: caFile,
		AutoOffsetReset: "earliest", AllowAutoTopicCreation: true,
		RetryBackoff: 10 * time.Millisecond, RetryBackoffMax: 50 * time.Millisecond,
	}
}

// SASLSSL produces and consumes one record through SASL_SSL with a private
// CA. With KAFKA_SASL_SSL_BROKERS it uses the examples/kafka-sasl-ssl broker
// and its SCRAM-SHA-512 account; otherwise a per-run Redpanda container with
// SCRAM-SHA-256 is started instead of skipping.
//
//	KAFKA_SASL_SSL_BROKERS        localhost:29094
//	KAFKA_SASL_SSL_CA_LOCATION    examples/kafka-sasl-ssl/certs/ca.crt
//	KAFKA_SASL_SSL_USERNAME       app
//	KAFKA_SASL_SSL_PASSWORD_FILE  examples/kafka-sasl-ssl/certs/app.password
//	KAFKA_SASL_SSL_TOPIC          a topic the user may write and read (default "tasks.events")
func SASLSSL(t *testing.T) {
	t.Helper()

	cfg := saslSSLConfig(t)
	producer, err := kafka.NewProducer(t.Context(), cfg, nil)
	require.NoError(t, err, "connect over SASL_SSL with the custom CA")

	defer producer.Close()

	want := testx.Unique("sasl-ssl-record")
	got := make(chan string, 1)

	require.NoError(t, producer.Send(t.Context(), kafka.Message{Value: []byte(want)}))

	c, err := kafka.NewConsumer(t.Context(), cfg, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, func(_ context.Context, m kafka.Message) error {
			got <- string(m.Value)
			return nil
		})
	}()

	defer func() { cancel(); assert.NoError(t, <-done, "Run returns nil once ctx is cancelled") }()

	select {
	case v := <-got:
		require.Equal(t, want, v)
	case <-time.After(60 * time.Second):
		t.Fatal("no record came back through the SASL_SSL listener")
	}
}
