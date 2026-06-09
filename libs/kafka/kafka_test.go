package kafka_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := kafka.LoadConfig("TASKS_")
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	require.Equal(t, "tasks.events", cfg.Topic)
	require.Equal(t, []string{"tasks.events"}, cfg.Topics, "Topics falls back to the single Topic")

	t.Setenv("TASKS_KAFKA_BROKERS", "a:9092,b:9092")
	t.Setenv("TASKS_KAFKA_TOPICS", "x,y")
	cfg, err = kafka.LoadConfig("TASKS_")
	require.NoError(t, err)
	require.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)
	require.Equal(t, []string{"x", "y"}, cfg.Topics)
}

// startKafka spins up an ephemeral Kafka (KRaft, no ZooKeeper) via
// testcontainers and returns the broker seed list. Skips under -short or when
// Docker is unavailable.
func startKafka(t *testing.T) []string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}

	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			require.NoError(t, err, "kafka container must start in CI")
		}
		t.Skipf("could not start kafka container (docker unavailable?): %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	return brokers
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	brokers := startKafka(t)
	ctx := context.Background()

	const topic = "tasks.events.test"
	prodCfg := kafka.Config{Brokers: brokers, Topic: topic, ClientID: "test-producer", DialTimeout: 10 * time.Second}
	prod, err := kafka.NewProducer(ctx, prodCfg, nil)
	require.NoError(t, err)
	defer prod.Close()

	require.NoError(t, prod.ReadyCheck()(ctx))

	const n = 5
	for i := range n {
		err := prod.Publish(ctx, topic, fmt.Appendf(nil, "key-%d", i), fmt.Appendf(nil, "value-%d", i))
		require.NoError(t, err)
	}

	consCfg := kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: "test-group",
		ClientID: "test-consumer", DialTimeout: 10 * time.Second,
	}
	cons, err := kafka.NewConsumer(ctx, consCfg, nil)
	require.NoError(t, err)
	defer cons.Close()

	var (
		mu   sync.Mutex
		got  = map[string]string{}
		done = make(chan struct{})
	)
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	go func() {
		_ = cons.Run(runCtx, func(_ context.Context, msg kafka.Message) error {
			mu.Lock()
			got[string(msg.Key)] = string(msg.Value)
			full := len(got) == n
			mu.Unlock()
			if full {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-runCtx.Done():
		t.Fatal("timed out waiting for all records")
	}
	cancel()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, n)
	for i := range n {
		require.Equal(t, fmt.Sprintf("value-%d", i), got[fmt.Sprintf("key-%d", i)])
	}
}

func TestConsumerStopsOnContextCancel(t *testing.T) {
	brokers := startKafka(t)
	ctx := context.Background()

	cons, err := kafka.NewConsumer(ctx, kafka.Config{
		Brokers: brokers, Topics: []string{"tasks.events.idle"}, Group: "idle-group",
		ClientID: "idle-consumer", DialTimeout: 10 * time.Second,
	}, nil)
	require.NoError(t, err)
	defer cons.Close()

	runCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- cons.Run(runCtx, func(context.Context, kafka.Message) error { return nil }) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "a cancelled context is a clean shutdown, not an error")
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop on context cancel")
	}
}
