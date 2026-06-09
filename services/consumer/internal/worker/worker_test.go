package worker_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/services/consumer/internal/worker"
)

// counterValue reads a named counter out of a registry by gathering it.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			var sum float64
			for _, m := range mf.GetMetric() {
				sum += m.GetCounter().GetValue()
			}
			return sum
		}
	}
	return 0
}

// fakeConsumer drives the worker's handler with a fixed set of messages, no
// broker required.
type fakeConsumer struct{ msgs []kafka.Message }

func (f *fakeConsumer) Run(ctx context.Context, h kafka.Handler) error {
	for _, m := range f.msgs {
		if err := h(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func TestWorkerCountsConsumedAndSkipped(t *testing.T) {
	valid, _ := json.Marshal(map[string]any{"id": "1", "title": "a"})
	fc := &fakeConsumer{msgs: []kafka.Message{
		{Value: valid},
		{Value: []byte("not json")},
		{Value: valid},
	}}
	reg := prometheus.NewRegistry()
	w := worker.New(fc, slog.New(slog.DiscardHandler), reg)

	require.NoError(t, w.Run(context.Background()))

	require.Equal(t, float64(2), counterValue(t, reg, "consumer_tasks_consumed_total"))
	require.Equal(t, float64(1), counterValue(t, reg, "consumer_tasks_skipped_total"))
}

func TestWorkerConsumesFromRealKafka(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	ctx := context.Background()
	const topic = "tasks.events.consumer-it"

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			require.NoError(t, err)
		}
		t.Skipf("docker unavailable (kafka): %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	// Produce two valid task.created events.
	prod, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: brokers, Topic: topic, ClientID: "consumer-it-prod", DialTimeout: 10 * time.Second,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer prod.Close()
	for _, id := range []string{"a", "b"} {
		payload, _ := json.Marshal(map[string]any{"id": id, "title": "t-" + id, "created_at": time.Now().UTC()})
		require.NoError(t, prod.Publish(ctx, topic, []byte(id), payload))
	}

	// Run the worker over a real consumer.
	cons, err := kafka.NewConsumer(ctx, kafka.Config{
		Brokers: brokers, Topics: []string{topic}, Group: "consumer-it-group",
		ClientID: "consumer-it", DialTimeout: 10 * time.Second,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer cons.Close()

	reg := prometheus.NewRegistry()
	w := worker.New(cons, slog.New(slog.DiscardHandler), reg)

	runCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	require.Eventually(t, func() bool {
		return counterValue(t, reg, "consumer_tasks_consumed_total") == 2
	}, 25*time.Second, 250*time.Millisecond, "worker should consume both events")
}
