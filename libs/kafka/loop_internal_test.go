package kafka

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// What the loop promises about one record, against the scripted client: each
// case runs one record through Run and reads the outcome off the client and
// the registry.
//
//nolint:paralleltest // serial: the provider registry holds one driver per binary (Register); use() configures that shared fake for the next constructor
func TestConsumer_Loop(t *testing.T) {
	record := Message{Topic: "t", Partition: 0, Offset: 3, Value: []byte("x")}
	next := []Offset{{TopicPartition: TopicPartition{"t", 0}, Next: 4}}

	for _, tc := range []struct {
		name    string
		client  *fakeConsumer
		handler func(ctx context.Context, stop context.CancelFunc) error
		until   func(fc *fakeConsumer, reg *prometheus.Registry) bool
		check   func(t *testing.T, fc *fakeConsumer, reg *prometheus.Registry)
	}{
		{
			name:    "a handler panic is an error of the record: retried, then parked and committed",
			client:  &fakeConsumer{dlq: &fakeProducer{}},
			handler: func(context.Context, context.CancelFunc) error { panic("nil map") },
			check: func(t *testing.T, fc *fakeConsumer, _ *prometheus.Registry) {
				t.Helper()

				_, sent := fc.dlq.sends()
				require.Len(t, sent, 1)
				attempts, _ := sent[0].Header(HeaderDLQAttempts)
				assert.Equal(t, "2", string(attempts), "the first attempt and MaxRetries retries")
				assert.Equal(t, next, fc.committed()[0])
			},
		},
		{
			name:    "a dead-letter publish that fails is retried",
			client:  &fakeConsumer{dlq: &fakeProducer{failFirst: 1}},
			handler: func(context.Context, context.CancelFunc) error { return Permanent(errors.New("undecodable")) },
			check: func(t *testing.T, fc *fakeConsumer, _ *prometheus.Registry) {
				t.Helper()

				attempts, sent := fc.dlq.sends()
				assert.Equal(t, 2, attempts)
				assert.Len(t, sent, 1)
			},
		},
		{
			name:    "a fetch error is counted and the loop keeps polling",
			client:  &fakeConsumer{pollErrs: []error{errors.New("broker down")}},
			handler: func(context.Context, context.CancelFunc) error { return nil },
			check: func(t *testing.T, _ *fakeConsumer, reg *prometheus.Registry) {
				t.Helper()
				assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_consumer_fetch_errors_total", nil))
			},
		},
		{
			name:    "a failed commit is counted",
			client:  &fakeConsumer{commitErr: errors.New("coordinator not available")},
			handler: func(context.Context, context.CancelFunc) error { return nil },
			until: func(_ *fakeConsumer, reg *prometheus.Registry) bool {
				return testx.Metric(t, reg, "kafka_consumer_commit_errors_total", nil) >= 1
			},
		},
		{
			name:   "a record finished during shutdown is committed with a context the shutdown did not cancel",
			client: &fakeConsumer{},
			handler: func(_ context.Context, stop context.CancelFunc) error {
				stop()
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.client
			client.batches = [][]Message{{record}}
			use(t, client, nil, nil)

			c, err := NewConsumer(context.Background(), testConfig(), nil)
			require.NoError(t, err)
			client.rb.Assigned(context.Background(), []TopicPartition{{"t", 0}})

			reg := prometheus.NewRegistry()
			reg.MustRegister(c.Collectors()...)

			ctx, stop := context.WithCancel(context.Background())
			defer stop()

			done := make(chan error, 1)
			go func() {
				done <- c.Run(ctx, func(ctx context.Context, _ Message) error { return tc.handler(ctx, stop) })
			}()

			until := tc.until
			if until == nil {
				until = func(fc *fakeConsumer, _ *prometheus.Registry) bool { return len(fc.committed()) == 1 }
			}

			require.Eventually(t, func() bool { return until(client, reg) }, 5*time.Second, time.Millisecond)
			stop()
			require.NoError(t, <-done)

			if tc.check != nil {
				tc.check(t, client, reg)
			}
		})
	}
}

// A retry waits out the backoff schedule: at least half of
// KAFKA_CONSUMER_RETRY_BACKOFF (equal jitter), never the next attempt at once.
//
//nolint:paralleltest // serial: the provider registry holds one driver per binary (Register); use() configures that shared fake for the next constructor
func TestConsumer_RetryWaitsOutTheBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeConsumer{batches: [][]Message{{{Topic: "t"}}}}
		use(t, fc, nil, nil)

		cfg := testConfig()
		cfg.RetryBackoff, cfg.RetryBackoffMax = 10*time.Second, 10*time.Second
		c, err := NewConsumer(t.Context(), cfg, nil)
		require.NoError(t, err)
		fc.rb.Assigned(t.Context(), []TopicPartition{{"t", 0}})

		ctx, stop := context.WithCancel(t.Context())

		var attempts []time.Time

		err = c.Run(ctx, func(_ context.Context, m Message) error {
			attempts = append(attempts, time.Now())

			if m.Attempt == 1 {
				return errors.New("downstream unavailable")
			}

			stop()

			return nil
		})
		require.NoError(t, err)
		require.Len(t, attempts, 2)

		wait := attempts[1].Sub(attempts[0])
		assert.GreaterOrEqual(t, wait, 5*time.Second)
		assert.LessOrEqual(t, wait, 10*time.Second)
	})
}

// Run is single-use and needs a handler; Close may be called any number of
// times, Run's own close included.
//
//nolint:paralleltest // serial: the provider registry holds one driver per binary (Register); use() configures that shared fake for the next constructor
func TestConsumer_RunAndCloseMisuse(t *testing.T) {
	handler := func(context.Context, Message) error { return nil }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("a nil handler is refused before anything is polled", func(t *testing.T) {
		fc := &fakeConsumer{dlq: &fakeProducer{}, batches: [][]Message{{{Topic: "t"}}}}
		use(t, fc, nil, nil)

		c, err := NewConsumer(context.Background(), testConfig(), nil)
		require.NoError(t, err)
		fc.rb.Assigned(context.Background(), []TopicPartition{{"t", 0}})

		require.ErrorContains(t, c.Run(context.Background(), nil), "nil Handler")
		assert.Len(t, fc.batches, 1, "nothing was polled")
		assert.Empty(t, fc.committed())
	})

	t.Run("a second Run is refused; Close is idempotent", func(t *testing.T) {
		fc := &fakeConsumer{}
		use(t, fc, nil, nil)

		c, err := NewConsumer(context.Background(), testConfig(), nil)
		require.NoError(t, err)

		require.NoError(t, c.Run(cancelled, handler))
		require.ErrorContains(t, c.Run(cancelled, handler), "called twice")
		c.Close()
		c.Close()
		assert.Equal(t, 1, fc.closes)
	})
}

// A producer's Send: bounded by the publish budget plus a second whatever
// the caller's context allows, not attempted with a context that is already
// over, and refused after Close with an error that says so.
//
//nolint:paralleltest // serial: the provider registry holds one driver per binary (Register); use() configures that shared fake for the next constructor
func TestProducer_SendBoundsAndClose(t *testing.T) {
	client := &fakeProducer{}
	use(t, nil, client, nil)

	cfg := testConfig()
	cfg.PublishTimeout = 3 * time.Second
	producer, err := NewProducer(context.Background(), cfg, nil)
	require.NoError(t, err)

	require.NoError(t, producer.Send(context.Background(), Message{Value: []byte("x")}))
	require.Len(t, client.budgets, 1)
	assert.InDelta(t, 4*time.Second, client.budgets[0], float64(time.Second/2))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	var se *SendError

	err = producer.Send(cancelled, Message{Value: []byte("x")})
	require.ErrorAs(t, err, &se)
	assert.Equal(t, KindUnavailable, se.Kind)
	require.ErrorIs(t, err, context.Canceled)

	producer.Close()
	producer.Close()

	err = producer.Send(context.Background(), Message{Value: []byte("x")})
	require.ErrorIs(t, err, ErrClientClosed)
	require.ErrorAs(t, err, &se)
	assert.Equal(t, KindUnavailable, se.Kind)
	require.ErrorIs(t, producer.ReadyCheck()(context.Background()), ErrClientClosed)

	attempts, _ := client.sends()
	assert.Equal(t, 1, attempts, "neither refused send reached the client")
	assert.Equal(t, 1, client.closes)
}

// Two producers of one process register on one registry through a labelled
// registerer each (README "Observability").
//
//nolint:paralleltest // serial: the provider registry holds one driver per binary (Register); use() configures that shared fake for the next constructor
func TestProducer_TwoOnOneRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()

	for _, name := range []string{"events", "audit"} {
		use(t, nil, &fakeProducer{}, nil)

		p, err := NewProducer(context.Background(), testConfig(), nil)
		require.NoError(t, err)
		prometheus.WrapRegistererWith(prometheus.Labels{"producer": name}, reg).MustRegister(p.Collectors()...)
		require.NoError(t, p.Send(context.Background(), Message{Topic: "t"}))
	}

	for _, name := range []string{"events", "audit"} {
		assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_producer_records_total", map[string]string{"producer": name, "result": "ok"}))
	}
}

// A binary that links no provider is told so by the constructors.
//
//nolint:paralleltest // serial: unlinks the binary's one provider (Register) for the duration of the test
func TestConstructors_NoProviderLinked(t *testing.T) {
	driverMu.Lock()
	linked := driver
	driver = nil
	driverMu.Unlock()

	defer func() {
		driverMu.Lock()
		driver = linked
		driverMu.Unlock()
	}()

	_, err := NewProducer(context.Background(), testConfig(), nil)
	require.ErrorContains(t, err, "no provider linked")

	_, err = NewConsumer(context.Background(), testConfig(), nil)
	require.ErrorContains(t, err, "no provider linked")
}
