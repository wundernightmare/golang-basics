package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// The instrument set is a contract for dashboards and alerts: lint it the way
// Prometheus would (naming, units, help) before any broker is involved.
func TestMetrics_AreLintClean(t *testing.T) {
	reg := prometheus.NewRegistry()
	pm, cm := newProducerMetrics(), newConsumerMetrics()
	reg.MustRegister(pm.records, pm.duration)
	reg.MustRegister(cm.records, cm.duration, cm.commitErrs, cm.dlq, cm.fetchErrs, cm.assigned, cm.rebalances, cm.lag)
	// Vectors expose nothing until observed; touch one series each.
	pm.records.WithLabelValues("t", resultOK).Inc()
	pm.duration.WithLabelValues("t").Observe(0.01)
	cm.records.WithLabelValues("t", consumeRetried).Inc()
	cm.duration.WithLabelValues("t").Observe(0.01)
	cm.commitErrs.Inc()
	cm.dlq.WithLabelValues("t.dlq").Inc()
	cm.fetchErrs.WithLabelValues("t").Inc()
	cm.assigned.WithLabelValues("t").Set(2)
	cm.rebalances.WithLabelValues("assigned").Inc()
	cm.lag.set([]lagPoint{{topic: "t", partition: 0, lag: 3}})
	cm.lag.pollErrors.Inc()

	testx.LintMetrics(t, reg)
	assert.Equal(t, 3.0, testx.Metric(t, reg, "kafka_consumer_group_lag", map[string]string{"topic": "t", "partition": "0"}))
	assert.Equal(t, 0.0, testx.Metric(t, reg, "kafka_consumer_lag_last_success_timestamp_seconds", nil), "no successful poll yet")
}

// A scrape racing a poll sees a whole snapshot — never an emptied vector
// half-way through a refill.
func TestLagCollector_SwapsWholeSnapshots(t *testing.T) {
	lc := newLagCollector()
	reg := prometheus.NewRegistry()
	reg.MustRegister(lc)
	full := func(v int64) []lagPoint {
		pts := make([]lagPoint, 8)
		for i := range pts {
			pts[i] = lagPoint{topic: "t", partition: int32(i), lag: v}
		}
		return pts
	}
	lc.set(full(0))

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		for v := int64(1); ctx.Err() == nil; v++ {
			lc.set(full(v))
		}
	})
	for range 200 {
		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() == "kafka_consumer_group_lag" {
				require.Len(t, mf.GetMetric(), 8, "a scrape never sees a partial set")
			}
		}
	}
	cancel()
	wg.Wait()

	lc.drop(map[string][]int32{"t": {0, 1, 2}})
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "kafka_consumer_group_lag" {
			assert.Len(t, mf.GetMetric(), 5, "revoked partitions leave the snapshot at once")
		}
	}
}

func TestPublishResult_IsBounded(t *testing.T) {
	cases := map[error]string{
		nil:                                             resultOK,
		context.DeadlineExceeded:                        resultTimeout,
		kgo.ErrRecordTimeout:                            resultTimeout,
		kerr.MessageTooLarge:                            resultTooLarge,
		kerr.RecordListTooLarge:                         resultTooLarge,
		kerr.SaslAuthenticationFailed:                   resultAuth,
		kerr.TopicAuthorizationFailed:                   resultAuth,
		kerr.UnknownTopicOrPartition:                    resultUnknownTopic,
		errors.New("connection reset"):                  resultOther,
		fmt.Errorf("wrapped: %w", kerr.MessageTooLarge): resultTooLarge,
	}
	for err, want := range cases {
		assert.Equal(t, want, publishResult(err), "%v", err)
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")
	require.NoError(t, Permanent(nil))
	p := Permanent(base)
	assert.True(t, IsPermanent(p))
	assert.True(t, IsPermanent(fmt.Errorf("handler: %w", p)), "survives wrapping")
	require.ErrorIs(t, p, base)
	assert.Equal(t, "bad payload", p.Error())
	assert.False(t, IsPermanent(base))
}

func TestBackoff_ExponentialJitteredCapped(t *testing.T) {
	initial, maxDelay := 100*time.Millisecond, time.Second
	for n, ceiling := range []time.Duration{100, 200, 400, 800, 1000, 1000, 1000} {
		ceiling *= time.Millisecond
		for range 50 {
			d := backoff(n, initial, maxDelay)
			assert.GreaterOrEqual(t, d, ceiling/2, "retry %d", n)
			assert.LessOrEqual(t, d, ceiling, "retry %d", n)
		}
	}
	assert.LessOrEqual(t, backoff(1000, initial, maxDelay), maxDelay, "no overflow on a large retry count")
}

func TestMessage_Header(t *testing.T) {
	m := toMessage(&kgo.Record{Topic: "t", Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte("e1")}, {Key: "event_id", Value: []byte("e2")}}}, 1)
	v, ok := m.Header("event_id")
	assert.True(t, ok)
	assert.Equal(t, "e1", string(v), "first wins")
	_, ok = m.Header("missing")
	assert.False(t, ok)
	assert.Equal(t, 1, m.Attempt)
}

func TestRequestID_FromEventIDOrFresh(t *testing.T) {
	rec := func(id string) *kgo.Record {
		return &kgo.Record{Headers: []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte(id)}}}
	}
	assert.Equal(t, "0190c8a2-7b4e-7c1d-9f00-000000000001", requestID(rec("0190c8a2-7b4e-7c1d-9f00-000000000001")))
	for _, bad := range []string{"", "has space", "new\nline", string(make([]byte, 200))} {
		id := requestID(rec(bad))
		assert.Len(t, id, 16, "%q is replaced by a fresh id", bad)
	}
	assert.Len(t, requestID(&kgo.Record{}), 16)
}
