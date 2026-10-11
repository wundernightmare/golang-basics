package kafka

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// The instrument set is a contract for dashboards and alerts: lint it the
// way Prometheus would (naming, units, help) before any broker is involved.
func TestMetrics_AreLintClean(t *testing.T) {
	stats := func() ClientStats {
		return ClientStats{Valid: true, BrokersUp: 3, RequestErrors: 2, QueueBytes: 10, QueueMaxBytes: 64 << 20, LagMax: 7, LagSum: 12}
	}
	reg := prometheus.NewRegistry()
	pm := newProducerMetrics(stats, "test")
	cm := newConsumerMetrics(stats, "test")
	reg.MustRegister(pm.records, pm.duration, pm.client)
	reg.MustRegister(cm.records, cm.duration, cm.commitErrs, cm.dlq, cm.fetchErrs, cm.assigned, cm.rebalances, cm.client)
	pm.records.WithLabelValues("t", "ok").Inc()
	pm.duration.WithLabelValues("t").Observe(0.01)
	cm.records.WithLabelValues("t", consumeRetried).Inc()
	cm.duration.WithLabelValues("t").Observe(0.01)
	cm.dlq.WithLabelValues("t.dlq").Inc()
	cm.rebalances.WithLabelValues("assigned").Inc()

	testx.LintMetrics(t, reg)
	assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_client_info", map[string]string{"client": "consumer", "provider": "test"}))
	assert.Equal(t, 3.0, testx.Metric(t, reg, "kafka_client_brokers_up", map[string]string{"client": "producer"}))
	assert.Equal(t, 2.0, testx.Metric(t, reg, "kafka_client_request_errors_total", map[string]string{"client": "consumer"}))
	assert.Equal(t, float64(64<<20), testx.Metric(t, reg, "kafka_producer_queue_max_bytes", nil))
	assert.Equal(t, 7.0, testx.Metric(t, reg, "kafka_consumer_lag_max", nil))
	assert.Equal(t, 12.0, testx.Metric(t, reg, "kafka_consumer_lag", nil))
}

// A client that stopped reporting shows no client gauges rather than its
// last numbers.
func TestClientCollector_StaleSnapshotIsNotExposed(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newClientCollector("consumer", "test", func() ClientStats { return ClientStats{LagSum: 9} }))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "kafka_consumer_lag", nil))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "kafka_client_info", map[string]string{"client": "consumer", "provider": "test"}))
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
	m := Message{Topic: "t", Headers: []Header{{Key: "event_id", Value: []byte("e1")}, {Key: "event_id", Value: []byte("e2")}}}
	v, ok := m.Header("event_id")
	assert.True(t, ok)
	assert.Equal(t, "e1", string(v), "first wins")
	_, ok = m.Header("missing")
	assert.False(t, ok)
}

func TestRequestID_FromEventIDOrFresh(t *testing.T) {
	rec := func(id string) Message { return Message{Headers: []Header{{Key: HeaderEventID, Value: []byte(id)}}} }
	assert.Equal(t, "0190c8a2-7b4e-7c1d-9f00-000000000001", requestID(rec("0190c8a2-7b4e-7c1d-9f00-000000000001")))
	for _, bad := range []string{"", "has space", "new\nline", string(make([]byte, 200))} {
		id := requestID(rec(bad))
		assert.Len(t, id, 16, "%q is replaced by a fresh id", bad)
	}
	assert.Len(t, requestID(Message{}), 16)
}

func TestHeaderCarrier(t *testing.T) {
	hs := []Header{{Key: "traceparent", Value: []byte("old")}}
	c := headerCarrier{&hs}
	c.Set("traceparent", "new")
	c.Set("tracestate", "x=1")
	assert.Equal(t, "new", c.Get("traceparent"), "replaced, not duplicated")
	assert.Equal(t, []string{"traceparent", "tracestate"}, c.Keys())
}
