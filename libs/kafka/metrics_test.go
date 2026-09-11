package kafka

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The instrument set is a contract for dashboards and alerts: lint it the way
// Prometheus would (naming, units, help) before any broker is involved.
func TestMetricsAreLintClean(t *testing.T) {
	reg := prometheus.NewRegistry()
	pm, cm := newProducerMetrics(), newConsumerMetrics()
	reg.MustRegister(pm.records, pm.duration, cm.records, cm.handlerErrs, cm.fetchErrs, cm.duration, cm.lag)
	// Vectors expose nothing until observed; touch one series each.
	pm.records.WithLabelValues("t", "ok").Inc()
	pm.duration.WithLabelValues("t").Observe(0.01)
	cm.records.WithLabelValues("t").Inc()
	cm.handlerErrs.WithLabelValues("t").Inc()
	cm.fetchErrs.WithLabelValues("t").Inc()
	cm.duration.WithLabelValues("t").Observe(0.01)
	cm.lag.WithLabelValues("t", "0").Set(3)

	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)
}
