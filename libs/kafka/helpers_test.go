package kafka

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
	"github.com/stretchr/testify/require"
)

// metric gathers g and returns the value of the first series of name whose
// labels include every pair in labels — a histogram's sample count — or -1
// when there is none. Local, not libs/testx: that module's span recorder
// would put the OpenTelemetry SDK into this module's go.mod.
func metric(tb testing.TB, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	tb.Helper()

	families, err := g.Gather()
	require.NoError(tb, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

	next:
		for _, series := range family.GetMetric() {
			have := map[string]string{}
			for _, l := range series.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}

			for k, v := range labels {
				if have[k] != v {
					continue next
				}
			}

			if series.GetHistogram() != nil {
				return float64(series.GetHistogram().GetSampleCount())
			}

			return series.GetCounter().GetValue() + series.GetGauge().GetValue()
		}
	}

	return -1
}

// lintMetrics fails the test on a promlint finding (naming, units, help):
// metric names are a contract for dashboards.
func lintMetrics(tb testing.TB, g prometheus.Gatherer) {
	tb.Helper()

	families, err := g.Gather()
	require.NoError(tb, err)

	problems, err := promlint.NewWithMetricFamilies(families).Lint()
	require.NoError(tb, err)
	require.Empty(tb, problems, "promlint findings")
}
