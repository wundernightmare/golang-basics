package valkey

import (
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func TestMetricsAreLintClean(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics()
	reg.MustRegister(m.lookups, m.duration, m.errors)

	for _, r := range []string{"hit", "miss", "error"} {
		m.lookups.WithLabelValues(r).Inc()
	}

	for _, op := range []string{"get", "set", "del", "publish"} {
		m.duration.WithLabelValues(op).Observe(0.001)
		m.errors.WithLabelValues(op).Inc()
	}

	testx.LintMetrics(t, reg)
}

// One line per interval, and the next one says how many it stands for.
func TestWarnGatePassesOnePerInterval(t *testing.T) {
	t.Parallel()

	var gate warnGate

	start := time.Now()

	dropped, ok := gate.pass(start)
	assert.True(t, ok)
	assert.Zero(t, dropped)

	for _, later := range []time.Duration{time.Millisecond, warnInterval - time.Nanosecond} {
		_, ok = gate.pass(start.Add(later))
		assert.False(t, ok, "%s after a logged warning", later)
	}

	dropped, ok = gate.pass(start.Add(warnInterval))
	assert.True(t, ok)
	assert.Equal(t, 2, dropped)

	dropped, ok = gate.pass(start.Add(2 * warnInterval))
	assert.True(t, ok)
	assert.Zero(t, dropped, "the count starts over")

	// The line carries the count only when something was left out.
	var (
		logged warnGate
		logs   testx.LogBuffer
	)

	log := slog.New(slog.NewJSONHandler(&logs, nil))

	logged.log(t.Context(), log, "cache failed")
	logged.log(t.Context(), log, "cache failed")
	logged.next = time.Time{} // the interval is over
	logged.log(t.Context(), log, "cache failed")

	lines := logs.Lines(t)
	require.Len(t, lines, 2)
	assert.NotContains(t, lines[0], "suppressed")
	assert.EqualValues(t, 1, lines[1]["suppressed"])
}
