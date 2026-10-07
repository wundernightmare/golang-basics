package valkey

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func TestMetricsAreLintClean(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics()
	reg.MustRegister(m.lookups, m.duration, m.errors)
	for _, r := range []string{"hit", "miss", "error"} {
		m.lookups.WithLabelValues(r).Inc()
	}
	for _, op := range []string{"get", "set", "del"} {
		m.duration.WithLabelValues(op).Observe(0.001)
		m.errors.WithLabelValues(op).Inc()
	}
	testx.LintMetrics(t, reg)
}

func TestJitterStaysWithinTenPercent(t *testing.T) {
	const ttl = time.Minute
	seen := map[time.Duration]bool{}
	for range 1000 {
		j := Jitter(ttl)
		assert.GreaterOrEqual(t, j, 54*time.Second)
		assert.LessOrEqual(t, j, 66*time.Second)
		seen[j] = true
	}
	assert.Greater(t, len(seen), 100, "the TTL is actually spread")
	assert.Equal(t, time.Duration(0), Jitter(0))
	assert.Equal(t, 5*time.Nanosecond, Jitter(5*time.Nanosecond), "too small to spread")
}
