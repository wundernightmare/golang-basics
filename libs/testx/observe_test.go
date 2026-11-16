package testx_test

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/wundernightmare/golang-basics/libs/testx"
)

// records is a testing.TB that notes a failure instead of failing the test
// that looks at the outcome.
type records struct {
	testing.TB

	failures []string
}

func (r *records) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// jobsRegistry holds jobs_failed_total{reason="timeout"} = 3 and the summary
// job_seconds{queue="mail"} with two observations.
func jobsRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()

	failed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jobs_failed_total", Help: "Failed jobs."}, []string{"reason"})
	failed.WithLabelValues("timeout").Add(3)

	seconds := prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "job_seconds", Help: "Job duration."}, []string{"queue"})
	seconds.WithLabelValues("mail").Observe(1)
	seconds.WithLabelValues("mail").Observe(2)

	reg.MustRegister(failed, seconds)

	return reg
}

func TestMetric(t *testing.T) {
	t.Parallel()

	reg := jobsRegistry()

	assert.InDelta(t, 3.0, testx.Metric(t, reg, "jobs_failed_total", map[string]string{"reason": "timeout"}), 0)
	assert.InDelta(t, 2.0, testx.Metric(t, reg, "job_seconds", map[string]string{"queue": "mail"}), 0,
		"a summary reads as its sample count, like a histogram")
	assert.InDelta(t, -1.0, testx.Metric(t, reg, "jobs_failed_total", map[string]string{"reason": "refused"}), 0)
}

// NoMetric passes only for a series that is absent from a family that is
// there: a misspelt metric name or label key must not read as "no such series".
func TestNoMetric(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		metric string
		labels map[string]string
		fails  bool
	}{
		"absent series of a known family": {metric: "jobs_failed_total", labels: map[string]string{"reason": "refused"}},
		"series exists":                   {metric: "jobs_failed_total", labels: map[string]string{"reason": "timeout"}, fails: true},
		"unknown metric name":             {metric: "job_failed_total", labels: map[string]string{"reason": "refused"}, fails: true},
		"unknown label key":               {metric: "jobs_failed_total", labels: map[string]string{"cause": "refused"}, fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tb := &records{TB: t}
			testx.NoMetric(tb, jobsRegistry(), tc.metric, tc.labels)

			assert.Equal(t, tc.fails, len(tb.failures) > 0, tb.failures)
		})
	}
}

// JSON has one number type and no tuples: a want written with Go's int or a
// slice must match what encoding/json decoded.
func TestLogBufferFind(t *testing.T) {
	t.Parallel()

	var buf testx.LogBuffer

	_, _ = buf.Write([]byte(`{"msg":"done","status":500,"tags":["a"]}` + "\n"))

	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "done", "status": 500}))
	assert.NotNil(t, buf.Find(t, map[string]any{"tags": []any{"a"}}))
	assert.Nil(t, buf.Find(t, map[string]any{"status": 404}))
}
