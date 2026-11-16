package testx

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TraceRecorder records every span its Provider produces, in memory, for the
// duration of the test. Nothing global is touched — pass Provider and
// Propagator to the code under test explicitly (httpx.WithTracerProvider,
// kafka.WithTracerProvider, pgx / valkey / resilient / outbox options, …), so
// tests that record spans can run in parallel. Code that falls back to the
// global provider records nothing here: a span assertion then fails loudly.
type TraceRecorder struct {
	*tracetest.SpanRecorder

	// Provider always samples and feeds the recorder.
	Provider *sdktrace.TracerProvider
	// Propagator is W3C trace context + baggage, the one otelx.Init installs.
	Propagator propagation.TextMapPropagator
}

// Tracer is a tracer from Provider, for the test's own parent spans.
func (r *TraceRecorder) Tracer() trace.Tracer { return r.Provider.Tracer("test") }

// Recorder returns a [TraceRecorder] whose provider is shut down at cleanup.
func Recorder(tb testing.TB) *TraceRecorder {
	tb.Helper()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr), sdktrace.WithSampler(sdktrace.AlwaysSample()))

	tb.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return &TraceRecorder{
		SpanRecorder: sr,
		Provider:     tp,
		Propagator:   propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	}
}

// Metric gathers g and returns the value of the first series of name whose
// labels include every pair in labels: a counter's, gauge's or untyped
// metric's value, a histogram's or summary's sample count. A missing series
// returns -1 — and so does a metric name or label key that does not exist:
// assert that a series is absent with [NoMetric], which tells the two apart.
func Metric(tb testing.TB, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	tb.Helper()

	mfs, err := g.Gather()
	require.NoError(tb, err)

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}

	next:
		for _, series := range mf.GetMetric() {
			for k, v := range labels {
				if !hasLabel(series, k, v) {
					continue next
				}
			}

			switch {
			case series.GetCounter() != nil:
				return series.GetCounter().GetValue()
			case series.GetGauge() != nil:
				return series.GetGauge().GetValue()
			case series.GetHistogram() != nil:
				return float64(series.GetHistogram().GetSampleCount())
			case series.GetSummary() != nil:
				return float64(series.GetSummary().GetSampleCount())
			case series.GetUntyped() != nil:
				return series.GetUntyped().GetValue()
			}
		}
	}

	return -1
}

// NoMetric fails the test unless the series is absent from a metric family
// that is there. [Metric] answers -1 to a misspelt metric name or label key
// as well, so an "== -1" assertion keeps passing after a rename; NoMetric
// requires the family to be gathered and every key of labels to be one of its
// label names. A family is gathered only once it has a series: record another
// one (the success beside the error) before asserting that one is missing.
func NoMetric(tb testing.TB, gatherer prometheus.Gatherer, name string, labels map[string]string) {
	tb.Helper()

	mfs, err := gatherer.Gather()
	require.NoError(tb, err)

	i := slices.IndexFunc(mfs, func(mf *dto.MetricFamily) bool { return mf.GetName() == name })
	if i < 0 || len(mfs[i].GetMetric()) == 0 {
		tb.Errorf("testx: metric family %q is not gathered: an absent series cannot be told from a misspelt name", name)

		return
	}

	// client_golang gives every series of a family the same label names.
	known := mfs[i].GetMetric()[0].GetLabel()
	for k := range labels {
		if !slices.ContainsFunc(known, func(l *dto.LabelPair) bool { return l.GetName() == k }) {
			tb.Errorf("testx: metric %q has no label %q", name, k)
		}
	}

	if v := Metric(tb, gatherer, name, labels); v >= 0 {
		tb.Errorf("testx: series %s%v exists (value %v)", name, labels, v)
	}
}

func hasLabel(m *dto.Metric, k, v string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == k && l.GetValue() == v {
			return true
		}
	}

	return false
}

// LintMetrics fails the test if the exposition of g would draw promlint
// findings (naming, units, help): metric names are a contract for dashboards.
func LintMetrics(tb testing.TB, g prometheus.Gatherer) {
	tb.Helper()

	mfs, err := g.Gather()
	require.NoError(tb, err)

	problems, err := promlint.NewWithMetricFamilies(mfs).Lint()
	require.NoError(tb, err)
	require.Empty(tb, problems, "promlint findings")
}

// LogBuffer is an io.Writer for httpx.LogConfig.Writer that captures the
// real logger's output in memory, safe for concurrent writers.
type LogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *LogBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.b.Write(p)
}

// String returns everything written so far.
func (l *LogBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.b.String()
}

// Lines parses every JSON record written so far, in order.
func (l *LogBuffer) Lines(tb testing.TB) []map[string]any {
	tb.Helper()

	var out []map[string]any

	for line := range strings.SplitSeq(strings.TrimSpace(l.String()), "\n") {
		if line == "" {
			continue
		}

		var m map[string]any
		require.NoError(tb, json.Unmarshal([]byte(line), &m), line)
		out = append(out, m)
	}

	return out
}

// Find returns the first parsed line whose fields match every pair in want.
// Values compare as testify's EqualValues does: encoding/json decodes every
// number as float64, so want may say 500 for the field 500.
func (l *LogBuffer) Find(tb testing.TB, want map[string]any) map[string]any {
	tb.Helper()

next:
	for _, m := range l.Lines(tb) {
		for k, v := range want {
			if !assert.ObjectsAreEqualValues(v, m[k]) {
				continue next
			}
		}

		return m
	}

	return nil
}
