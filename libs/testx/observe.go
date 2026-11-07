package testx

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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
// labels include every pair in labels: a counter's or gauge's value, a
// histogram's sample count. A missing series returns -1, so "the error series
// does not exist" is an assertion, not a crash.
func Metric(tb testing.TB, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	tb.Helper()

	mfs, err := g.Gather()
	require.NoError(tb, err)

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}

	next:
		for _, m := range mf.GetMetric() {
			for k, v := range labels {
				if !hasLabel(m, k, v) {
					continue next
				}
			}

			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}

	return -1
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

	problems, err := testutil.GatherAndLint(g)
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
func (l *LogBuffer) Find(tb testing.TB, want map[string]any) map[string]any {
	tb.Helper()

next:
	for _, m := range l.Lines(tb) {
		for k, v := range want {
			if m[k] != v {
				continue next
			}
		}

		return m
	}

	return nil
}
