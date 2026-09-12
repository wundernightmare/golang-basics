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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Recorder installs a global tracer provider (the one otelx.Init would
// install, and the one the libs' instrumentation reads) that records every
// span in memory for the duration of the test. Call it before the client
// under test is built: the instrumentation captures the provider then.
func Recorder(tb testing.TB) *tracetest.SpanRecorder {
	tb.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	tb.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return sr
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
	for _, line := range strings.Split(strings.TrimSpace(l.String()), "\n") {
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
