package httpx_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what
// was written. NewLogger writes to stdout by design (any log collector tails
// container output), so the tests read it back the same way.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	done := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r)
		done <- buf.String()
	}()
	fn()
	os.Stdout = orig
	_ = w.Close()
	return <-done
}

func TestLogger_StampsTraceAndSpanFromContext(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	out := captureStdout(t, func() {
		log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json"})
		log.InfoContext(ctx, "with trace")
		log.Info("without trace")
	})

	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], `"trace_id":"0102030405060708090a0b0c0d0e0f10"`)
	assert.Contains(t, lines[0], `"span_id":"aabbccddeeff0011"`)
	assert.NotContains(t, lines[1], "trace_id")
}

func TestLogger_SamplesInfoButNeverWarn(t *testing.T) {
	out := captureStdout(t, func() {
		log := httpx.NewLogger(httpx.LogConfig{
			Level: "info", Format: "text", SampleInitial: 3, SampleThereafter: 10, SampleTick: time.Minute,
		})
		for range 30 {
			log.Info("hot loop")
		}
		for range 30 {
			log.Warn("always")
		}
		log.Info("other message") // its own budget
	})

	count := func(msg string) int {
		return strings.Count(out, "msg=\""+msg+"\"") + strings.Count(out, "msg="+msg+"\n")
	}
	// 30 records: first 3 pass, then every 10th of the remainder (10th, 20th) → 5.
	assert.Equal(t, 5, count("hot loop"))
	assert.Equal(t, 30, count("always"))
	assert.Equal(t, 1, count("other message"))
}

func TestLogger_SamplingOffWhenInitialIsZero(t *testing.T) {
	out := captureStdout(t, func() {
		log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "text"})
		for range 500 {
			log.Info("unsampled")
		}
	})
	assert.Equal(t, 500, strings.Count(out, "msg=unsampled"))
}

func TestLogger_DroppedCountIsExported(t *testing.T) {
	// Level filtering happens in the JSON/text handler's Enabled, which slog
	// consults before Handle, so records below the level never reach the
	// sampler and are not counted as dropped.
	log := httpx.NewLogger(httpx.LogConfig{
		Level: "error", Format: "text", SampleInitial: 1, SampleThereafter: 1000, SampleTick: time.Minute,
	})
	out := captureStdout(t, func() {
		for range 10 {
			log.Info("filtered")
		}
	})
	assert.Empty(t, out)
	assert.Equal(t, float64(0), droppedInfo(t, log))

	// With info enabled the drops are counted and surface on the registry.
	log = httpx.NewLogger(httpx.LogConfig{
		Level: "info", Format: "text", SampleInitial: 1, SampleThereafter: 1000, SampleTick: time.Minute,
	})
	_ = captureStdout(t, func() {
		for range 10 {
			log.Info("dropped")
		}
	})
	assert.Equal(t, float64(9), droppedInfo(t, log))
}

// droppedInfo reads log_dropped_total{level="info"} off a fresh registry built
// around log, exactly as NewServer wires it.
func droppedInfo(t *testing.T, log *slog.Logger) float64 {
	t.Helper()
	m := httpx.NewMetrics(httpx.Build("t"), log)
	mfs, err := m.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "log_dropped_total" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			for _, l := range mm.GetLabel() {
				if l.GetName() == "level" && l.GetValue() == "info" {
					return mm.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestBuild_ReportsServiceAndVersion(t *testing.T) {
	b := httpx.Build("svc")
	assert.Equal(t, "svc", b.Service)
	assert.Equal(t, httpx.Version, b.Version)
	assert.NotEmpty(t, b.GoVersion)
	assert.NotEmpty(t, b.Revision)
}
