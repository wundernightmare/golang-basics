package httpx_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

const (
	testTraceID = "0102030405060708090a0b0c0d0e0f10"
	testSpanID  = "aabbccddeeff0011"
)

// tracedContext carries a sampled span context and a request id, the way a
// request context inside the server does.
func tracedContext() context.Context {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	return httpx.WithRequestID(ctx, "req-1")
}

func jsonLogger(buf *testx.LogBuffer) *slog.Logger {
	return httpx.NewLogger(httpx.LogConfig{Service: "svc", Level: "info", Format: "json", Writer: buf})
}

func TestLogger_StampsTraceSpanAndRequestFromContext(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := jsonLogger(buf)
	log.InfoContext(tracedContext(), "with trace")
	log.Info("without trace")

	lines := buf.Lines(t)
	require.Len(t, lines, 2)
	assert.Equal(t, testTraceID, lines[0]["trace_id"])
	assert.Equal(t, testSpanID, lines[0]["span_id"])
	assert.Equal(t, "req-1", lines[0]["request_id"])
	assert.Equal(t, "svc", lines[0]["service"])
	for _, k := range []string{"trace_id", "span_id", "request_id"} {
		assert.NotContains(t, lines[1], k)
	}
}

// A backend joins logs to traces on a top-level trace_id: a logger scoped
// with WithGroup must not bury the correlation ids inside the group.
func TestLogger_CorrelationIDsStayTopLevelUnderWithGroup(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := jsonLogger(buf).With("component", "store").WithGroup("db").With("table", "tasks").WithGroup("q")
	log.InfoContext(tracedContext(), "query", "rows", 3)

	lines := buf.Lines(t)
	require.Len(t, lines, 1)
	line := lines[0]
	assert.Equal(t, testTraceID, line["trace_id"])
	assert.Equal(t, testSpanID, line["span_id"])
	assert.Equal(t, "req-1", line["request_id"])
	assert.Equal(t, "store", line["component"], "attrs before the group stay top-level")
	assert.Equal(t, "svc", line["service"])
	db, ok := line["db"].(map[string]any)
	require.True(t, ok, "the group is kept: %v", line)
	assert.Equal(t, "tasks", db["table"])
	assert.Equal(t, map[string]any{"rows": float64(3)}, db["q"])
	assert.NotContains(t, db, "trace_id")

	// Without a trace the grouped logger still works and adds nothing.
	log.Info("plain", "rows", 1)
	plain := buf.Find(t, map[string]any{"msg": "plain"})
	require.NotNil(t, plain)
	assert.NotContains(t, plain, "trace_id")
	assert.Equal(t, map[string]any{"table": "tasks", "q": map[string]any{"rows": float64(1)}}, plain["db"])
}

func TestLogger_TextFormat(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "debug", Format: "text", Writer: buf})
	log.DebugContext(tracedContext(), "hello", "k", "v")
	out := buf.String()
	assert.Contains(t, out, "msg=hello")
	assert.Contains(t, out, "trace_id="+testTraceID)
	assert.Contains(t, out, "request_id=req-1")
}

func TestLogger_UnknownLevelFallsBackToInfo(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "chatty", Format: "json", Writer: buf})
	log.Debug("hidden")
	log.Info("shown")
	assert.Len(t, buf.Lines(t), 1)
	assert.Equal(t, slog.LevelInfo, httpx.LogLevelOf(log).Base())
}

func TestLogger_WritesToStdoutByDefault(t *testing.T) {
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json"}) // Writer nil
	os.Stdout = orig
	log.Info("to stdout")
	require.NoError(t, w.Close())
	var out bytes.Buffer
	_, _ = out.ReadFrom(r)
	assert.Contains(t, out.String(), `"msg":"to stdout"`)
}

func TestLogger_SamplesInfoButNeverWarn(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{
		Level: "info", Format: "json", Writer: buf, SampleInitial: 3, SampleThereafter: 10, SampleTick: time.Minute,
	})
	for range 30 {
		log.Info("hot loop")
	}
	for range 30 {
		log.Warn("always")
	}
	log.Info("other message") // its own budget

	lines := buf.Lines(t)
	// 30 records: first 3 pass, then every 10th of the remainder (13th, 23rd) → 5.
	assert.Len(t, filterMsg(lines, "hot loop"), 5)
	assert.Len(t, filterMsg(lines, "always"), 30)
	assert.Len(t, filterMsg(lines, "other message"), 1)
}

func TestLogger_SamplingOffWhenInitialIsZero(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	for range 500 {
		log.Info("unsampled")
	}
	assert.Len(t, buf.Lines(t), 500)
}

func TestLogger_DroppedCountIsExported(t *testing.T) {
	// Records below the level never reach the sampler, so they are not
	// counted as dropped.
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{
		Level: "error", Format: "json", Writer: buf, SampleInitial: 1, SampleThereafter: 1000, SampleTick: time.Minute,
	})
	for range 10 {
		log.Info("filtered")
	}
	assert.Empty(t, buf.String())
	assert.Equal(t, 0.0, droppedInfo(t, log))

	// With info enabled the drops are counted and surface on the registry.
	log = httpx.NewLogger(httpx.LogConfig{
		Level: "info", Format: "json", Writer: buf, SampleInitial: 1, SampleThereafter: 1000, SampleTick: time.Minute,
	})
	for range 10 {
		log.Info("dropped")
	}
	assert.Equal(t, 9.0, droppedInfo(t, log))
}

// droppedInfo reads log_dropped_total{level="info"} off a fresh registry
// built around log, exactly as NewServer wires it.
func droppedInfo(t testing.TB, log *slog.Logger) float64 {
	t.Helper()
	m := httpx.NewMetrics(httpx.Build("t"), log)
	return testx.Metric(t, m.Registry, "log_dropped_total", map[string]string{"level": "info"})
}

func TestLogLevelOf(t *testing.T) {
	log := httpx.NewLogger(httpx.LogConfig{Level: "warn", Writer: &testx.LogBuffer{}})
	lvl := httpx.LogLevelOf(log)
	require.NotNil(t, lvl)
	assert.Same(t, lvl, httpx.LogLevelOf(log.With("k", "v").WithGroup("g")), "derived loggers share the control")
	assert.Nil(t, httpx.LogLevelOf(slog.New(slog.DiscardHandler)))

	prev := lvl.Set(slog.LevelDebug, 0)
	assert.Equal(t, slog.LevelWarn, prev)
	level, expires := lvl.Level()
	assert.Equal(t, slog.LevelDebug, level)
	assert.True(t, expires.IsZero(), "ttl 0 is permanent until reset")
	assert.Equal(t, slog.LevelDebug, lvl.Reset())
	level, _ = lvl.Level()
	assert.Equal(t, slog.LevelWarn, level)
}

func TestBuild_ReportsServiceAndVersion(t *testing.T) {
	b := httpx.Build("svc")
	assert.Equal(t, "svc", b.Service)
	assert.Equal(t, httpx.Version, b.Version)
	assert.NotEmpty(t, b.GoVersion)
	assert.NotEmpty(t, b.Revision)
}

func TestSignalContext_CancelledByTheFirstSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no SIGTERM delivery to self on windows")
	}
	ctx, stop := httpx.SignalContext()
	defer stop()
	require.NoError(t, ctx.Err())

	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, self.Signal(syscall.SIGTERM))
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("SIGTERM did not cancel the context")
	}
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	// The handler is released right after the first signal, so a second one
	// gets the default disposition and terminates the process — not asserted
	// by sending one, for obvious reasons.
}
