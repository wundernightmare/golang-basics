package httpx_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/secret"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

var theTrace = trace.TraceID{0: 0x4b, 15: 0x36}

func spanContext(t testing.TB, sampled bool, state string) trace.SpanContext {
	t.Helper()

	cfg := trace.SpanContextConfig{TraceID: theTrace, SpanID: trace.SpanID{7: 1}}
	if sampled {
		cfg.TraceFlags = trace.FlagsSampled
	}

	if state != "" {
		parsed, err := trace.ParseTraceState(state)
		require.NoError(t, err)

		cfg.TraceState = parsed
	}

	return trace.NewSpanContext(cfg)
}

func inSpan(t testing.TB, sampled bool, state string) context.Context {
	t.Helper()

	return trace.ContextWithSpanContext(context.Background(), spanContext(t, sampled, state))
}

// triggers is a retroactive buffer as the logger sees it.
type triggers struct {
	accept bool

	mu        sync.Mutex
	triggered map[trace.TraceID]bool
	byLog     []trace.TraceID
	asked     int
}

func (tr *triggers) TriggerByLog(id trace.TraceID) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tr.byLog = append(tr.byLog, id)
	if tr.accept {
		if tr.triggered == nil {
			tr.triggered = map[trace.TraceID]bool{}
		}

		tr.triggered[id] = true
	}

	return tr.accept
}

func (tr *triggers) IsTriggered(id trace.TraceID) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	tr.asked++

	return tr.triggered[id]
}

func bufferedLogger(format string) (*slog.Logger, *testx.LogBuffer) {
	buf := &testx.LogBuffer{}

	return httpx.NewLogger(httpx.LogConfig{Level: "info", Format: format, Writer: buf}), buf
}

// trace_id is a link to a trace; the id of a trace that was not sampled is
// logged under a name of its own.
func TestLogger_TraceIDIsALinkAndAnUnsampledTraceHasNone(t *testing.T) {
	t.Parallel()

	log, buf := bufferedLogger("json")
	log.InfoContext(inSpan(t, true, ""), "sampled")
	log.InfoContext(inSpan(t, false, ""), "unsampled")
	log.WarnContext(inSpan(t, false, ""), "unsampled warn")
	log.InfoContext(context.Background(), "no span")

	lines := buf.Lines(t)
	require.Len(t, lines, 4)

	assert.Equal(t, theTrace.String(), lines[0]["trace_id"])
	assert.NotContains(t, lines[0], "trace_unsampled_id")
	assert.Equal(t, "0000000000000001", lines[0]["span_id"])

	for _, line := range lines[1:3] {
		assert.Equal(t, theTrace.String(), line["trace_unsampled_id"], line["msg"])
		assert.NotContains(t, line, "trace_id", "nothing was exported: nothing to link to")
		assert.Equal(t, "0000000000000001", line["span_id"])
	}

	assert.NotContains(t, lines[3], "trace_id")
	assert.NotContains(t, lines[3], "trace_unsampled_id")
	assert.NotContains(t, lines[3], "span_id")

	ecs, ecsBuf := bufferedLogger("ecs")
	ecs.InfoContext(inSpan(t, true, ""), "sampled")
	ecs.InfoContext(inSpan(t, false, ""), "unsampled")

	lines = ecsBuf.Lines(t)
	assert.Equal(t, theTrace.String(), lines[0]["trace.id"])
	assert.Equal(t, theTrace.String(), lines[1]["trace.unsampled_id"])
	assert.NotContains(t, lines[1], "trace.id")
}

// With a retroactive buffer bound, a warn or error line of an unsampled
// trace triggers it — and carries trace_id when the buffer took the trigger.
func TestLogger_AWarnLineTriggersItsUnsampledTrace(t *testing.T) {
	t.Parallel()

	log, buf := bufferedLogger("json")
	buffer := &triggers{accept: true}
	require.True(t, httpx.BindTraceTrigger(log, buffer))

	unsampled := inSpan(t, false, "")

	log.InfoContext(unsampled, "before")
	log.WarnContext(unsampled, "slow")
	log.InfoContext(unsampled, "after")
	log.With("k", "v").WithGroup("g").ErrorContext(unsampled, "failed", "n", 1)

	lines := buf.Lines(t)
	require.Len(t, lines, 4)
	assert.Equal(t, theTrace.String(), lines[0]["trace_unsampled_id"], "written before the trigger")
	assert.Equal(t, theTrace.String(), lines[1]["trace_id"])
	assert.Equal(t, theTrace.String(), lines[2]["trace_id"], "an info line of a triggered trace links to it")
	assert.Equal(t, theTrace.String(), lines[3]["trace_id"], "a derived logger shares the binding, and the id stays at the top level")
	assert.Equal(t, []trace.TraceID{theTrace, theTrace}, buffer.byLog, "the warn and the error; an info line only asks")
	assert.Equal(t, 2, buffer.asked)

	// A sampled trace is none of the buffer's business.
	log.ErrorContext(inSpan(t, true, ""), "sampled")
	log.ErrorContext(context.Background(), "no span")
	assert.Len(t, buffer.byLog, 2)
	assert.Equal(t, 2, buffer.asked)
}

func TestLogger_ARefusedTriggerLeavesTheLineUnlinked(t *testing.T) {
	t.Parallel()

	log, buf := bufferedLogger("json")
	require.True(t, httpx.BindTraceTrigger(log, &triggers{accept: false}))

	log.ErrorContext(inSpan(t, false, ""), "failed")

	line := buf.Lines(t)[0]
	assert.Equal(t, theTrace.String(), line["trace_unsampled_id"], "over the rate limit: the trace stays unsampled")
	assert.NotContains(t, line, "trace_id")
}

func TestBindTraceTrigger(t *testing.T) {
	t.Parallel()

	assert.False(t, httpx.BindTraceTrigger(slog.New(slog.DiscardHandler), &triggers{}), "not a logger of NewLogger")

	log, buf := bufferedLogger("json")
	child := log.With("component", "x")
	buffer := &triggers{accept: true}

	require.True(t, httpx.BindTraceTrigger(child, buffer), "bound through a derived logger")
	log.WarnContext(inSpan(t, false, ""), "parent")
	assert.Len(t, buffer.byLog, 1, "and it holds for the whole family")

	require.True(t, httpx.BindTraceTrigger(log, nil))
	log.WarnContext(inSpan(t, false, ""), "unbound")
	assert.Len(t, buffer.byLog, 1)
	assert.Equal(t, theTrace.String(), buf.Lines(t)[1]["trace_unsampled_id"])
}

func TestForceTrace(t *testing.T) {
	t.Parallel()

	assert.False(t, httpx.TraceForced(context.Background()))
	assert.True(t, httpx.TraceForced(httpx.ForceTrace(context.Background())))

	derived, cancel := context.WithCancel(httpx.ForceTrace(context.Background()))
	defer cancel()

	assert.True(t, httpx.TraceForced(derived), "inherited")
}

func TestIsDebugTrace(t *testing.T) {
	t.Parallel()

	const mark = httpx.DebugTraceStateKey + "=" + httpx.DebugTraceStateValue

	assert.Equal(t, "dbg=1", mark, "the mark is a contract between services: changing it splits a pipeline mid-deploy")

	assert.True(t, httpx.IsDebugTrace(spanContext(t, true, mark)))
	assert.True(t, httpx.IsDebugTrace(spanContext(t, true, "vendor=x,"+mark)))
	assert.False(t, httpx.IsDebugTrace(spanContext(t, false, mark)), "the mark without the sampled flag is not a debug trace")
	assert.False(t, httpx.IsDebugTrace(spanContext(t, true, httpx.DebugTraceStateKey+"=other")))
	assert.False(t, httpx.IsDebugTrace(spanContext(t, true, "")))
	assert.False(t, httpx.IsDebugTrace(trace.SpanContext{}))
}

func TestFollowDebugTrace(t *testing.T) {
	t.Parallel()

	const mark = httpx.DebugTraceStateKey + "=" + httpx.DebugTraceStateValue

	assert.True(t, httpx.DebugLogging(httpx.FollowDebugTrace(inSpan(t, true, mark))))
	assert.False(t, httpx.DebugLogging(httpx.FollowDebugTrace(inSpan(t, true, ""))))
	assert.False(t, httpx.DebugLogging(httpx.FollowDebugTrace(inSpan(t, false, mark))))
	assert.False(t, httpx.DebugLogging(httpx.FollowDebugTrace(context.Background())))

	already := httpx.WithDebugLogging(context.Background())
	assert.Equal(t, already, httpx.FollowDebugTrace(already), "nothing to add")

	plain := inSpan(t, true, "")
	assert.Equal(t, plain, httpx.FollowDebugTrace(plain), "the context itself when there is nothing to follow")
}

// forcedSampler samples exactly the spans started under ForceTrace, marking
// their trace — the part of libs/otelx's sampler this package relies on.
type forcedSampler struct{}

func (forcedSampler) Description() string { return "forced" }

func (forcedSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	parent := trace.SpanContextFromContext(p.ParentContext)
	state := parent.TraceState()

	switch {
	case httpx.TraceForced(p.ParentContext):
		state, _ = state.Insert(httpx.DebugTraceStateKey, httpx.DebugTraceStateValue)

		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state}
	case parent.IsSampled():
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state}
	default:
		return sdktrace.SamplingResult{Decision: sdktrace.RecordOnly, Tracestate: state}
	}
}

func tracedDebugServer(t *testing.T, cfg httpx.Config) (*httpx.Server, *testx.LogBuffer, *bool, *trace.SpanContext) {
	t.Helper()

	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(forcedSampler{}))

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var (
		debugging bool
		seen      trace.SpanContext
	)

	srv, buf := loggedServer(t, cfg, httpx.WithTracerProvider(provider), httpx.WithPropagator(propagation.TraceContext{}))
	srv.Mux().HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) {
		debugging = httpx.DebugLogging(r.Context())
		seen = trace.SpanContextFromContext(r.Context())
		srv.Logger().DebugContext(r.Context(), "detail")
		w.WriteHeader(http.StatusNoContent)
	})

	return srv, buf, &debugging, &seen
}

func work(srv *httpx.Server, headers ...string) {
	req := httptest.NewRequest(http.MethodGet, "/work", http.NoBody)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
}

// A request with the debug token asks the sampler for its trace before the
// server span starts; one without it does not.
func TestServer_TheDebugTokenForcesTheTrace(t *testing.T) {
	t.Parallel()

	srv, buf, debugging, seen := tracedDebugServer(t, httpx.Config{DebugToken: secret.New("let-me-see")})

	work(srv)
	assert.False(t, *debugging)
	assert.False(t, seen.IsSampled())
	assert.False(t, httpx.IsDebugTrace(*seen))

	work(srv, httpx.DebugTokenHeader, "wrong")
	assert.False(t, seen.IsSampled())

	work(srv, httpx.DebugTokenHeader, "let-me-see")
	assert.True(t, *debugging)
	assert.True(t, httpx.IsDebugTrace(*seen), "sampled and marked: the services behind will follow")
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "detail", "trace_id": seen.TraceID().String()}))
}

// A request that arrives inside a debug trace is logged from debug up; the
// mark of an unsampled trace, or a sampled trace without it, changes nothing.
func TestServer_FollowsADebugTraceStartedUpstream(t *testing.T) {
	t.Parallel()

	const (
		sampledParent   = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		unsampledParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
		mark            = httpx.DebugTraceStateKey + "=" + httpx.DebugTraceStateValue
	)

	srv, buf, debugging, _ := tracedDebugServer(t, httpx.Config{})

	work(srv, "Traceparent", sampledParent)
	assert.False(t, *debugging, "a sampled trace is not a debug trace")

	work(srv, "Traceparent", unsampledParent, "Tracestate", mark)
	assert.False(t, *debugging)
	assert.Nil(t, buf.Find(t, map[string]any{"msg": "detail"}))

	work(srv, "Traceparent", sampledParent, "Tracestate", mark)
	assert.True(t, *debugging)
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "detail", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"}))
}

// A subject under investigation: its unsampled trace is handed to the
// retroactive buffer; a sampled one needs nothing, nor does another subject.
func TestDebugSubject_TriggersTheUnsampledTrace(t *testing.T) {
	t.Parallel()

	srv, _ := loggedServer(t, httpx.Config{AdminAddr: "127.0.0.1:0"})
	buffer := &triggers{accept: true}
	require.True(t, httpx.BindTraceTrigger(srv.Logger(), buffer))

	code, _ := admin(t, srv, http.MethodPut, "/admin/debug-subjects/acme?ttl=1m")
	require.Equal(t, http.StatusOK, code)

	assert.False(t, httpx.DebugLogging(srv.DebugSubject(inSpan(t, false, ""), "other")))
	assert.Empty(t, buffer.byLog)

	assert.True(t, httpx.DebugLogging(srv.DebugSubject(inSpan(t, true, ""), "acme")))
	assert.Empty(t, buffer.byLog, "a sampled trace is exported already")

	assert.True(t, httpx.DebugLogging(srv.DebugSubject(context.Background(), "acme")))
	assert.Empty(t, buffer.byLog, "no trace, nothing to export")

	assert.True(t, httpx.DebugLogging(srv.DebugSubject(inSpan(t, false, ""), "acme")))
	assert.Equal(t, []trace.TraceID{theTrace}, buffer.byLog)
}
