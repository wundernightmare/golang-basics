package otelx

// The retroactive buffer and the debug trace through the real stack: the SDK
// provider Init builds (newProvider) in front of an in-memory exporter, the
// real httpx logger and server. Providers are passed explicitly, so these
// run in parallel.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/otelx/retro"
	"github.com/wundernightmare/golang-basics/libs/secret"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

// pipeline is one service's tracing and logging, as Init wires them.
type pipeline struct {
	t        *testing.T
	provider *sdktrace.TracerProvider
	exported *tracetest.InMemoryExporter
	buffer   *retro.Processor
	log      *slog.Logger
	lines    *testx.LogBuffer
	prop     propagation.TextMapPropagator
}

func newPipeline(t *testing.T, cfg Config) *pipeline {
	t.Helper()

	cfg.withDefaults()

	pipe := &pipeline{
		t: t, exported: tracetest.NewInMemoryExporter(), lines: &testx.LogBuffer{},
		prop: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	}
	pipe.log = httpx.NewLogger(httpx.LogConfig{Service: "svc", Level: "info", Format: "json", Writer: pipe.lines})
	pipe.provider, pipe.buffer = newProvider(cfg, pipe.exported, resource.NewSchemaless(attribute.String("service.name", "svc")))

	if pipe.buffer != nil {
		require.True(t, httpx.BindTraceTrigger(pipe.log, pipe.buffer))
	}

	t.Cleanup(func() { _ = pipe.provider.Shutdown(context.Background()) })

	return pipe
}

// spans is what reached the exporter: name → sampled flag.
func (pipe *pipeline) spans() map[string]bool {
	pipe.t.Helper()

	require.NoError(pipe.t, pipe.provider.ForceFlush(context.Background()))

	out := map[string]bool{}
	for _, span := range pipe.exported.GetSpans() {
		out[span.Name] = span.SpanContext.IsSampled()
	}

	return out
}

// ids is, for every line, which of the two trace keys it carries.
func (pipe *pipeline) ids() []string {
	pipe.t.Helper()

	var out []string

	for _, line := range pipe.lines.Lines(pipe.t) {
		msg, _ := line["msg"].(string)

		_, linked := line["trace_id"]
		_, unsampled := line["trace_unsampled_id"]

		switch {
		case linked && unsampled:
			out = append(out, msg+": both")
		case linked:
			out = append(out, msg+": trace_id")
		case unsampled:
			out = append(out, msg+": trace_unsampled_id")
		default:
			out = append(out, msg+": none")
		}
	}

	return out
}

// work is a message handled at the given sampling: kafka.consume → decode
// (ended before body runs) and → store (open while it runs). It returns the
// traceparent the store span would send downstream.
func (pipe *pipeline) work(body func(ctx context.Context)) string {
	tracer := pipe.provider.Tracer("svc")

	ctx, consume := tracer.Start(context.Background(), "kafka.consume")
	_, decode := tracer.Start(ctx, "decode")
	decode.End()

	ctx, store := tracer.Start(ctx, "store")
	body(ctx)

	carrier := propagation.MapCarrier{}
	pipe.prop.Inject(ctx, carrier)

	store.End()
	consume.End()

	return carrier.Get("traceparent")
}

func never() Config { return Config{SamplerRatio: -1, TrustRemoteParent: true} }

func TestRetro_AWarnLineExportsTheWholeTraceOfTheProcess(t *testing.T) {
	t.Parallel()

	pipe := newPipeline(t, never())
	traceparent := pipe.work(func(ctx context.Context) {
		pipe.log.InfoContext(ctx, "before")
		pipe.log.WarnContext(ctx, "slow store")
		pipe.log.InfoContext(ctx, "after")
	})

	// The span that ended before the trigger (from the arena) and the two
	// that were still open (at their end), all exported as sampled.
	assert.Equal(t, map[string]bool{"decode": true, "kafka.consume": true, "store": true}, pipe.spans())
	assert.Equal(t, []string{
		"before: trace_unsampled_id", // written before the trigger: no trace yet to link to
		"slow store: trace_id",
		"after: trace_id",
	}, pipe.ids())
	assert.Regexp(t, "-00$", traceparent, "recording a span for the buffer must not make the next service sample it")
}

func TestRetro_AnErrorLineTriggersAndAnInfoLineDoesNot(t *testing.T) {
	t.Parallel()

	failed := newPipeline(t, never())
	failed.work(func(ctx context.Context) { failed.log.ErrorContext(ctx, "failed") })
	assert.Len(t, failed.spans(), 3)
	assert.Equal(t, []string{"failed: trace_id"}, failed.ids())

	fine := newPipeline(t, never())
	fine.work(func(ctx context.Context) { fine.log.InfoContext(ctx, "stored") })
	assert.Empty(t, fine.spans())
	assert.Equal(t, []string{"stored: trace_unsampled_id"}, fine.ids())
}

// A logger derived from the service's — With, WithGroup — triggers as its
// parent does: the binding belongs to the family.
func TestRetro_DerivedLoggersTriggerToo(t *testing.T) {
	t.Parallel()

	pipe := newPipeline(t, never())
	pipe.work(func(ctx context.Context) {
		pipe.log.With("component", "store").WithGroup("db").WarnContext(ctx, "slow store", "rows", 3)
	})

	assert.Len(t, pipe.spans(), 3)
	assert.Equal(t, []string{"slow store: trace_id"}, pipe.ids())
}

func TestRetro_WithoutTheBufferAWarnLineExportsNothing(t *testing.T) {
	t.Parallel()

	cfg := never()
	cfg.Retro.BufferMiB = -1

	pipe := newPipeline(t, cfg)
	require.Nil(t, pipe.buffer)

	traceparent := pipe.work(func(ctx context.Context) { pipe.log.WarnContext(ctx, "slow store") })

	assert.Empty(t, pipe.spans())
	assert.Equal(t, []string{"slow store: trace_unsampled_id"}, pipe.ids(), "an id, and not a link to a trace that does not exist")
	assert.Regexp(t, "-00$", traceparent)
}

func TestRetro_ASampledTraceNeverReachesTheBuffer(t *testing.T) {
	t.Parallel()

	pipe := newPipeline(t, Config{SamplerRatio: 1, TrustRemoteParent: true})
	traceparent := pipe.work(func(ctx context.Context) {
		pipe.log.InfoContext(ctx, "stored")
		pipe.log.WarnContext(ctx, "slow store")
	})

	assert.Equal(t, map[string]bool{"decode": true, "kafka.consume": true, "store": true}, pipe.spans())
	assert.Equal(t, []string{"stored: trace_id", "slow store: trace_id"}, pipe.ids())
	assert.Regexp(t, "-01$", traceparent)

	families := gather(t, pipe.buffer)
	assert.InDelta(t, 0, families["trace_retro_spans_total/kept"], 0)
	assert.InDelta(t, 0, families["trace_retro_triggers_total/accepted"], 0, "a warn line of a sampled trace is not a trigger")
}

// A storm of errors does not become a storm of exports: above the budget a
// trace stays unsampled, and its lines say so.
func TestRetro_TheTriggerBudgetBoundsTheExports(t *testing.T) {
	t.Parallel()

	cfg := never()
	cfg.Retro.TriggersPerSecond = 2

	pipe := newPipeline(t, cfg)

	for range 6 {
		pipe.work(func(ctx context.Context) { pipe.log.WarnContext(ctx, "slow store") })
	}

	ids := pipe.ids()
	sort.Strings(ids)
	assert.Equal(t, []string{
		"slow store: trace_id", "slow store: trace_id",
		"slow store: trace_unsampled_id", "slow store: trace_unsampled_id",
		"slow store: trace_unsampled_id", "slow store: trace_unsampled_id",
	}, ids)

	require.NoError(t, pipe.provider.ForceFlush(context.Background()))
	assert.Len(t, pipe.exported.GetSpans(), 2*3)

	families := gather(t, pipe.buffer)
	assert.InDelta(t, 2, families["trace_retro_triggers_total/accepted"], 0)
	assert.InDelta(t, 4, families["trace_retro_triggers_total/rate_limited"], 0)
}

// gather reads the buffer's families: "family/outcome" → value.
func gather(t *testing.T, buffer *retro.Processor) map[string]float64 {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(buffer.Collectors()...)
	testx.LintMetrics(t, reg)

	out := map[string]float64{}

	for _, family := range []string{"trace_retro_spans_total", "trace_retro_triggers_total"} {
		for _, outcome := range []string{"kept", "exported", "evicted", "accepted", "repeated", "rate_limited"} {
			if (family == "trace_retro_spans_total") != (outcome == "kept" || outcome == "exported" || outcome == "evicted") {
				continue
			}

			out[family+"/"+outcome] = testx.Metric(t, reg, family, map[string]string{"outcome": outcome})
		}
	}

	return out
}

// ── Debug traces ─────────────────────────────────────────────────────────

const debugToken = "let-me-see"

// service is an httpx server on a pipeline of its own.
func (pipe *pipeline) service(name string, cfg httpx.Config, handle func(srv *httpx.Server, w http.ResponseWriter, r *http.Request)) *httpx.Server {
	pipe.t.Helper()

	cfg.Service, cfg.Addr = name, ":0"

	srv, err := httpx.NewServer(cfg, pipe.log, httpx.WithTracerProvider(pipe.provider), httpx.WithPropagator(pipe.prop))
	require.NoError(pipe.t, err)

	srv.Mux().HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) { handle(srv, w, r) })

	return srv
}

// A request with the debug token is traced end to end at ratio "never": the
// edge samples it and marks the trace, the service behind follows the flag
// and logs the request from debug up. A request without the token leaves
// nothing, and a client cannot fake the mark.
func TestDebugTrace_TravelsFromTheEdgeToTheServiceBehindIt(t *testing.T) {
	t.Parallel()

	backend := newPipeline(t, Config{SamplerRatio: -1, TrustRemoteParent: true})
	behind := backend.service("backend", httpx.Config{}, func(srv *httpx.Server, w http.ResponseWriter, r *http.Request) {
		srv.Logger().DebugContext(r.Context(), "backend detail")
		w.WriteHeader(http.StatusNoContent)
	})

	edge := newPipeline(t, Config{SamplerRatio: -1, TrustRemoteParent: false})
	front := edge.service("edge", httpx.Config{DebugToken: secret.New(debugToken)}, func(srv *httpx.Server, w http.ResponseWriter, r *http.Request) {
		srv.Logger().DebugContext(r.Context(), "edge detail")

		call := httptest.NewRequest(http.MethodGet, "/work", http.NoBody)
		edge.prop.Inject(r.Context(), propagation.HeaderCarrier(call.Header))

		rec := httptest.NewRecorder()
		behind.Handler().ServeHTTP(rec, call)
		w.WriteHeader(rec.Code)
	})

	request := func(headers ...string) {
		req := httptest.NewRequest(http.MethodGet, "/work", http.NoBody)
		for i := 0; i+1 < len(headers); i += 2 {
			req.Header.Set(headers[i], headers[i+1])
		}

		rec := httptest.NewRecorder()
		front.Handler().ServeHTTP(rec, req)
		require.Equal(t, http.StatusNoContent, rec.Code)
	}

	// An ordinary request, and one from a client that knows the mark but
	// not the token.
	request()
	request("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"Tracestate", httpx.DebugTraceStateKey+"="+httpx.DebugTraceStateValue)

	assert.Empty(t, edge.spans())
	assert.Empty(t, backend.spans())
	assert.Nil(t, edge.lines.Find(t, map[string]any{"msg": "edge detail"}))
	assert.Nil(t, backend.lines.Find(t, map[string]any{"msg": "backend detail"}))

	request(httpx.DebugTokenHeader, debugToken)

	require.NoError(t, edge.provider.ForceFlush(context.Background()))
	require.NoError(t, backend.provider.ForceFlush(context.Background()))

	atEdge, atBackend := edge.exported.GetSpans(), backend.exported.GetSpans()
	require.Len(t, atEdge, 1)
	require.Len(t, atBackend, 1)

	for _, span := range []tracetest.SpanStub{atEdge[0], atBackend[0]} {
		assert.True(t, span.SpanContext.IsSampled())
		assert.True(t, httpx.IsDebugTrace(span.SpanContext), "the mark travels in the tracestate")
	}

	assert.Equal(t, atEdge[0].SpanContext.TraceID(), atBackend[0].SpanContext.TraceID(), "one trace")
	assert.Equal(t, atEdge[0].SpanContext.SpanID(), atBackend[0].Parent.SpanID())

	traceID := atEdge[0].SpanContext.TraceID().String()
	assert.NotNil(t, edge.lines.Find(t, map[string]any{"msg": "edge detail", "trace_id": traceID}))
	assert.NotNil(t, backend.lines.Find(t, map[string]any{"msg": "backend detail", "trace_id": traceID}),
		"the service behind logs the request from debug up, without a token of its own")
}

// A subject under investigation is known after its request's span started.
// With the buffer the trace of the request is exported all the same.
func TestDebugSubject_ExportsItsUnsampledTrace(t *testing.T) {
	t.Parallel()

	pipe := newPipeline(t, never())

	var srv *httpx.Server

	srv = pipe.service("svc", httpx.Config{AdminAddr: "127.0.0.1:0"}, func(_ *httpx.Server, w http.ResponseWriter, r *http.Request) {
		ctx := srv.DebugSubject(r.Context(), r.Header.Get("X-Tenant"))
		srv.Logger().DebugContext(ctx, "tenant detail")
		w.WriteHeader(http.StatusNoContent)
	})

	admin := httptest.NewRecorder()
	srv.Admin().ServeHTTP(admin, httptest.NewRequest(http.MethodPut, "/admin/debug-subjects/acme?ttl=1m", http.NoBody))
	require.Equal(t, http.StatusOK, admin.Code, admin.Body.String())

	for _, tenant := range []string{"other", "acme"} {
		req := httptest.NewRequest(http.MethodGet, "/work", http.NoBody)
		req.Header.Set("X-Tenant", tenant)
		srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}

	require.NoError(t, pipe.provider.ForceFlush(context.Background()))

	spans := pipe.exported.GetSpans()
	require.Len(t, spans, 1, "the request of the subject, and not the other one")
	assert.True(t, spans[0].SpanContext.IsSampled())
	assert.Contains(t, spans[0].Attributes, attribute.String(httpx.DebugSubjectAttribute, "acme"))
	assert.NotNil(t, pipe.lines.Find(t, map[string]any{
		"msg": "tenant detail", "trace_id": spans[0].SpanContext.TraceID().String(),
	}))
}
