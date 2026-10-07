package otelx_test

// Telemetry contract: the three signals a request produces must agree with
// each other and with what actually happened. A real httpx server (real
// net/http chain, real slog handler chain, real Prometheus registry) with the
// real OpenTelemetry SDK behind an in-memory recorder — nothing is faked,
// only the destinations are in-process — and the access-log line, the
// recorded span and the metrics must all describe the same request.
//
// testx.Recorder swaps the global tracer provider, so none of these tests
// runs in parallel.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

const (
	upstreamTrace  = "4bf92f3577b34da6a3ce929d0e0e4736"
	upstreamSpan   = "00f067aa0ba902b7"
	upstreamHeader = "00-" + upstreamTrace + "-" + upstreamSpan + "-01"
)

func newTracedServer(t testing.TB, buf *testx.LogBuffer) *httpx.Server {
	t.Helper()
	log := httpx.NewLogger(httpx.LogConfig{Service: "contract", Level: "info", Format: "json", Writer: buf})
	srv, err := httpx.NewServer(httpx.Config{Service: "contract", Addr: ":0", SlowRequest: time.Minute}, log)
	require.NoError(t, err)
	srv.Mux().HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		// A handler log line, with the request context: must carry the trace.
		srv.Logger().InfoContext(r.Context(), "loading item", "id", r.PathValue("id"))
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id")})
	})
	srv.Mux().HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.Internal("could not load", errors.New("db down")))
	})
	srv.Mux().HandleFunc("GET /bad-gateway", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	srv.Mux().HandleFunc("/any", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return srv
}

func serve(srv *httpx.Server, method, target, traceparent string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	if traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func attrs(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestLogsSpansAndMetricsDescribeTheSameRequest(t *testing.T) {
	sr := testx.Recorder(t)
	buf := &testx.LogBuffer{}
	srv := newTracedServer(t, buf)

	// An upstream caller with its own trace: the server span must continue it.
	rec := serve(srv, http.MethodGet, "/items/42?full=1", upstreamHeader)
	require.Equal(t, http.StatusOK, rec.Code)
	requestID := rec.Header().Get(httpx.RequestIDHeader)
	require.NotEmpty(t, requestID)

	// --- span --------------------------------------------------------------
	spans := sr.Ended()
	require.Len(t, spans, 1, "exactly one server span per request")
	span := spans[0]
	assert.Equal(t, "GET /items/{id}", span.Name(), "named by route template, not raw path")
	assert.Equal(t, trace.SpanKindServer, span.SpanKind())
	assert.Equal(t, upstreamTrace, span.SpanContext().TraceID().String(), "continues the caller's trace")
	assert.Equal(t, upstreamSpan, span.Parent().SpanID().String(), "as a child of the caller's span")
	assert.True(t, span.Parent().IsRemote())
	a := attrs(span)
	assert.Equal(t, "/items/{id}", a["http.route"].AsString())
	assert.Equal(t, int64(200), a["http.response.status_code"].AsInt64())
	assert.Equal(t, "GET", a["http.request.method"].AsString())
	assert.Equal(t, "/items/42", a["url.path"].AsString())
	assert.Equal(t, "full=1", a["url.query"].AsString())
	assert.Equal(t, "http", a["url.scheme"].AsString())
	assert.Equal(t, "192.0.2.1", a["client.address"].AsString())
	assert.Equal(t, codes.Unset, span.Status().Code)
	assert.Equal(t, "github.com/tracehubmmp/golang-basics/libs/httpx", span.InstrumentationScope().Name)

	// --- logs --------------------------------------------------------------
	lines := buf.Lines(t)
	require.Len(t, lines, 2, "one handler line + one access-log line")
	handlerLine, accessLine := lines[0], lines[1]
	assert.Equal(t, "loading item", handlerLine["msg"])
	assert.Equal(t, "request", accessLine["msg"])
	for _, l := range lines {
		assert.Equal(t, upstreamTrace, l["trace_id"], "every log line in the request carries the trace")
		assert.Equal(t, span.SpanContext().SpanID().String(), l["span_id"], "…and the server span")
		assert.Equal(t, requestID, l["request_id"], "…and the request id the client got back")
		assert.Equal(t, "contract", l["service"])
	}
	assert.Equal(t, "GET", accessLine["method"])
	assert.Equal(t, "/items/{id}", accessLine["route"])
	assert.Equal(t, "/items/42", accessLine["path"])
	assert.Equal(t, float64(200), accessLine["status"])
	assert.Equal(t, "INFO", accessLine["level"])
	assert.Equal(t, "192.0.2.1", accessLine["client_ip"])
	assert.Equal(t, float64(rec.Body.Len()), accessLine["bytes"])
	assert.Contains(t, accessLine, "latency_ms")

	// --- metrics -----------------------------------------------------------
	reg := srv.Metrics.Registry
	labels := map[string]string{"method": "GET", "route": "/items/{id}", "status": "200"}
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", labels))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_request_duration_seconds", map[string]string{"method": "GET", "route": "/items/{id}"}))
	assert.Equal(t, 0.0, testx.Metric(t, reg, "http_requests_in_flight", nil), "back to zero once the request finished")

	// The sampled span's trace id is the exemplar: a dashboard can jump from
	// the metric to this trace.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var counterEx, histEx *dto.Exemplar
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			switch mf.GetName() {
			case "http_requests_total":
				counterEx = m.GetCounter().GetExemplar()
			case "http_request_duration_seconds":
				for _, b := range m.GetHistogram().GetBucket() {
					if b.GetExemplar() != nil {
						histEx = b.GetExemplar()
					}
				}
				for _, e := range m.GetHistogram().GetExemplars() { // native histogram exemplars
					histEx = e
				}
			}
		}
	}
	for name, ex := range map[string]*dto.Exemplar{"http_requests_total": counterEx, "http_request_duration_seconds": histEx} {
		require.NotNil(t, ex, "%s carries an exemplar", name)
		require.Len(t, ex.GetLabel(), 1, name)
		assert.Equal(t, "trace_id", ex.GetLabel()[0].GetName(), name)
		assert.Equal(t, upstreamTrace, ex.GetLabel()[0].GetValue(), name)
	}
	testx.LintMetrics(t, reg)
}

func TestErrorRequestIsErrorEverywhere(t *testing.T) {
	sr := testx.Recorder(t)
	buf := &testx.LogBuffer{}
	srv := newTracedServer(t, buf)

	rec := serve(srv, http.MethodGet, "/boom", "")
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.NotContains(t, rec.Body.String(), "db down", "the cause never reaches the client")

	spans := sr.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "GET /boom", span.Name())
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, int64(500), attrs(span)["http.response.status_code"].AsInt64())
	var recorded bool
	for _, ev := range span.Events() {
		if ev.Name == "exception" {
			for _, kv := range ev.Attributes {
				if kv.Key == "exception.message" && kv.Value.AsString() == "db down" {
					recorded = true
				}
			}
		}
	}
	assert.True(t, recorded, "the cause is recorded on the span: %v", span.Events())

	traceID := span.SpanContext().TraceID().String()
	failed := buf.Find(t, map[string]any{"msg": "request failed"})
	require.NotNil(t, failed, "the cause is logged")
	assert.Equal(t, "db down", failed["err"])
	assert.Equal(t, traceID, failed["trace_id"])
	access := buf.Find(t, map[string]any{"msg": "request"})
	require.NotNil(t, access)
	assert.Equal(t, "ERROR", access["level"], "5xx is an error-level access-log line")
	assert.Equal(t, traceID, access["trace_id"])

	assert.Equal(t, 1.0, testx.Metric(t, srv.Metrics.Registry, "http_requests_total",
		map[string]string{"method": "GET", "route": "/boom", "status": "500"}))
}

func TestA5xxWrittenByHandIsAnErrorSpan(t *testing.T) {
	sr := testx.Recorder(t)
	srv := newTracedServer(t, &testx.LogBuffer{})
	serve(srv, http.MethodGet, "/bad-gateway", "")
	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code, "any 5xx marks the span, not only WriteError")
	assert.Equal(t, int64(502), attrs(spans[0])["http.response.status_code"].AsInt64())

	serve(srv, http.MethodGet, "/nope", "")
	spans = sr.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, codes.Unset, spans[1].Status().Code, "a 4xx is the client's error, not the server's")
	assert.Equal(t, "GET", spans[1].Name(), "an unmatched request has no route to name it by")
	assert.NotContains(t, attrs(spans[1]), attribute.Key("http.route"))
}

func TestOddMethodsDoNotGrowSpanNamesOrSeries(t *testing.T) {
	sr := testx.Recorder(t)
	srv := newTracedServer(t, &testx.LogBuffer{})
	serve(srv, "FOO", "/any", "")

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "_OTHER /any", spans[0].Name())
	a := attrs(spans[0])
	assert.Equal(t, "_OTHER", a["http.request.method"].AsString())
	assert.Equal(t, "FOO", a["http.request.method_original"].AsString(), "the original is kept as an attribute")
	assert.Equal(t, 1.0, testx.Metric(t, srv.Metrics.Registry, "http_requests_total", map[string]string{"method": "_OTHER", "route": "/any"}))
}

// With tracing disabled (Init installs only the propagators), a request that
// arrives with a trace context still logs under the caller's trace id: logs
// correlate across services even where nothing is exported.
func TestDisabledTracingStillCorrelatesLogsWithTheCaller(t *testing.T) {
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	_, err := otelx.Init(context.Background(), otelx.Config{Enabled: false}, nil)
	require.NoError(t, err)

	buf := &testx.LogBuffer{}
	srv := newTracedServer(t, buf)
	serve(srv, http.MethodGet, "/items/1", upstreamHeader)
	line := buf.Find(t, map[string]any{"msg": "request"})
	require.NotNil(t, line)
	assert.Equal(t, upstreamTrace, line["trace_id"])

	// The caller's sampled flag rides on the non-recording span, so the
	// exemplar points at the caller's trace — the one that was exported.
	var ex *dto.Exemplar
	for _, m := range seriesOf(t, srv, "http_requests_total") {
		ex = m.GetCounter().GetExemplar()
	}
	require.NotNil(t, ex)
	assert.Equal(t, upstreamTrace, ex.GetLabel()[0].GetValue())

	// Without an inbound context there is nothing to correlate.
	serve(srv, http.MethodGet, "/items/2", "")
	lines := buf.Lines(t)
	assert.NotContains(t, lines[len(lines)-1], "trace_id")
}

func seriesOf(t testing.TB, srv *httpx.Server, name string) []*dto.Metric {
	t.Helper()
	mfs, err := srv.Metrics.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()
		}
	}
	return nil
}

// Sampling must be exact and never lose warnings, also under concurrency:
// counts are atomic per message, so the pass/drop decision for the n-th
// record is a pure function of n. That makes this deterministic, not flaky.
func TestLogSamplingIsExactUnderConcurrency(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{
		Level: "info", Format: "json", Writer: buf,
		SampleInitial: 10, SampleThereafter: 25, SampleTick: time.Hour,
	})
	const goroutines, per = 8, 250 // 2000 info records of one message
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range per {
				log.Info("hot path", "k", 1)
				log.Warn("never dropped")
			}
		})
	}
	wg.Wait()

	info, warn := 0, 0
	for _, l := range buf.Lines(t) {
		switch l["msg"] {
		case "hot path":
			info++
		case "never dropped":
			warn++
		}
	}
	total := goroutines * per
	assert.Equal(t, 10+(total-10)/25, info, "first 10, then every 25th")
	assert.Equal(t, total, warn)

	srv, err := httpx.NewServer(httpx.Config{Service: "s", Addr: ":0"}, log)
	require.NoError(t, err)
	assert.Equal(t, float64(total-info), testx.Metric(t, srv.Metrics.Registry, "log_dropped_total", map[string]string{"level": "info"}))
}
