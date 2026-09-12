package otelx_test

// Telemetry contract: the three signals a request produces must agree with
// each other and with what actually happened. A real httpx server (real gin
// engine, real slog handler chain, real Prometheus registry) with the real
// OpenTelemetry SDK behind an in-memory exporter — nothing is faked, only the
// destinations are in-process — and the access-log line, the recorded span
// and the metrics must all describe the same request.

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func newTracedServer(t testing.TB, buf *testx.LogBuffer) *httpx.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	log := httpx.NewLogger(httpx.LogConfig{Service: "contract", Level: "info", Format: "json", Writer: buf})
	srv := httpx.NewServer(httpx.Config{Service: "contract", Addr: ":0", SlowRequest: time.Second}, log,
		httpx.WithMiddleware(otelx.GinMiddleware("contract")))
	srv.Engine().GET("/items/:id", func(c *gin.Context) {
		// A handler log line, with the request context: must carry the trace.
		srv.Logger().InfoContext(c.Request.Context(), "loading item", "id", c.Param("id"))
		c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
	})
	srv.Engine().GET("/boom", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })
	return srv
}

func TestLogsSpansAndMetricsDescribeTheSameRequest(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		sr := testx.Recorder(t)
		buf := &testx.LogBuffer{}
		srv := newTracedServer(t, buf)

		// An upstream caller with its own trace: the server span must continue it.
		const upstreamTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
		req := httptest.NewRequest(http.MethodGet, "/items/42", nil)
		req.Header.Set("traceparent", "00-"+upstreamTrace+"-00f067aa0ba902b7-01")
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// --- span --------------------------------------------------------------
		spans := sr.Ended()
		require.Len(t, spans, 1, "exactly one server span per request")
		span := spans[0]
		assert.Equal(t, "GET /items/:id", span.Name(), "named by route template, not raw path")
		assert.Equal(t, trace.SpanKindServer, span.SpanKind())
		assert.Equal(t, upstreamTrace, span.SpanContext().TraceID().String(), "continues the caller's trace")

		// --- logs --------------------------------------------------------------
		lines := buf.Lines(t)
		require.Len(t, lines, 2, "one handler line + one access-log line")
		handlerLine, accessLine := lines[0], lines[1]
		assert.Equal(t, "loading item", handlerLine["msg"])
		assert.Equal(t, "request", accessLine["msg"])
		for _, l := range lines {
			assert.Equal(t, upstreamTrace, l["trace_id"], "every log line in the request carries the trace")
			assert.Equal(t, span.SpanContext().SpanID().String(), l["span_id"], "…and the server span")
			assert.Equal(t, "contract", l["service"])
		}
		assert.Equal(t, "/items/:id", accessLine["route"])
		assert.Equal(t, "/items/42", accessLine["path"])
		assert.Equal(t, float64(200), accessLine["status"])
		assert.Equal(t, "INFO", accessLine["level"])
		assert.Greater(t, accessLine["latency_ms"], 0.0)

		// --- metrics -----------------------------------------------------------
		reg := srv.Metrics.Registry
		assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "GET", "path": "/items/:id", "status": "200"}))
		assert.Equal(t, 1.0, testx.Metric(t, reg, "http_request_duration_seconds", map[string]string{"method": "GET", "path": "/items/:id"}))
		assert.Equal(t, 0.0, testx.Metric(t, reg, "http_requests_in_flight", nil), "back to zero once the request finished")
		testx.LintMetrics(t, reg)
	}, "otelx", "contract")
}

func TestErrorRequestIsErrorEverywhere(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		sr := testx.Recorder(t)
		buf := &testx.LogBuffer{}
		srv := newTracedServer(t, buf)

		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))
		require.Equal(t, http.StatusInternalServerError, rec.Code)

		spans := sr.Ended()
		require.Len(t, spans, 1)
		assert.Equal(t, "GET /boom", spans[0].Name())

		lines := buf.Lines(t)
		require.Len(t, lines, 1)
		assert.Equal(t, "ERROR", lines[0]["level"], "5xx is an error-level access-log line")
		assert.Equal(t, spans[0].SpanContext().TraceID().String(), lines[0]["trace_id"])

		assert.Equal(t, 1.0, testx.Metric(t, srv.Metrics.Registry, "http_requests_total", map[string]string{"method": "GET", "path": "/boom", "status": "500"}))
	}, "otelx", "contract")
}

// Sampling must be exact and never lose warnings, also under concurrency:
// counts are atomic per message, so the pass/drop decision for the n-th
// record is a pure function of n. That makes this deterministic, not flaky.
func TestLogSamplingIsExactUnderConcurrency(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		buf := &testx.LogBuffer{}
		log := httpx.NewLogger(httpx.LogConfig{
			Level: "info", Format: "json", Writer: buf,
			SampleInitial: 10, SampleThereafter: 25, SampleTick: time.Hour,
		})
		const goroutines, per = 8, 250 // 2000 info records of one message
		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range per {
					log.Info("hot path", "k", 1)
					log.Warn("never dropped")
				}
			}()
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

		srv := httpx.NewServer(httpx.Config{Service: "s", Addr: ":0"}, log)
		assert.Equal(t, float64(total-info), testx.Metric(t, srv.Metrics.Registry, "log_dropped_total", map[string]string{"level": "info"}))
	}, "otelx", "contract")
}
