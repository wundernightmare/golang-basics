package otelx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tracehubmmp/golang-basics/libs/otelx"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := otelx.LoadConfig("TASKS_")
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
	require.Equal(t, "localhost:4317", cfg.Endpoint)
	require.InEpsilon(t, 1.0, cfg.SamplerRatio, 1e-9)

	t.Setenv("TASKS_OTEL_ENABLED", "true")
	t.Setenv("TASKS_OTEL_SERVICE_NAME", "tasks")
	cfg, err = otelx.LoadConfig("TASKS_")
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, "tasks", cfg.ServiceName)
}

func TestInitDisabledInstallsPropagatorAndNoopShutdown(t *testing.T) {
	shutdown, err := otelx.Init(context.Background(), otelx.Config{Enabled: false, ServiceName: "x"}, nil)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Propagators are installed even when export is off, so cross-hop context
	// still flows: the W3C traceparent field must be advertised.
	require.Contains(t, otel.GetTextMapPropagator().Fields(), "traceparent")

	require.NoError(t, shutdown(context.Background()))
}

func TestGinMiddlewareRecordsSpan(t *testing.T) {
	// Install an in-memory tracer provider so the middleware's spans land
	// somewhere we can assert on.
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.Use(otelx.GinMiddleware("tasks"))
	e.GET("/tasks/:id", func(c *gin.Context) { c.Status(http.StatusOK) })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks/42", nil)
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	// The span is named by method + matched route template, not the raw path,
	// so /tasks/42 and /tasks/99 share one span name (low cardinality).
	require.Equal(t, "GET /tasks/:id", spans[0].Name())
}
