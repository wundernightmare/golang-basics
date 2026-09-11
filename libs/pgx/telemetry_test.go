package pgx_test

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
)

// Every query is a client span inside the caller's trace, and the pool
// statistics on /metrics move with real usage. Real Postgres (testcontainers).
func TestQueriesAreTracedAndPoolStatsAreExported(t *testing.T) {
	cfg := startPostgres(t)
	sr := installRecorder(t)
	ctx := context.Background()

	db, err := pgx.New(ctx, cfg, nil)
	require.NoError(t, err)
	defer db.Close()
	reg := prometheus.NewRegistry()
	reg.MustRegister(db.Collectors()...)

	before := metricValue(t, reg, "pgxpool_acquire_total", nil)

	qctx, parent := otel.Tracer("test").Start(ctx, "handler")
	var one int
	require.NoError(t, db.Pool().QueryRow(qctx, "SELECT 1").Scan(&one))
	parent.End()

	var query sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() == parent.SpanContext().TraceID() && s.SpanKind() == trace.SpanKindClient &&
			strings.HasPrefix(strings.ToUpper(s.Name()), "SELECT") {
			query = s
		}
	}
	require.NotNil(t, query, "a client span named by the statement, in the caller's trace")
	require.Equal(t, parent.SpanContext().SpanID(), query.Parent().SpanID(), "child of the handler span")

	require.Greater(t, metricValue(t, reg, "pgxpool_acquire_total", nil), before, "the query acquired a connection")
	require.Equal(t, float64(cfg.MaxConns), metricValue(t, reg, "pgxpool_max_conns", nil))
	require.Equal(t, 0.0, metricValue(t, reg, "pgxpool_conns", map[string]string{"state": "acquired"}), "released after the query")
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)
}
