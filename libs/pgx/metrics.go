package pgx

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// Collectors returns the Prometheus collectors for the service to register on
// its metrics registry: the pool's live statistics ([pgxpool.Pool.Stat]) and
// the per-operation query metrics recorded by the pool's query tracer.
//
//	pgxpool_conns{state="idle|acquired|constructing"}   connections by state
//	pgxpool_max_conns                                    configured ceiling
//	pgxpool_acquire_total                                successful acquires
//	pgxpool_acquire_duration_seconds_total               time spent waiting to acquire
//	pgxpool_empty_acquire_total                          acquires that had to wait for a free conn
//	pgxpool_canceled_acquire_total                       acquires cancelled by context
//	pgx_query_duration_seconds{operation}                Exec / Query / QueryRow latency
//	pgx_query_errors_total{operation}                    of those, the ones that failed
//
// pgxpool_empty_acquire_total climbing while pgxpool_conns{state="acquired"}
// sits at pgxpool_max_conns is the "pool exhausted" signal; the acquire
// duration counter turns it into seconds of wait per second.
//
// operation is the name the caller put on the context with [WithOperation]
// ("tasks.create"), or the statement's leading keyword in lower case
// ("select", "insert", "begin", …) when there is none — bounded either way,
// never the SQL text.
func (db *DB) Collectors() []prometheus.Collector {
	stat := db.pool.Stat
	gauge := func(name, help string, f func() float64, labels prometheus.Labels) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, f)
	}
	counter := func(name, help string, f func() float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, f)
	}
	const conns, connsHelp = "pgxpool_conns", "Connections in the pool by state."
	return []prometheus.Collector{
		gauge(conns, connsHelp, func() float64 { return float64(stat().IdleConns()) }, prometheus.Labels{"state": "idle"}),
		gauge(conns, connsHelp, func() float64 { return float64(stat().AcquiredConns()) }, prometheus.Labels{"state": "acquired"}),
		gauge(conns, connsHelp, func() float64 { return float64(stat().ConstructingConns()) }, prometheus.Labels{"state": "constructing"}),
		gauge("pgxpool_max_conns", "Configured maximum pool size.", func() float64 { return float64(stat().MaxConns()) }, nil),
		counter("pgxpool_acquire_total", "Successful connection acquires.", func() float64 { return float64(stat().AcquireCount()) }),
		counter("pgxpool_acquire_duration_seconds_total", "Cumulative time spent acquiring connections.",
			func() float64 { return stat().AcquireDuration().Seconds() }),
		counter("pgxpool_empty_acquire_total", "Acquires that waited because the pool was empty.",
			func() float64 { return float64(stat().EmptyAcquireCount()) }),
		counter("pgxpool_canceled_acquire_total", "Acquires cancelled by their context while waiting.",
			func() float64 { return float64(stat().CanceledAcquireCount()) }),
		db.metrics.duration,
		db.metrics.errors,
	}
}

type operationKey struct{}

// WithOperation names the queries run with ctx for the query metrics
// (pgx_query_duration_seconds{operation=name}). Use a constant per call site
// ("tasks.list"), never anything derived from input: it is a label.
func WithOperation(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, operationKey{}, name)
}

// queryMetrics is a [pgx.QueryTracer] that times every Exec / Query /
// QueryRow. It runs beside otelpgx through a multitracer.
type queryMetrics struct {
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
}

func newQueryMetrics() *queryMetrics {
	return &queryMetrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pgx_query_duration_seconds",
			Help:    "PostgreSQL query latency in seconds by operation (WithOperation name, else the statement keyword).",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"operation"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgx_query_errors_total",
			Help: "PostgreSQL queries that failed, by operation (a cancelled context counts).",
		}, []string{"operation"}),
	}
}

type queryStart struct {
	op    string
	start time.Time
}

type queryStartKey struct{}

// TraceQueryStart implements [pgx.QueryTracer].
func (m *queryMetrics) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	op, _ := ctx.Value(operationKey{}).(string)
	if op == "" {
		op = statementKeyword(data.SQL)
	}
	return context.WithValue(ctx, queryStartKey{}, queryStart{op: op, start: time.Now()})
}

// TraceQueryEnd implements [pgx.QueryTracer].
func (m *queryMetrics) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	qs, ok := ctx.Value(queryStartKey{}).(queryStart)
	if !ok {
		return
	}
	m.duration.WithLabelValues(qs.op).Observe(time.Since(qs.start).Seconds())
	// "no rows" is an answer, not a failure (it surfaces at Scan, but guard anyway).
	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
		m.errors.WithLabelValues(qs.op).Inc()
	}
}

// knownKeywords bounds the fallback label: anything else is "other".
var knownKeywords = map[string]bool{
	"select": true, "insert": true, "update": true, "delete": true, "with": true,
	"begin": true, "commit": true, "rollback": true, "savepoint": true, "release": true,
	"create": true, "alter": true, "drop": true, "truncate": true, "copy": true,
	"set": true, "show": true, "lock": true, "listen": true, "notify": true, "call": true,
}

// statementKeyword returns the lower-cased leading keyword of sql, skipping
// whitespace and "--" comments, or "other".
func statementKeyword(sql string) string {
	s := sql
	for {
		s = strings.TrimLeft(s, " \t\r\n(")
		if !strings.HasPrefix(s, "--") {
			break
		}
		nl := strings.IndexByte(s, '\n')
		if nl < 0 {
			return "other"
		}
		s = s[nl+1:]
	}
	end := strings.IndexFunc(s, func(r rune) bool { return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') })
	if end < 0 {
		end = len(s)
	}
	kw := strings.ToLower(s[:end])
	if knownKeywords[kw] {
		return kw
	}
	return "other"
}
