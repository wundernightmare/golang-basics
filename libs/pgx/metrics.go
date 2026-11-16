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
//	db_pool_connections{state="idle|in_use|constructing"}  connections by state
//	db_pool_max_connections                                 configured ceiling
//	db_pool_acquires_waited_total                           acquires that had to wait for a free conn
//	db_pool_acquire_wait_seconds_total                      time spent acquiring connections
//	db_pool_acquires_canceled_total                         acquires cancelled by context while waiting
//	db_query_duration_seconds{operation}                    Exec / Query / QueryRow / SendBatch / CopyFrom latency
//	db_query_errors_total{operation}                        of those, the ones that failed
//
// The names are the workspace's metrics contract (docs/METRICS.md), shared
// with nodejs-basics. db_pool_connections{state="in_use"} at
// db_pool_max_connections is the "pool exhausted" signal;
// db_pool_acquires_waited_total climbing says requests feel it, and the wait
// counter turns it into seconds of wait per second. What the server itself
// sees (backends, locks, replication) is postgres-exporter's job, not ours.
//
// operation is the name the caller put on the context with [WithOperation]
// ("tasks.create"), or, when there is none, the statement's leading keyword
// in lower case ("select", "insert", …), "batch" for a SendBatch and "copy"
// for a CopyFrom — bounded either way, never the SQL text. The statements
// that delimit a transaction (begin, commit, rollback, savepoint, release)
// are always under their keyword: [DB.InTx] sends them with the caller's
// context, and under its name they would count as that operation's queries.
//
// A batch is one observation, from SendBatch to the Close of its results. A
// call through [DB.Conn] or [DB.InTx] that fails before it has a connection
// (the pool exhausted until the deadline, the pool closed) counts in
// db_query_errors_total under its operation — "begin" for an InTx — and has
// no observation in the histogram, so in such a moment the errors of an
// operation can outnumber its observations. Calls on the raw [DB.Pool] that
// fail this way are in neither metric.
//
// Two DBs in one process have the same metric names. To register both on one
// registry, wrap the registry for each of them — for every one, not only the
// second:
//
//	prometheus.WrapRegistererWith(prometheus.Labels{"pool": "replica"}, reg).MustRegister(replica.Collectors()...)
func (db *DB) Collectors() []prometheus.Collector {
	stat := db.pool.Stat
	gauge := func(name, help string, f func() float64, labels prometheus.Labels) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, f)
	}
	counter := func(name, help string, f func() float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, f)
	}

	const conns, connsHelp = "db_pool_connections", "Connections in the pool by state."

	return []prometheus.Collector{
		gauge(conns, connsHelp, func() float64 { return float64(stat().IdleConns()) }, prometheus.Labels{"state": "idle"}),
		gauge(conns, connsHelp, func() float64 { return float64(stat().AcquiredConns()) }, prometheus.Labels{"state": "in_use"}),
		gauge(conns, connsHelp, func() float64 { return float64(stat().ConstructingConns()) }, prometheus.Labels{"state": "constructing"}),
		gauge("db_pool_max_connections", "Configured maximum pool size.", func() float64 { return float64(stat().MaxConns()) }, nil),
		counter("db_pool_acquires_waited_total", "Connection acquires that waited because the pool had no idle connection.",
			func() float64 { return float64(stat().EmptyAcquireCount()) }),
		counter("db_pool_acquire_wait_seconds_total", "Cumulative time spent acquiring connections.",
			func() float64 { return stat().AcquireDuration().Seconds() }),
		counter("db_pool_acquires_canceled_total", "Connection acquires cancelled by their context while waiting.",
			func() float64 { return float64(stat().CanceledAcquireCount()) }),
		db.metrics.duration,
		db.metrics.errors,
	}
}

type operationKey struct{}

// WithOperation names the queries, batches and copies run with ctx for the
// query metrics (db_query_duration_seconds{operation=name}). Use a constant
// per call site ("tasks.list"), never anything derived from input: it is a
// label.
func WithOperation(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, operationKey{}, name)
}

// queryMetrics is the pgx tracer ([pgx.QueryTracer], [pgx.BatchTracer],
// [pgx.CopyFromTracer]) that times every Exec / Query / QueryRow / SendBatch /
// CopyFrom. It runs beside otelpgx through a multitracer.
type queryMetrics struct {
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
}

func newQueryMetrics() *queryMetrics {
	return &queryMetrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "PostgreSQL query latency in seconds by operation (WithOperation name, else the statement keyword).",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"operation"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "db_query_errors_total",
			Help: "PostgreSQL queries that failed, by operation (a cancelled context and a call that got no connection count).",
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
	return started(ctx, queryOperation(ctx, data.SQL))
}

// TraceQueryEnd implements [pgx.QueryTracer].
func (m *queryMetrics) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	m.end(ctx, data.Err)
}

// TraceBatchStart implements [pgx.BatchTracer].
func (m *queryMetrics) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return started(ctx, operation(ctx, "batch"))
}

// TraceBatchQuery implements [pgx.BatchTracer]. The batch is measured as a
// whole; the error of a statement in it arrives again at TraceBatchEnd.
func (m *queryMetrics) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

// TraceBatchEnd implements [pgx.BatchTracer].
func (m *queryMetrics) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	m.end(ctx, data.Err)
}

// TraceCopyFromStart implements [pgx.CopyFromTracer].
func (m *queryMetrics) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return started(ctx, operation(ctx, "copy"))
}

// TraceCopyFromEnd implements [pgx.CopyFromTracer].
func (m *queryMetrics) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	m.end(ctx, data.Err)
}

// queryOperation is the operation label of one statement.
func queryOperation(ctx context.Context, sql string) string {
	keyword := statementKeyword(sql)
	if transactionControl[keyword] {
		return keyword
	}

	return operation(ctx, keyword)
}

// operation is the caller's [WithOperation] name, or else fallback.
func operation(ctx context.Context, fallback string) string {
	if op, _ := ctx.Value(operationKey{}).(string); op != "" {
		return op
	}

	return fallback
}

func started(ctx context.Context, op string) context.Context {
	if try, _ := ctx.Value(attemptKey{}).(*attempt); try != nil {
		try.traced = true
	}

	return context.WithValue(ctx, queryStartKey{}, queryStart{op: op, start: time.Now()})
}

func (m *queryMetrics) end(ctx context.Context, err error) {
	qs, ok := ctx.Value(queryStartKey{}).(queryStart)
	if !ok {
		return
	}

	m.duration.WithLabelValues(qs.op).Observe(time.Since(qs.start).Seconds())
	// "no rows" is an answer, not a failure (it surfaces at Scan, but guard anyway).
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		m.errors.WithLabelValues(qs.op).Inc()
	}
}

// attempt is how a call made through [DB.Conn] or [DB.InTx] learns whether it
// got as far as a connection. pgx calls its tracer only there, so a call that
// failed earlier — no free connection before the deadline, a closed pool, a
// transaction already over — would be in no query metric. The tracer marks
// the attempt it finds on the context of the call, on the goroutine of the
// call and before the call returns; an attempt left unmarked is counted as a
// failed query by [queryMetrics.missed].
type attempt struct{ traced bool }

type attemptKey struct{}

func watch(ctx context.Context) (context.Context, *attempt) {
	try := &attempt{}

	return context.WithValue(ctx, attemptKey{}, try), try
}

// missed counts try as a failed query of op unless the tracer saw it. Such a
// call has no observation in the duration histogram: it ran no statement.
func (m *queryMetrics) missed(try *attempt, op string) {
	if !try.traced {
		m.errors.WithLabelValues(op).Inc()
	}
}

// transactionControl is the statements that delimit a transaction and belong
// to no operation.
var transactionControl = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table (Go has no const maps)
	"begin": true, "commit": true, "rollback": true, "savepoint": true, "release": true,
}

// knownKeywords bounds the fallback label: anything else is "other".
var knownKeywords = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table (Go has no const maps)
	"select": true, "insert": true, "update": true, "delete": true, "with": true,
	"begin": true, "commit": true, "rollback": true, "savepoint": true, "release": true,
	"create": true, "alter": true, "drop": true, "truncate": true, "copy": true,
	"set": true, "show": true, "lock": true, "listen": true, "notify": true, "call": true,
}

// statementKeyword returns the lower-cased leading keyword of sql, or "other".
// Leading whitespace, "--" comments and opening parentheses are skipped, so
// "(SELECT …) UNION …" is a "select"; a leading "/* … */" comment is not
// parsed and makes the statement "other".
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
