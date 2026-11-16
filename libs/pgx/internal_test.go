package pgx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/secret"
)

func TestStatementKeyword(t *testing.T) {
	t.Parallel()

	for sql, want := range map[string]string{
		"SELECT 1":                       "select",
		"  \n\tinsert into t values (1)": "insert",
		"-- why\n-- and how\nUPDATE t":   "update",
		"(SELECT 1) UNION (SELECT 2)":    "select",
		"release savepoint sp_1":         "release",
		"/* hint */ SELECT 1":            "other",
		"EXPLAIN SELECT 1":               "other",
		"-- a comment and nothing else":  "other",
		"":                               "other",
		"vacuum full":                    "other",
	} {
		assert.Equal(t, want, statementKeyword(sql), "%q", sql)
	}
}

func TestRuntimeParams(t *testing.T) {
	t.Parallel()

	got := Config{
		StatementTimeout:                1500 * time.Millisecond,
		LockTimeout:                     500 * time.Microsecond,
		IdleInTransactionSessionTimeout: 0,
	}.runtimeParams()

	assert.Equal(t, map[string]string{
		"statement_timeout": "1500",
		"lock_timeout":      "1", // not "0", which would switch it off
	}, got, "milliseconds; a zero timeout is not sent")
}

func TestIsolationRank(t *testing.T) {
	t.Parallel()

	ascending := []pgx.TxIsoLevel{pgx.ReadUncommitted, pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable}
	for i := 1; i < len(ascending); i++ {
		assert.Greater(t, isolationRank(ascending[i]), isolationRank(ascending[i-1]), "%s over %s", ascending[i], ascending[i-1])
	}

	assert.Equal(t, isolationRank(pgx.ReadCommitted), isolationRank(""), "no level is the server default")
}

func TestRetryable(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"serialization failure": {&pgconn.PgError{Code: "40001"}, true},
		"deadlock, wrapped":     {fmt.Errorf("insert: %w", &pgconn.PgError{Code: "40P01"}), true},
		"unique violation":      {&pgconn.PgError{Code: "23505"}, false},
		"not from the server":   {context.DeadlineExceeded, false},
	} {
		assert.Equal(t, tc.want, retryable(tc.err), name)
	}
}

// What the config says about the pool reaches pgxpool; a zero leaves
// pgxpool's own value.
func TestPoolConfig(t *testing.T) {
	t.Parallel()

	const dsn = "postgres://app:app@db:5432/app"

	set, err := Config{
		URL: secret.New(dsn), MaxConns: 7, MinConns: 7,
		MaxConnLifetime: 11 * time.Minute, MaxConnIdleTime: 5 * time.Minute, DialTimeout: 3 * time.Second,
		StatementTimeout: time.Second,
	}.poolConfig()
	require.NoError(t, err)
	assert.Equal(t, int32(7), set.MaxConns)
	assert.Equal(t, int32(7), set.MinConns, "as many warm connections as the pool holds")
	assert.Equal(t, 11*time.Minute, set.MaxConnLifetime)
	assert.Equal(t, 66*time.Second, set.MaxConnLifetimeJitter, "a tenth of the lifetime")
	assert.Equal(t, 5*time.Minute, set.MaxConnIdleTime)
	assert.Equal(t, 3*time.Second, set.ConnConfig.ConnectTimeout)
	assert.Equal(t, "1000", set.ConnConfig.RuntimeParams["statement_timeout"])

	zero, err := Config{URL: secret.New(dsn + "?pool_max_conns=4&pool_max_conn_lifetime=2h&pool_max_conn_lifetime_jitter=1m")}.poolConfig()
	require.NoError(t, err)
	assert.Equal(t, int32(4), zero.MaxConns)
	assert.Equal(t, 2*time.Hour, zero.MaxConnLifetime)
	assert.Equal(t, time.Minute, zero.MaxConnLifetimeJitter, "the DSN's own jitter stands")
	assert.Equal(t, 30*time.Minute, zero.MaxConnIdleTime, "pgxpool's own")
	assert.Equal(t, defaultDialTimeout, zero.ConnConfig.ConnectTimeout)
}

// The operation label of the query metrics, for each kind of call.
func TestQueryMetrics_Operation(t *testing.T) {
	t.Parallel()

	named := WithOperation(context.Background(), "tasks.create")
	failed := errors.New("server said no")

	query := func(sql string) func(*queryMetrics, context.Context, error) {
		return func(m *queryMetrics, ctx context.Context, err error) {
			m.TraceQueryEnd(m.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql}), nil, pgx.TraceQueryEndData{Err: err})
		}
	}
	batch := func(m *queryMetrics, ctx context.Context, err error) {
		ctx = m.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
		m.TraceBatchQuery(ctx, nil, pgx.TraceBatchQueryData{Err: err})
		m.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{Err: err})
	}
	copyFrom := func(m *queryMetrics, ctx context.Context, err error) {
		m.TraceCopyFromEnd(m.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{}), nil, pgx.TraceCopyFromEndData{Err: err})
	}

	for name, tc := range map[string]struct {
		call       func(*queryMetrics, context.Context, error)
		ctx        context.Context //nolint:containedctx // a table of contexts is the test
		err        error
		operation  string
		wantErrors float64
	}{
		"query, unnamed":           {query("SELECT 1"), context.Background(), nil, "select", 0},
		"query, named":             {query("SELECT 1"), named, failed, "tasks.create", 1},
		"no rows is an answer":     {query("SELECT 1"), named, pgx.ErrNoRows, "tasks.create", 0},
		"begin of a named unit":    {query("begin isolation level serializable"), named, nil, "begin", 0},
		"commit of a named unit":   {query("commit"), named, failed, "commit", 1},
		"rollback of a named unit": {query("rollback"), named, nil, "rollback", 0},
		"savepoint in a named one": {query("savepoint sp_1"), named, nil, "savepoint", 0},
		"batch, unnamed":           {batch, context.Background(), failed, "batch", 1},
		"batch, named":             {batch, named, nil, "tasks.create", 0},
		"copy, unnamed":            {copyFrom, context.Background(), failed, "copy", 1},
		"copy, named":              {copyFrom, named, nil, "tasks.create", 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := newQueryMetrics()
			tc.call(m, tc.ctx, tc.err)

			reg := prometheus.NewRegistry()
			reg.MustRegister(m.duration, m.errors)

			labels := map[string]string{"operation": tc.operation}
			assert.Equal(t, 1.0, sample(t, reg, "db_query_duration_seconds", labels), "one observation under the label")
			assert.Equal(t, tc.wantErrors, sample(t, reg, "db_query_errors_total", labels))
		})
	}
}

// sample reads one series: the value of a gauge or counter, the number of
// observations of a histogram, 0 for a series that does not exist.
func sample(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := g.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		for _, metric := range family.GetMetric() {
			if !hasLabels(metric, labels) {
				continue
			}

			switch family.GetType() {
			case dto.MetricType_HISTOGRAM:
				return float64(metric.GetHistogram().GetSampleCount())
			case dto.MetricType_GAUGE:
				return metric.GetGauge().GetValue()
			default:
				return metric.GetCounter().GetValue()
			}
		}
	}

	return 0
}

func hasLabels(metric *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, pair := range metric.GetLabel() {
		got[pair.GetName()] = pair.GetValue()
	}

	for name, value := range want {
		if got[name] != value {
			return false
		}
	}

	return true
}

// silentDB is a DB whose server accepts the TCP connection and never says a
// word: what a dead network path or a hung server looks like to the client.
func silentDB(t *testing.T, mut func(*Config)) *DB {
	t.Helper()

	var lc net.ListenConfig

	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		conns []net.Conn
		done  = make(chan struct{})
	)

	go func() {
		defer close(done)

		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			mu.Lock()

			conns = append(conns, conn)

			mu.Unlock()
		}
	}()

	cfg := Config{
		URL:         secret.New("postgres://app:app@" + listener.Addr().String() + "/app?sslmode=disable"),
		MaxConns:    2,
		DialTimeout: time.Minute,
	}
	if mut != nil {
		mut(&cfg)
	}

	db, _, err := open(t.Context(), cfg, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		// The server first: a connection attempt outlives the call that
		// started it, and Close waits for it.
		require.NoError(t, listener.Close())
		<-done

		mu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()

		db.Close()
	})

	return db
}

// With DB_OP_TIMEOUT a call whose context has no deadline still ends when
// the server does not answer — every method of Conn, and the begin of InTx.
func TestOpTimeout_EndsACallTheServerNeverAnswers(t *testing.T) {
	t.Parallel()

	db := silentDB(t, func(c *Config) { c.OpTimeout = 50 * time.Millisecond })
	ctx := context.Background()

	for name, call := range map[string]func() error{
		"Exec": func() error {
			_, err := db.Conn(ctx).Exec(ctx, "SELECT 1")
			return err
		},
		"Query": func() error {
			rows, err := db.Conn(ctx).Query(ctx, "SELECT 1")
			rows.Close()

			return err
		},
		"QueryRow": func() error { return db.Conn(ctx).QueryRow(ctx, "SELECT 1").Scan(new(int)) },
		"SendBatch": func() error {
			batch := &pgx.Batch{}
			batch.Queue("SELECT 1")

			return db.Conn(ctx).SendBatch(ctx, batch).Close()
		},
		"CopyFrom": func() error {
			_, err := db.Conn(ctx).CopyFrom(ctx, pgx.Identifier{"t"}, []string{"id"}, pgx.CopyFromRows([][]any{{1}}))
			return err
		},
		"InTx": func() error { return db.InTx(ctx, func(context.Context) error { return nil }) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, call(), context.DeadlineExceeded)
		})
	}
}

// DB_DIAL_TIMEOUT is the bound of a connection attempt when nothing else
// is: no deadline on the context, no DB_OP_TIMEOUT.
func TestDialTimeout_EndsAnAttemptTheServerNeverAnswers(t *testing.T) {
	t.Parallel()

	db := silentDB(t, func(c *Config) { c.DialTimeout = 50 * time.Millisecond })

	_, err := db.Conn(context.Background()).Exec(context.Background(), "SELECT 1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// The readiness probe answers within its own timeout though neither its
// context nor the dial would end it, and says the database is what failed.
func TestReadyCheck_HasItsOwnTimeout(t *testing.T) {
	t.Parallel()

	db := silentDB(t, nil)

	start := time.Now()
	err := db.ReadyCheck()(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "postgres unreachable")
	assert.Less(t, time.Since(start), 10*readyCheckTimeout)
}

// The pool gauges and counters follow the pool: a connection being opened,
// an acquire given up on.
func TestCollectors_FollowThePool(t *testing.T) {
	t.Parallel()

	db := silentDB(t, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(db.Collectors()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, err := db.Conn(ctx).Exec(ctx, "SELECT 1")
		done <- err
	}()

	constructing := map[string]string{"state": "constructing"}

	require.Eventually(t, func() bool { return sample(t, reg, "db_pool_connections", constructing) == 1 },
		5*time.Second, 5*time.Millisecond)
	assert.Equal(t, 2.0, sample(t, reg, "db_pool_max_connections", nil))
	assert.Equal(t, 0.0, sample(t, reg, "db_pool_acquires_canceled_total", nil))

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	assert.Equal(t, 1.0, sample(t, reg, "db_pool_acquires_canceled_total", nil))
}

// After Close every way into the database fails with ErrClosed, and a second
// Close is harmless.
func TestClosed(t *testing.T) {
	t.Parallel()

	db, _, err := open(t.Context(), Config{URL: secret.New("postgres://app:app@127.0.0.1:1/app")}, nil)
	require.NoError(t, err)
	db.Close()
	db.Close()

	ctx := context.Background()
	_, execErr := db.Conn(ctx).Exec(ctx, "SELECT 1")
	_, poolErr := db.Pool().Exec(ctx, "SELECT 1")
	called := false

	for name, err := range map[string]error{
		"Conn": execErr,
		"Pool": poolErr,
		"InTx": db.InTx(ctx, func(context.Context) error {
			called = true
			return nil
		}),
		"ReadyCheck": db.ReadyCheck()(ctx),
	} {
		require.ErrorIs(t, err, ErrClosed, name)
	}

	assert.False(t, called, "the unit never started")
}

// Two DBs share metric names: on one registry each needs the registry
// wrapped with a label of its own.
func TestCollectors_TwoPoolsOnOneRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	for _, name := range []string{"primary", "replica"} {
		db, _, err := open(t.Context(), Config{URL: secret.New("postgres://app:app@127.0.0.1:1/app"), MaxConns: 3}, nil)
		require.NoError(t, err)
		t.Cleanup(db.Close)

		wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"pool": name}, reg)
		for _, collector := range db.Collectors() {
			require.NoError(t, wrapped.Register(collector), name)
		}
	}

	assert.Equal(t, 3.0, sample(t, reg, "db_pool_max_connections", map[string]string{"pool": "primary"}))
	assert.Equal(t, 3.0, sample(t, reg, "db_pool_max_connections", map[string]string{"pool": "replica"}))
}

// A call that fails before it reaches a connection — here the pool is closed
// — is a failed query of its operation, though the pgx tracer never sees it.
func TestQueryErrors_CountACallThatNeverGotAConnection(t *testing.T) {
	t.Parallel()

	db, _, err := open(t.Context(), Config{URL: secret.New("postgres://app:app@127.0.0.1:1/app")}, nil)
	require.NoError(t, err)
	db.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(db.Collectors()...)

	ctx := context.Background()
	named := WithOperation(ctx, "tasks.create")

	for operation, call := range map[string]func() error{
		"select": func() error {
			_, err := db.Conn(ctx).Exec(ctx, "SELECT 1")
			return err
		},
		"update": func() error {
			rows, err := db.Conn(ctx).Query(ctx, "UPDATE t SET a = 1 RETURNING a")
			rows.Close()

			return err
		},
		"tasks.create": func() error { return db.Conn(named).QueryRow(named, "SELECT 1").Scan(new(int)) },
		"batch": func() error {
			batch := &pgx.Batch{}
			batch.Queue("SELECT 1")

			return db.Conn(ctx).SendBatch(ctx, batch).Close()
		},
		"copy": func() error {
			_, err := db.Conn(ctx).CopyFrom(ctx, pgx.Identifier{"t"}, []string{"id"}, pgx.CopyFromRows(nil))
			return err
		},
		"begin": func() error { return db.InTx(named, func(context.Context) error { return nil }) },
	} {
		require.ErrorIs(t, call(), ErrClosed, operation)
		assert.Equal(t, 1.0, sample(t, reg, "db_query_errors_total", map[string]string{"operation": operation}), operation)
	}
}
