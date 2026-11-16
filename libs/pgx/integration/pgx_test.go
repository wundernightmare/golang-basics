package integration_test

import (
	"context"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// schemaConfig returns a config whose connections live in a fresh schema of
// the shared Postgres: search_path points there, so the tables a test (or a
// migration) creates are its own.
func schemaConfig(t testing.TB) (pgx.Config, string) {
	t.Helper()
	base := containers.Postgres(t)
	schema := strings.ReplaceAll(testx.Unique("t"), "-", "_")
	ctx := context.Background()
	admin, err := pgx.New(ctx, pgx.Config{URL: secret.New(base), MaxConns: 1}, nil)
	require.NoError(t, err)
	_, err = admin.Pool().Exec(ctx, `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	admin.Close()

	u, err := url.Parse(base)
	require.NoError(t, err)

	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()

	return pgx.Config{URL: secret.New(u.String()), DialTimeout: 5 * time.Second, MaxConns: 5}, schema
}

func newDB(t testing.TB, cfg pgx.Config, opts ...pgx.Option) *pgx.DB {
	t.Helper()

	db, err := pgx.New(context.Background(), cfg, nil, opts...)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	return db
}

var widgets = fstest.MapFS{
	"001_widgets.sql":      {Data: []byte(`CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT NOT NULL);`)},
	"002_seed_widgets.sql": {Data: []byte("INSERT INTO widgets (id, name) VALUES (1, 'gadget');\nINSERT INTO widgets (id, name) VALUES (2, 'gizmo');")},
}

func appliedVersions(t testing.TB, db *pgx.DB) []int64 {
	t.Helper()

	rows, err := db.Pool().Query(context.Background(), `SELECT version FROM schema_migrations ORDER BY version`)
	require.NoError(t, err)

	var out []int64

	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		out = append(out, v)
	}

	require.NoError(t, rows.Err())

	return out
}

func TestMigrate_FreshSchemaThenRerunIsANoop(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	db := newDB(t, cfg)
	ctx := context.Background()

	require.NoError(t, db.Migrate(ctx, widgets))
	assert.Equal(t, []int64{1, 2}, appliedVersions(t, db))

	var n int
	require.NoError(t, db.Pool().QueryRow(ctx, `SELECT count(*) FROM widgets`).Scan(&n))
	assert.Equal(t, 2, n, "a multi-statement file runs every statement")

	// Re-run: nothing applies again (the seed would violate the primary key).
	require.NoError(t, pgx.Migrate(ctx, db.Pool(), widgets, nil))
	assert.Equal(t, []int64{1, 2}, appliedVersions(t, db))

	// A new file is picked up on the next run.
	next := fstest.MapFS{"003_widget_color.sql": {Data: []byte(`ALTER TABLE widgets ADD COLUMN color TEXT`)}}
	maps.Copy(next, widgets)

	require.NoError(t, pgx.Migrate(ctx, db.Pool(), next, nil))
	assert.Equal(t, []int64{1, 2, 3}, appliedVersions(t, db))
}

func TestMigrate_ConcurrentRunnersDoNotCollide(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	ctx := context.Background()
	// Separate pools: separate "replicas".
	dbs := []*pgx.DB{newDB(t, cfg), newDB(t, cfg), newDB(t, cfg), newDB(t, cfg)}

	var wg sync.WaitGroup

	errs := make([]error, len(dbs))
	start := make(chan struct{})

	for i, db := range dbs {
		wg.Go(func() {
			<-start

			errs[i] = pgx.Migrate(ctx, db.Pool(), widgets, nil)
		})
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "runner %d", i)
	}

	assert.Equal(t, []int64{1, 2}, appliedVersions(t, dbs[0]), "each version applied exactly once")

	var n int
	require.NoError(t, dbs[0].Pool().QueryRow(ctx, `SELECT count(*) FROM widgets`).Scan(&n))
	assert.Equal(t, 2, n)
}

// A runner that finds the migration lock taken waits for it however long the
// holder needs: lock_timeout and statement_timeout are lifted for that wait,
// and are back for the migration itself.
func TestMigrate_SecondRunnerOutwaitsTheSessionTimeouts(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	cfg.LockTimeout = 200 * time.Millisecond
	cfg.StatementTimeout = 500 * time.Millisecond
	ctx := context.Background()
	dbs := []*pgx.DB{newDB(t, cfg), newDB(t, cfg)}

	// Longer in total than either timeout, each statement within both.
	slow := fstest.MapFS{"001_slow.sql": {Data: []byte(`
		DO $$ BEGIN
			IF current_setting('lock_timeout') <> '200ms' OR current_setting('statement_timeout') <> '500ms' THEN
				RAISE EXCEPTION 'guard rails are off inside the migration: lock_timeout=%, statement_timeout=%',
					current_setting('lock_timeout'), current_setting('statement_timeout');
			END IF;
		END $$;
		SELECT pg_sleep(0.3); SELECT pg_sleep(0.3); SELECT pg_sleep(0.3);
		CREATE TABLE slow (id INT PRIMARY KEY);`)}}

	var wg sync.WaitGroup

	errs := make([]error, len(dbs))

	for i, db := range dbs {
		wg.Go(func() { errs[i] = pgx.Migrate(ctx, db.Pool(), slow, nil) })
	}

	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "runner %d", i)
	}

	assert.Equal(t, []int64{1}, appliedVersions(t, dbs[0]))
}

func TestMigrate_FailingMigrationRollsBackAndIsNamed(t *testing.T) {
	t.Parallel()

	cfg, schema := schemaConfig(t)
	db := newDB(t, cfg)
	ctx := context.Background()

	broken := fstest.MapFS{
		"001_ok.sql":     {Data: []byte(`CREATE TABLE ok_table (id INT)`)},
		"002_broken.sql": {Data: []byte("CREATE TABLE half_done (id INT);\nTHIS IS NOT VALID SQL;")},
		"003_never.sql":  {Data: []byte(`CREATE TABLE never_reached (id INT)`)},
	}
	err := pgx.Migrate(ctx, db.Pool(), broken, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "002_broken.sql", "the error names the file")

	assert.Equal(t, []int64{1}, appliedVersions(t, db), "earlier migrations stay, the failing one is not recorded")

	for table, want := range map[string]bool{"ok_table": true, "half_done": false, "never_reached": false} {
		var exists bool
		require.NoError(t, db.Pool().QueryRow(ctx,
			`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
			schema, table).Scan(&exists))
		assert.Equal(t, want, exists, table)
	}
}

func TestSessionTimeoutsAreApplied(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	cfg.StatementTimeout = 1500 * time.Millisecond
	cfg.LockTimeout = 250 * time.Millisecond
	cfg.IdleInTransactionSessionTimeout = 0 // left at the server default
	db := newDB(t, cfg)
	ctx := context.Background()

	show := func(name string) string {
		var v string
		require.NoError(t, db.Pool().QueryRow(ctx, "SHOW "+name).Scan(&v))

		return v
	}
	assert.Equal(t, "1500ms", show("statement_timeout"))
	assert.Equal(t, "250ms", show("lock_timeout"))
	assert.Equal(t, "0", show("idle_in_transaction_session_timeout"), "zero leaves the server default (off)")

	// And the statement timeout bites: Postgres cancels, the error says so.
	_, err := db.Pool().Exec(ctx, `SELECT pg_sleep(3)`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "statement timeout")
}

// DB_PASSWORD_FILE wins over the URL's password; a bad file fails the boot.
func TestNew_PasswordFile(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	u, err := url.Parse(cfg.URL.Expose())
	require.NoError(t, err)

	right, _ := u.User.Password()
	u.User = url.UserPassword(u.User.Username(), "wrong")
	cfg.URL = secret.New(u.String())

	dir := t.TempDir()
	cfg.PasswordFile = dir + "/pw"
	require.NoError(t, os.WriteFile(cfg.PasswordFile, []byte(right+"\n"), 0o600))
	db := newDB(t, cfg)
	require.NoError(t, db.ReadyCheck()(context.Background()), "the file's password (trailing newline dropped) is used")

	for name, path := range map[string]string{"missing": dir + "/nope", "empty": dir + "/empty"} {
		require.NoError(t, os.WriteFile(dir+"/empty", []byte("\n"), 0o600))

		cfg.PasswordFile = path
		_, err := pgx.New(context.Background(), cfg, nil)
		require.ErrorContains(t, err, "DB_PASSWORD_FILE", name)
	}
}

// A rotated secret mount reaches the connections opened after it: the file is
// read for each of them, not once at boot.
func TestNew_PasswordFileIsReadForEveryConnection(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	admin := newDB(t, cfg)
	ctx := context.Background()
	role := strings.ReplaceAll(testx.Unique("rotating"), "-", "_")
	_, err := admin.Pool().Exec(ctx, `CREATE ROLE `+role+` LOGIN PASSWORD 'first'`)
	require.NoError(t, err)

	u, err := url.Parse(cfg.URL.Expose())
	require.NoError(t, err)

	u.User = url.User(role)
	cfg.URL = secret.New(u.String())
	cfg.PasswordFile = filepath.Join(t.TempDir(), "pw")
	require.NoError(t, os.WriteFile(cfg.PasswordFile, []byte("first"), 0o600))

	db := newDB(t, cfg)

	_, err = admin.Pool().Exec(ctx, `ALTER ROLE `+role+` PASSWORD 'second'`)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfg.PasswordFile, []byte("second"), 0o600))
	db.Pool().Reset() // what DB_MAX_CONN_LIFETIME does in time

	require.NoError(t, db.ReadyCheck()(ctx))
}

// DB_OP_TIMEOUT stays out of the way of calls that are answered — results are
// read to the end and their connection goes back — and ends the ones waiting
// for a connection of an exhausted pool, which no server-side timeout sees.
func TestOpTimeout_AndAnExhaustedPool(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	cfg.MaxConns = 1
	cfg.OpTimeout = 300 * time.Millisecond
	cfg.StatementTimeout = 100 * time.Millisecond
	db := newDB(t, cfg)
	repo := items{db: db}
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	reg.MustRegister(db.Collectors()...)

	_, err := db.Conn(ctx).Exec(ctx, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	require.NoError(t, db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 1, "a") }))

	batch := &pgxv5.Batch{}
	batch.Queue(`INSERT INTO items (id, name) VALUES (2, 'b')`)
	require.NoError(t, db.Conn(ctx).SendBatch(ctx, batch).Close())

	var n int
	require.NoError(t, db.Conn(ctx).QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&n))
	assert.Equal(t, 2, n)
	assert.Equal(t, []int{1, 2}, repo.committed(t))
	require.Zero(t, db.Pool().Stat().AcquiredConns(), "every result gave its connection back")

	held, err := db.Pool().Acquire(ctx)
	require.NoError(t, err)

	require.ErrorIs(t, repo.insert(ctx, 3, "c"), context.DeadlineExceeded, "a context without a deadline, a pool without a connection")
	require.ErrorIs(t, db.InTx(ctx, func(context.Context) error { return nil }), context.DeadlineExceeded)
	require.ErrorContains(t, db.ReadyCheck()(ctx), "postgres pool exhausted (1 of 1 connections in use)")
	assert.Equal(t, 1.0, testx.Metric(t, reg, "db_pool_connections", map[string]string{"state": "in_use"}))

	// Neither call reached a connection, and both are failed queries.
	for _, operation := range []string{"insert", "begin"} {
		assert.Equal(t, 1.0, testx.Metric(t, reg, "db_query_errors_total", map[string]string{"operation": operation}), operation)
	}

	held.Release()

	// pgxpool counts an acquire as "waited" when no idle connection was
	// there for it — the first one of this pool already was.
	stat := db.Pool().Stat()
	require.Positive(t, stat.EmptyAcquireCount())
	assert.Equal(t, float64(stat.EmptyAcquireCount()), testx.Metric(t, reg, "db_pool_acquires_waited_total", nil))
	assert.Positive(t, testx.Metric(t, reg, "db_pool_acquire_wait_seconds_total", nil))
}

func TestReadyCheckFailsWhenClosed(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	ctx := context.Background()
	db, err := pgx.New(ctx, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, db.ReadyCheck()(ctx))
	db.Close()
	require.Error(t, db.ReadyCheck()(ctx), "readiness must fail once the pool is closed")
}

func TestNew_ZeroDialTimeoutStillPings(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	cfg.DialTimeout = 0 // a hand-built config: must not expire the boot ping immediately
	db := newDB(t, cfg)
	require.NoError(t, db.ReadyCheck()(context.Background()))
}

// Every query is a client span inside the caller's trace, and the pool
// statistics and query metrics move with real usage.
func TestQueriesAreTracedAndMeasured(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	recorder := testx.Recorder(t)
	db := newDB(t, cfg, pgx.WithTracerProvider(recorder.Provider))
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	reg.MustRegister(db.Collectors()...)

	qctx, parent := recorder.Tracer().Start(ctx, "handler")

	var one int
	require.NoError(t, db.Pool().QueryRow(qctx, "SELECT 1").Scan(&one))
	_, err := db.Pool().Exec(pgx.WithOperation(qctx, "widgets.broken"), "SELECT id FROM no_such_table")
	require.Error(t, err)
	parent.End()

	// The query span is a direct child of the handler span (its prepare step,
	// also named by the statement, is a child of the query span).
	var query sdktrace.ReadOnlySpan

	for _, s := range recorder.Ended() {
		if s.SpanKind() == trace.SpanKindClient && strings.HasPrefix(strings.ToUpper(s.Name()), "SELECT") &&
			s.Parent().SpanID() == parent.SpanContext().SpanID() {
			query = s
			break
		}
	}

	require.NotNil(t, query, "a client span named by the statement, child of the caller's span")
	assert.Equal(t, parent.SpanContext().TraceID(), query.SpanContext().TraceID())

	assert.Equal(t, float64(cfg.MaxConns), testx.Metric(t, reg, "db_pool_max_connections", nil))
	assert.Equal(t, 0.0, testx.Metric(t, reg, "db_pool_connections", map[string]string{"state": "in_use"}), "released after the query")
	assert.GreaterOrEqual(t, testx.Metric(t, reg, "db_pool_connections", map[string]string{"state": "idle"}), 1.0)
	assert.GreaterOrEqual(t, testx.Metric(t, reg, "db_pool_acquire_wait_seconds_total", nil), 0.0)
	assert.GreaterOrEqual(t, testx.Metric(t, reg, "db_query_duration_seconds", map[string]string{"operation": "select"}), 1.0,
		"unnamed queries are labelled by their keyword")
	assert.Equal(t, 1.0, testx.Metric(t, reg, "db_query_duration_seconds", map[string]string{"operation": "widgets.broken"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "db_query_errors_total", map[string]string{"operation": "widgets.broken"}))
	testx.NoMetric(t, reg, "db_query_errors_total", map[string]string{"operation": "select"})
	testx.LintMetrics(t, reg)
}

// SendBatch and CopyFrom are measured like a query, as one operation each,
// and the statements that only delimit a transaction never count towards the
// operation they delimit.
func TestBatchCopyAndTransactionControlAreMeasured(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	reg.MustRegister(repo.db.Collectors()...)

	count := func(name, operation string) float64 {
		return testx.Metric(t, reg, name, map[string]string{"operation": operation})
	}

	duplicate := &pgxv5.Batch{}
	duplicate.Queue(`INSERT INTO items (id, name) VALUES (1, 'a')`)
	duplicate.Queue(`INSERT INTO items (id, name) VALUES (1, 'again')`)
	require.Error(t, repo.db.Conn(ctx).SendBatch(ctx, duplicate).Close())
	require.NoError(t, repo.db.Conn(ctx).SendBatch(ctx, &pgxv5.Batch{}).Close(), "pgx does not trace an empty batch")

	assert.Equal(t, 1.0, count("db_query_duration_seconds", "batch"), "one observation for the batch, not one per statement")
	assert.Equal(t, 1.0, count("db_query_errors_total", "batch"), "the empty one is not a failure")

	_, err := repo.db.Conn(ctx).CopyFrom(pgx.WithOperation(ctx, "items.load"), pgxv5.Identifier{"items"}, []string{"id", "name"},
		pgxv5.CopyFromRows([][]any{{2, "b"}, {3, "c"}}))
	require.NoError(t, err)
	assert.Equal(t, 1.0, count("db_query_duration_seconds", "items.load"))

	require.NoError(t, repo.db.InTx(pgx.WithOperation(ctx, "items.add"), func(ctx context.Context) error {
		return repo.insert(ctx, 4, "d")
	}))
	assert.Equal(t, 1.0, count("db_query_duration_seconds", "items.add"), "the one statement of the unit")
	assert.Equal(t, 1.0, count("db_query_duration_seconds", "begin"))
	assert.Equal(t, 1.0, count("db_query_duration_seconds", "commit"))
}
