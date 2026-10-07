package pgx_test

import (
	"context"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// One Postgres per test binary; tests own their schema by name.
func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

func TestConfigDSN(t *testing.T) {
	t.Run("explicit URL wins", func(t *testing.T) {
		cfg := pgx.Config{URL: "postgres://u:p@h:1/db?sslmode=require", Host: "ignored"}
		assert.Equal(t, "postgres://u:p@h:1/db?sslmode=require", cfg.DSN())
	})
	t.Run("assembled from fields", func(t *testing.T) {
		cfg := pgx.Config{Host: "db", Port: 5432, User: "app", Password: "s3cret", Name: "app", SSLMode: "disable"}
		assert.Equal(t, "postgres://app:s3cret@db:5432/app?sslmode=disable", cfg.DSN())
	})
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := pgx.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, 5432, cfg.Port)
	assert.Equal(t, int32(10), cfg.MaxConns)
	assert.Equal(t, 30*time.Second, cfg.StatementTimeout)
	assert.Equal(t, 5*time.Second, cfg.LockTimeout)
	assert.Equal(t, time.Minute, cfg.IdleInTransactionSessionTimeout)

	t.Setenv("TASKS_DB_HOST", "db.internal")
	t.Setenv("TASKS_DB_MAX_CONNS", "42")
	t.Setenv("TASKS_DB_STATEMENT_TIMEOUT", "2s")
	cfg, err = pgx.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "db.internal", cfg.Host)
	assert.Equal(t, int32(42), cfg.MaxConns)
	assert.Equal(t, 2*time.Second, cfg.StatementTimeout)
}

func TestLoadMigrations(t *testing.T) {
	t.Run("ordered by version, not by name", func(t *testing.T) {
		ms, err := pgx.LoadMigrations(fstest.MapFS{
			"010_later.sql": {Data: []byte("SELECT 10")},
			"2_second.sql":  {Data: []byte("SELECT 2")},
			"001_first.sql": {Data: []byte("SELECT 1")},
			"README.md":     {Data: []byte("not a migration")},
			"sub/003_x.sql": {Data: []byte("ignored: not at the root")},
		})
		require.NoError(t, err)
		require.Len(t, ms, 3)
		assert.Equal(t, []int64{1, 2, 10}, []int64{ms[0].Version, ms[1].Version, ms[2].Version})
		assert.Equal(t, "first", ms[0].Name)
		assert.Equal(t, "001_first.sql", ms[0].File)
	})
	t.Run("a misnamed file is an error", func(t *testing.T) {
		_, err := pgx.LoadMigrations(fstest.MapFS{"create-tasks.sql": {Data: []byte("x")}})
		require.ErrorContains(t, err, "create-tasks.sql")
	})
	t.Run("a duplicate version is an error", func(t *testing.T) {
		_, err := pgx.LoadMigrations(fstest.MapFS{
			"001_a.sql": {Data: []byte("x")}, "1_b.sql": {Data: []byte("y")},
		})
		require.ErrorContains(t, err, "share version 1")
	})
}

// schemaConfig returns a config whose connections live in a fresh schema of
// the shared Postgres: search_path points there, so the tables a test (or a
// migration) creates are its own.
func schemaConfig(t testing.TB) (pgx.Config, string) {
	t.Helper()
	base := containers.Postgres(t)
	schema := strings.ReplaceAll(testx.Unique("t"), "-", "_")
	ctx := context.Background()
	admin, err := pgx.New(ctx, pgx.Config{URL: base, MaxConns: 1}, nil)
	require.NoError(t, err)
	_, err = admin.Pool().Exec(ctx, `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	admin.Close()

	u, err := url.Parse(base)
	require.NoError(t, err)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return pgx.Config{URL: u.String(), ConnectTimeout: 5 * time.Second, MaxConns: 5}, schema
}

func newDB(t testing.TB, cfg pgx.Config) *pgx.DB {
	t.Helper()
	db, err := pgx.New(context.Background(), cfg, nil)
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
	cfg, _ := schemaConfig(t)
	db := newDB(t, cfg)
	ctx := context.Background()

	require.NoError(t, pgx.Migrate(ctx, db.Pool(), widgets, nil))
	assert.Equal(t, []int64{1, 2}, appliedVersions(t, db))
	var n int
	require.NoError(t, db.Pool().QueryRow(ctx, `SELECT count(*) FROM widgets`).Scan(&n))
	assert.Equal(t, 2, n, "a multi-statement file runs every statement")

	// Re-run: nothing applies again (the seed would violate the primary key).
	require.NoError(t, pgx.Migrate(ctx, db.Pool(), widgets, nil))
	assert.Equal(t, []int64{1, 2}, appliedVersions(t, db))

	// A new file is picked up on the next run.
	next := fstest.MapFS{"003_widget_color.sql": {Data: []byte(`ALTER TABLE widgets ADD COLUMN color TEXT`)}}
	for k, v := range widgets {
		next[k] = v
	}
	require.NoError(t, pgx.Migrate(ctx, db.Pool(), next, nil))
	assert.Equal(t, []int64{1, 2, 3}, appliedVersions(t, db))
}

func TestMigrate_ConcurrentRunnersDoNotCollide(t *testing.T) {
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

func TestMigrate_FailingMigrationRollsBackAndIsNamed(t *testing.T) {
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

func TestReadyCheckFailsWhenClosed(t *testing.T) {
	cfg, _ := schemaConfig(t)
	ctx := context.Background()
	db, err := pgx.New(ctx, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, db.ReadyCheck()(ctx))
	db.Close()
	require.Error(t, db.ReadyCheck()(ctx), "readiness must fail once the pool is closed")
}

func TestNew_ZeroConnectTimeoutStillPings(t *testing.T) {
	cfg, _ := schemaConfig(t)
	cfg.ConnectTimeout = 0 // a hand-built config: must not expire the boot ping immediately
	db := newDB(t, cfg)
	require.NoError(t, db.ReadyCheck()(context.Background()))
}

// Every query is a client span inside the caller's trace, and the pool
// statistics and query metrics move with real usage.
func TestQueriesAreTracedAndMeasured(t *testing.T) {
	cfg, _ := schemaConfig(t)
	sr := testx.Recorder(t)
	db := newDB(t, cfg)
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	reg.MustRegister(db.Collectors()...)

	before := testx.Metric(t, reg, "pgxpool_acquire_total", nil)

	qctx, parent := otel.Tracer("test").Start(ctx, "handler")
	var one int
	require.NoError(t, db.Pool().QueryRow(qctx, "SELECT 1").Scan(&one))
	_, err := db.Pool().Exec(pgx.WithOperation(qctx, "widgets.broken"), "SELECT * FROM no_such_table")
	require.Error(t, err)
	parent.End()

	// The query span is a direct child of the handler span (its prepare step,
	// also named by the statement, is a child of the query span).
	var query sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanKind() == trace.SpanKindClient && strings.HasPrefix(strings.ToUpper(s.Name()), "SELECT") &&
			s.Parent().SpanID() == parent.SpanContext().SpanID() {
			query = s
			break
		}
	}
	require.NotNil(t, query, "a client span named by the statement, child of the caller's span")
	assert.Equal(t, parent.SpanContext().TraceID(), query.SpanContext().TraceID())

	assert.Greater(t, testx.Metric(t, reg, "pgxpool_acquire_total", nil), before, "the query acquired a connection")
	assert.Equal(t, float64(cfg.MaxConns), testx.Metric(t, reg, "pgxpool_max_conns", nil))
	assert.Equal(t, 0.0, testx.Metric(t, reg, "pgxpool_conns", map[string]string{"state": "acquired"}), "released after the query")
	assert.GreaterOrEqual(t, testx.Metric(t, reg, "pgx_query_duration_seconds", map[string]string{"operation": "select"}), 1.0,
		"unnamed queries are labelled by their keyword")
	assert.Equal(t, 1.0, testx.Metric(t, reg, "pgx_query_duration_seconds", map[string]string{"operation": "widgets.broken"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "pgx_query_errors_total", map[string]string{"operation": "widgets.broken"}))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "pgx_query_errors_total", map[string]string{"operation": "select"}))
	testx.LintMetrics(t, reg)
}

func TestNew_UnreachableDatabaseFailsFast(t *testing.T) {
	start := time.Now()
	_, err := pgx.New(context.Background(), pgx.Config{
		URL: "postgres://app:app@127.0.0.1:1/app?sslmode=disable", ConnectTimeout: time.Second,
	}, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.NotErrorIs(t, err, context.Canceled)
}
