package pgx_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/testo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// One Postgres per test binary (testx), tests own their tables by name.
func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }

func TestConfigDSN(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		testo.Run(t, "explicit URL wins", func(t testx.T) {
			cfg := pgx.Config{URL: "postgres://u:p@h:1/db?sslmode=require", Host: "ignored"}
			require.Equal(t, "postgres://u:p@h:1/db?sslmode=require", cfg.DSN())
		})
		testo.Run(t, "assembled from fields", func(t testx.T) {
			cfg := pgx.Config{Host: "db", Port: 5432, User: "app", Password: "s3cret", Name: "app", SSLMode: "disable"}
			require.Equal(t, "postgres://app:s3cret@db:5432/app?sslmode=disable", cfg.DSN())
		})
	}, "pgx", "unit")
}

func TestLoadConfigDefaults(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cfg, err := pgx.LoadConfig("TASKS_")
		require.NoError(t, err)
		require.Equal(t, "localhost", cfg.Host)
		require.Equal(t, 5432, cfg.Port)
		require.Equal(t, int32(10), cfg.MaxConns)

		t.Setenv("TASKS_DB_HOST", "db.internal")
		t.Setenv("TASKS_DB_MAX_CONNS", "42")
		cfg, err = pgx.LoadConfig("TASKS_")
		require.NoError(t, err)
		require.Equal(t, "db.internal", cfg.Host)
		require.Equal(t, int32(42), cfg.MaxConns)
	}, "pgx", "unit")
}

func sharedConfig(t testing.TB) pgx.Config {
	t.Helper()
	return pgx.Config{URL: testx.Postgres(t), ConnectTimeout: 5 * time.Second, MaxConns: 5}
}

func TestPoolLifecycle(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cfg := sharedConfig(t)
		ctx := context.Background()
		table := strings.ReplaceAll(testx.Unique("widgets"), "-", "_")

		db, err := pgx.New(ctx, cfg, nil)
		require.NoError(t, err)
		defer db.Close()

		// Readiness probe passes against a live database…
		require.NoError(t, db.ReadyCheck()(ctx))

		// …migrations apply transactionally…
		require.NoError(t, db.Migrate(ctx,
			`CREATE TABLE IF NOT EXISTS `+table+` (id INT PRIMARY KEY, name TEXT NOT NULL)`,
			`INSERT INTO `+table+` (id, name) VALUES (1, 'gadget') ON CONFLICT DO NOTHING`,
		))

		// …and the pool runs queries.
		var name string
		require.NoError(t, db.Pool().QueryRow(ctx, `SELECT name FROM `+table+` WHERE id = 1`).Scan(&name))
		require.Equal(t, "gadget", name)

		// A failing migration rolls back without partial application.
		gizmos := strings.ReplaceAll(testx.Unique("gizmos"), "-", "_")
		err = db.Migrate(ctx,
			`CREATE TABLE IF NOT EXISTS `+gizmos+` (id INT PRIMARY KEY)`,
			`THIS IS NOT VALID SQL`,
		)
		require.Error(t, err)
		var exists bool
		require.NoError(t, db.Pool().QueryRow(ctx,
			`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)`, gizmos).Scan(&exists))
		require.False(t, exists, "failed migration must not leave the table behind")
	}, "pgx", "integration")
}

func TestReadyCheckFailsWhenClosed(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cfg := sharedConfig(t)
		ctx := context.Background()

		db, err := pgx.New(ctx, cfg, nil)
		require.NoError(t, err)
		db.Close()

		require.Error(t, db.ReadyCheck()(ctx), "readiness must fail once the pool is closed")
	}, "pgx", "integration")
}

// Every query is a client span inside the caller's trace, and the pool
// statistics on /metrics move with real usage.
func TestQueriesAreTracedAndPoolStatsAreExported(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		cfg := sharedConfig(t)
		sr := testx.Recorder(t)
		ctx := context.Background()

		db, err := pgx.New(ctx, cfg, nil)
		require.NoError(t, err)
		defer db.Close()
		reg := prometheus.NewRegistry()
		reg.MustRegister(db.Collectors()...)

		before := testx.Metric(t, reg, "pgxpool_acquire_total", nil)

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

		require.Greater(t, testx.Metric(t, reg, "pgxpool_acquire_total", nil), before, "the query acquired a connection")
		require.Equal(t, float64(cfg.MaxConns), testx.Metric(t, reg, "pgxpool_max_conns", nil))
		require.Equal(t, 0.0, testx.Metric(t, reg, "pgxpool_conns", map[string]string{"state": "acquired"}), "released after the query")
		testx.LintMetrics(t, reg)
	}, "pgx", "integration", "telemetry")
}
