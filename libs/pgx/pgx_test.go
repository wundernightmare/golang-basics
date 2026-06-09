package pgx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
)

func TestConfigDSN(t *testing.T) {
	t.Parallel()

	t.Run("explicit URL wins", func(t *testing.T) {
		t.Parallel()
		cfg := pgx.Config{URL: "postgres://u:p@h:1/db?sslmode=require", Host: "ignored"}
		require.Equal(t, "postgres://u:p@h:1/db?sslmode=require", cfg.DSN())
	})

	t.Run("assembled from fields", func(t *testing.T) {
		t.Parallel()
		cfg := pgx.Config{Host: "db", Port: 5432, User: "app", Password: "s3cret", Name: "app", SSLMode: "disable"}
		require.Equal(t, "postgres://app:s3cret@db:5432/app?sslmode=disable", cfg.DSN())
	})
}

func TestLoadConfigDefaults(t *testing.T) {
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
}

// startPostgres spins up an ephemeral PostgreSQL via testcontainers and returns
// a Config pointing at it. It skips the test (rather than failing) when running
// with -short or when Docker is unavailable, so unit-only runs stay fast.
func startPostgres(t *testing.T) pgx.Config {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("app"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			require.NoError(t, err, "postgres container must start in CI")
		}
		t.Skipf("could not start postgres container (docker unavailable?): %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return pgx.Config{URL: dsn, ConnectTimeout: 5 * time.Second, MaxConns: 5}
}

func TestPoolLifecycle(t *testing.T) {
	cfg := startPostgres(t)
	ctx := context.Background()

	db, err := pgx.New(ctx, cfg, nil)
	require.NoError(t, err)
	defer db.Close()

	// Readiness probe passes against a live database…
	require.NoError(t, db.ReadyCheck()(ctx))

	// …migrations apply transactionally…
	require.NoError(t, db.Migrate(ctx,
		`CREATE TABLE IF NOT EXISTS widgets (id INT PRIMARY KEY, name TEXT NOT NULL)`,
		`INSERT INTO widgets (id, name) VALUES (1, 'gadget') ON CONFLICT DO NOTHING`,
	))

	// …and the pool runs queries.
	var name string
	require.NoError(t, db.Pool().QueryRow(ctx, `SELECT name FROM widgets WHERE id = 1`).Scan(&name))
	require.Equal(t, "gadget", name)

	// A failing migration rolls back without partial application.
	err = db.Migrate(ctx,
		`CREATE TABLE IF NOT EXISTS gizmos (id INT PRIMARY KEY)`,
		`THIS IS NOT VALID SQL`,
	)
	require.Error(t, err)
	var exists bool
	require.NoError(t, db.Pool().QueryRow(ctx,
		`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'gizmos')`).Scan(&exists))
	require.False(t, exists, "failed migration must not leave the gizmos table behind")
}

func TestReadyCheckFailsWhenClosed(t *testing.T) {
	cfg := startPostgres(t)
	ctx := context.Background()

	db, err := pgx.New(ctx, cfg, nil)
	require.NoError(t, err)
	db.Close()

	require.Error(t, db.ReadyCheck()(ctx), "readiness must fail once the pool is closed")
}
