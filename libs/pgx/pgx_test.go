package pgx_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/secret"
)

// Nothing here needs a database; what does lives in the integration module.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestConfigDSN(t *testing.T) {
	t.Parallel()

	fields := func(host string) pgx.Config {
		return pgx.Config{Host: host, Port: 5432, User: "app", Password: secret.New("s3cret"), Name: "app", SSLMode: "disable"}
	}

	for name, tc := range map[string]struct {
		cfg  pgx.Config
		want string
	}{
		"explicit URL wins": {
			pgx.Config{URL: secret.New("postgres://u:p@h:1/db?sslmode=require"), Host: "ignored"},
			"postgres://u:p@h:1/db?sslmode=require",
		},
		"assembled from fields":  {fields("db"), "postgres://app:s3cret@db:5432/app?sslmode=disable"},
		"IPv6 literal":           {fields("::1"), "postgres://app:s3cret@[::1]:5432/app?sslmode=disable"},
		"IPv6 literal, brackets": {fields("[2001:db8::10]"), "postgres://app:s3cret@[2001:db8::10]:5432/app?sslmode=disable"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, tc.cfg.DSN())
			require.NoError(t, tc.cfg.Validate(), "and pgx parses it")
		})
	}
}

// The process-environment path of LoadConfig (LoadConfigFrom with nil);
// everything else reads a map through LoadConfigFrom. Serial: t.Setenv
// cannot run in a parallel test.
func TestLoadConfig_ReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv("TASKS_DB_HOST", "db.from-env")

	cfg, err := pgx.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "db.from-env", cfg.Host, "LoadConfig reads the process environment")
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := pgx.LoadConfigFrom("TASKS_", map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, 5432, cfg.Port)
	assert.Equal(t, int32(10), cfg.MaxConns)
	assert.Equal(t, 30*time.Second, cfg.StatementTimeout)
	assert.Equal(t, 5*time.Second, cfg.LockTimeout)
	assert.Equal(t, time.Minute, cfg.IdleInTransactionSessionTimeout)
	assert.Zero(t, cfg.OpTimeout, "no client-side deadline unless asked for")

	cfg, err = pgx.LoadConfigFrom("TASKS_", map[string]string{
		"TASKS_DB_HOST":              "db.internal",
		"TASKS_DB_MAX_CONNS":         "42",
		"TASKS_DB_STATEMENT_TIMEOUT": "2s",
		"TASKS_DB_OP_TIMEOUT":        "3s",
	})
	require.NoError(t, err)
	assert.Equal(t, "db.internal", cfg.Host)
	assert.Equal(t, int32(42), cfg.MaxConns)
	assert.Equal(t, 2*time.Second, cfg.StatementTimeout)
	assert.Equal(t, 3*time.Second, cfg.OpTimeout)

	_, err = pgx.LoadConfigFrom("TASKS_", map[string]string{"TASKS_DB_MIN_CONNS": "11"})
	require.ErrorContains(t, err, "DB_MIN_CONNS", "what Validate refuses does not load")
}

func TestLoadMigrations(t *testing.T) {
	t.Parallel()

	t.Run("ordered by version, not by name", func(t *testing.T) {
		t.Parallel()

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
		t.Parallel()

		_, err := pgx.LoadMigrations(fstest.MapFS{"create-tasks.sql": {Data: []byte("x")}})
		require.ErrorContains(t, err, "create-tasks.sql")
	})
	t.Run("a duplicate version is an error", func(t *testing.T) {
		t.Parallel()

		_, err := pgx.LoadMigrations(fstest.MapFS{
			"001_a.sql": {Data: []byte("x")}, "1_b.sql": {Data: []byte("y")},
		})
		require.ErrorContains(t, err, "share version 1")
	})
}

// Validate refuses, without connecting, what New would refuse — so
// --check-config and the boot catch it before the first dial — and New
// refuses it with the same words.
func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pw"), []byte("s3cret\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "empty"), []byte("\n"), 0o600))

	valid, err := pgx.LoadConfigFrom("", map[string]string{})
	require.NoError(t, err)

	with := func(mut func(*pgx.Config)) pgx.Config {
		cfg := valid
		mut(&cfg)

		return cfg
	}

	require.NoError(t, with(func(c *pgx.Config) { c.PasswordFile = filepath.Join(dir, "pw") }).Validate())

	for name, tc := range map[string]struct {
		cfg  pgx.Config
		want string
	}{
		"URL with a port that is not one": {
			with(func(c *pgx.Config) { c.URL = secret.New("postgres://app:Sup3rS3cret@db:not-a-port/app") }), "parse dsn",
		},
		"unknown sslmode": {
			with(func(c *pgx.Config) { c.SSLMode = "bogus"; c.Password = secret.New("Sup3rS3cret") }), "sslmode is invalid",
		},
		"port out of range":          {with(func(c *pgx.Config) { c.Port = -1 }), "parse dsn"},
		"negative statement timeout": {with(func(c *pgx.Config) { c.StatementTimeout = -time.Second }), "DB_STATEMENT_TIMEOUT"},
		"negative operation timeout": {with(func(c *pgx.Config) { c.OpTimeout = -time.Second }), "DB_OP_TIMEOUT"},
		"negative pool size":         {with(func(c *pgx.Config) { c.MaxConns = -1 }), "DB_MAX_CONNS"},
		"more warm connections than the pool holds": {
			with(func(c *pgx.Config) { c.MinConns, c.MaxConns = 50, 2 }), "DB_MIN_CONNS 50 is above DB_MAX_CONNS 2",
		},
		"password file missing": {
			with(func(c *pgx.Config) { c.PasswordFile = filepath.Join(dir, "nope") }), "DB_PASSWORD_FILE: open " + filepath.Join(dir, "nope"),
		},
		"password file empty": {
			with(func(c *pgx.Config) { c.PasswordFile = filepath.Join(dir, "empty") }), "DB_PASSWORD_FILE: " + filepath.Join(dir, "empty") + " is empty",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			require.ErrorContains(t, err, tc.want)
			assert.NotContains(t, err.Error(), "Sup3rS3cret")

			_, newErr := pgx.New(t.Context(), tc.cfg, nil)
			require.EqualError(t, newErr, "pgx: "+err.Error())
		})
	}
}

// DATABASE_URL carries the password, so no way of printing the config shows it.
func TestConfig_PrintingDoesNotShowTheURL(t *testing.T) {
	t.Parallel()

	cfg := pgx.Config{URL: secret.New("postgres://app:Sup3rS3cret@db:5432/app")}

	var logged strings.Builder
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("config", "postgres", cfg)

	for form, printed := range map[string]string{
		"%v": fmt.Sprintf("%v", cfg), "%+v": fmt.Sprintf("%+v", cfg), "%#v": fmt.Sprintf("%#v", cfg), "slog": logged.String(),
	} {
		assert.NotContains(t, printed, "Sup3rS3cret", form)
	}
}

func TestNew_UnreachableDatabaseFailsFast(t *testing.T) {
	t.Parallel()

	start := time.Now()
	_, err := pgx.New(context.Background(), pgx.Config{
		URL: secret.New("postgres://app:app@127.0.0.1:1/app?sslmode=disable"), ConnectTimeout: time.Second,
	}, nil)
	require.ErrorContains(t, err, "pgx: initial ping")
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.NotErrorIs(t, err, context.Canceled)
}
