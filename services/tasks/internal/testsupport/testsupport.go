// Package testsupport gives the tasks tests an isolated, migrated database on
// the package's shared Postgres container: a fresh schema per test, selected
// through search_path, so tests never share the public tables (or each
// other's outbox) and can run in parallel. Test-only — nothing in the service
// imports it — and deliberately free of testify / testx / testcontainers (the
// callers pass the container's DSN), so it stays within the depguard rule
// that keeps those in _test.go files.
package testsupport

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/services/tasks/migrations"
)

var seq atomic.Uint64

// SchemaDSN creates a fresh schema on the Postgres at direct (the shared
// container's DSN) and returns via's DSN — direct itself when via is "", or
// a proxy in front of it — with search_path set to that schema. The schema is
// dropped when the test ends.
func SchemaDSN(t testing.TB, direct, via string) string {
	t.Helper()
	if via == "" {
		via = direct
	}
	schema := fmt.Sprintf("t_%d_%d", os.Getpid(), seq.Add(1))
	exec(t, direct, `CREATE SCHEMA `+schema)
	t.Cleanup(func() { exec(t, direct, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`) })

	u, err := url.Parse(via)
	if err != nil {
		t.Fatalf("testsupport: parse dsn: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func exec(t testing.TB, dsn, sql string) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.New(ctx, pgx.Config{URL: dsn, MaxConns: 1}, nil)
	if err != nil {
		t.Fatalf("testsupport: connect: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Pool().Exec(ctx, sql); err != nil {
		t.Fatalf("testsupport: %s: %v", sql, err)
	}
}

// DB returns a pool on a fresh, migrated schema of the Postgres at direct,
// closed when the test ends.
func DB(t testing.TB, direct string) *pgx.DB {
	t.Helper()
	return MigratedDB(t, SchemaDSN(t, direct, ""))
}

// MigratedDB opens dsn and applies the service's migrations.
func MigratedDB(t testing.TB, dsn string) *pgx.DB {
	t.Helper()
	ctx := context.Background()
	db, err := pgx.New(ctx, pgx.Config{URL: dsn, ConnectTimeout: 5 * time.Second, MaxConns: 8}, nil)
	if err != nil {
		t.Fatalf("testsupport: connect: %v", err)
	}
	t.Cleanup(db.Close)
	if err := pgx.Migrate(ctx, db.Pool(), migrations.FS, nil); err != nil {
		t.Fatalf("testsupport: migrate: %v", err)
	}
	return db
}
