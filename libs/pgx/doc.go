// Package pgx is the shared PostgreSQL access layer for golang-basics
// services — the database analogue of libs/httpx. It wraps a pgx connection
// pool ([github.com/jackc/pgx/v5/pgxpool]) with the cross-cutting concerns a
// service repeats: environment-driven configuration with a per-service prefix,
// a readiness check that plugs straight into libs/httpx, and a tiny migration
// helper so a service can bring its schema up on boot.
//
// It deliberately stops at "give me a healthy *pgxpool.Pool"; query building,
// repositories and the domain model live in each service (see services/tasks
// for a worked example). pgx is chosen over database/sql + lib/pq because it
// is the actively-maintained, batteries-included PostgreSQL driver for Go and
// exposes pgxpool for connection pooling without CGO.
//
// A service typically does:
//
//	cfg, _ := pgx.LoadConfig("TASKS_")
//	db, _ := pgx.New(ctx, cfg, logger)
//	defer db.Close()
//	srv.Health.Register("postgres", db.ReadyCheck())
package pgx
