// Package pgx is the shared PostgreSQL access layer for golang-basics
// services — the database analogue of libs/httpx. It wraps a pgx connection
// pool ([github.com/jackc/pgx/v5/pgxpool]) with the cross-cutting concerns a
// service repeats:
//
//   - configuration from the environment (per-service prefix) or YAML, meant
//     to be embedded in the service's own config (see [Config]);
//   - server-side guard rails on every connection: statement_timeout,
//     lock_timeout and idle_in_transaction_session_timeout (see [New]);
//   - a readiness check that plugs straight into libs/httpx ([DB.ReadyCheck]);
//   - tracing (a client span per query via otelpgx) and metrics: pool
//     statistics plus query latency and errors by operation ([DB.Collectors],
//     [WithOperation]);
//   - a versioned migration runner over NNN_name.sql files, safe for
//     replicas booting together ([Migrate]).
//
// It deliberately stops at "give me a healthy *pgxpool.Pool"; query building,
// repositories and the domain model live in each service (see services/tasks
// for a worked example). pgx is chosen over database/sql + lib/pq because it
// is the actively-maintained, batteries-included PostgreSQL driver for Go and
// exposes pgxpool for connection pooling without CGO.
//
// A service typically does:
//
//	//go:embed *.sql
//	var migrations embed.FS
//
//	db, err := pgx.New(ctx, cfg.Postgres, logger)
//	if err != nil { … }
//	defer db.Close()
//	if err := pgx.Migrate(ctx, db.Pool(), migrations, logger); err != nil { … }
//	srv.Health.Register("postgres", db.ReadyCheck())
//	srv.Metrics.Registry.MustRegister(db.Collectors()...)
package pgx
