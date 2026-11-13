package pgx_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

const bouncerPort = "6432/tcp"

// guardedRole is a login role whose session timeouts are set on the role
// itself (ALTER ROLE … SET): the way to have them behind a pooler.
const guardedRole = "guarded"

// bouncerConfig is one PgBouncer in front of the shared Postgres.
type bouncerConfig struct {
	name string
	// ignoreStartup is ignore_startup_parameters beyond what pgx always sends.
	ignoreStartup []string
	// maxPrepared is max_prepared_statements (0: PgBouncer does not track
	// protocol-level prepared statements).
	maxPrepared int
	// trackExtra is track_extra_parameters beyond PgBouncer's default: startup
	// parameters it remembers per client and sets on whatever server
	// connection serves that client next. Only parameters the server reports
	// back can be tracked; PostgreSQL 18 reports search_path.
	trackExtra []string
}

// Two PgBouncers, started on first use.
var (
	// stockBouncer is PgBouncer as its defaults leave it.
	stockBouncer = bouncerConfig{name: "stock"}
	// tunedBouncer is what the tests below arrive at as the safe setup, plus
	// the one unsafe convenience (ignoring the timeouts) they expose.
	tunedBouncer = bouncerConfig{
		name:          "tuned",
		maxPrepared:   200,
		trackExtra:    []string{"search_path"},
		ignoreStartup: []string{"statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout"},
	}
)

// bouncerINI renders pgbouncer.ini: one database alias per pooling mode and
// pool size the tests need, all pointing at the shared Postgres.
func bouncerINI(cfg bouncerConfig, upstreamPort string) string {
	upstream := "host=host.docker.internal port=" + upstreamPort + " dbname=app"

	return fmt.Sprintf(`[databases]
tx = %[1]s pool_mode=transaction pool_size=4
sess = %[1]s pool_mode=session pool_size=4
tx_pair = %[1]s pool_mode=transaction pool_size=2
sess_pair = %[1]s pool_mode=session pool_size=2
tx_one = %[1]s pool_mode=transaction pool_size=1
sess_one = %[1]s pool_mode=session pool_size=1

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
unix_socket_dir =
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt
admin_users = app
max_client_conn = 200
default_pool_size = 4
query_wait_timeout = 2
server_reset_query = DISCARD ALL
ignore_startup_parameters = %[2]s
max_prepared_statements = %[3]d
track_extra_parameters = %[4]s
`, upstream, strings.Join(append([]string{"extra_float_digits"}, cfg.ignoreStartup...), ","), cfg.maxPrepared,
		strings.Join(append([]string{"IntervalStyle"}, cfg.trackExtra...), ","))
}

// bouncer starts (once per test binary) the PgBouncer described by cfg and
// returns its host:port.
func bouncer(tb testing.TB, cfg bouncerConfig) string {
	tb.Helper()

	direct, err := url.Parse(containers.Postgres(tb))
	require.NoError(tb, err)

	return containers.Fixture(tb, "pgbouncer-"+cfg.name, func(ctx context.Context) (string, testcontainers.Container, error) {
		file := func(path, content string) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader([]byte(content)), ContainerFilePath: path, FileMode: 0o644}
		}

		hub := os.Getenv("DOCKER_HUB")
		if hub == "" {
			hub = "docker.io"
		}

		ctr, err := testcontainers.Run(ctx, "",
			testcontainers.WithDockerfile(testcontainers.FromDockerfile{
				Context:    "testdata/pgbouncer",
				Dockerfile: "Dockerfile",
				Repo:       "golang-basics-test-pgbouncer",
				Tag:        "local",
				BuildArgs:  map[string]*string{"DOCKER_HUB": &hub},
				KeepImage:  true,
			}),
			testcontainers.WithExposedPorts(bouncerPort),
			testcontainers.WithFiles(
				file("/etc/pgbouncer/pgbouncer.ini", bouncerINI(cfg, direct.Port())),
				file("/etc/pgbouncer/userlist.txt", `"app" "app"`+"\n"+`"`+guardedRole+`" "`+guardedRole+`"`+"\n"),
			),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			}),
			testcontainers.WithWaitStrategy(wait.ForListeningPort(bouncerPort).WithStartupTimeout(60*time.Second)))
		if err != nil {
			return "", ctr, err
		}

		return "localhost:" + containers.HostPort(tb, ctr, bouncerPort), ctr, nil
	})
}

// viaBouncer opens a pool through the given PgBouncer and database alias. No
// session timeouts unless mut sets them: a zero sends no startup parameter.
func viaBouncer(tb testing.TB, cfg bouncerConfig, alias string, mut func(*pgx.Config)) *pgx.DB {
	tb.Helper()

	db, err := openViaBouncer(tb, cfg, alias, mut)
	require.NoError(tb, err)
	tb.Cleanup(db.Close)

	return db
}

func openViaBouncer(tb testing.TB, cfg bouncerConfig, alias string, mut func(*pgx.Config)) (*pgx.DB, error) {
	tb.Helper()

	conf := pgx.Config{
		URL:            "postgres://app:app@" + bouncer(tb, cfg) + "/" + alias + "?sslmode=disable",
		MaxConns:       1,
		ConnectTimeout: 5 * time.Second,
	}
	if mut != nil {
		mut(&conf)
	}

	return pgx.New(context.Background(), conf, nil)
}

// directDB is a pool on the shared Postgres itself: the witness of what the
// server holds, and the hand that prepares it.
func directDB(tb testing.TB) *pgx.DB {
	tb.Helper()

	return newDB(tb, pgx.Config{URL: containers.Postgres(tb), MaxConns: 2, ConnectTimeout: 5 * time.Second})
}

// freshSchema creates a schema of the test's own and returns its name.
func freshSchema(tb testing.TB) string {
	tb.Helper()

	name := strings.ReplaceAll(testx.Unique("b"), "-", "_")
	_, err := directDB(tb).Pool().Exec(context.Background(), `CREATE SCHEMA `+name)
	require.NoError(tb, err)

	return name
}

// backend returns the PID of the server process a statement on q is served
// by — which server connection PgBouncer handed out. Simple protocol, so the
// question itself prepares nothing.
func backend(tb testing.TB, ctx context.Context, q pgx.Querier) int {
	tb.Helper()

	var pid int
	require.NoError(tb, q.QueryRow(ctx, `SELECT pg_backend_pid()`, pgxv5.QueryExecModeSimpleProtocol).Scan(&pid))

	return pid
}

// occupy opens transactions through holders until one of them sits on the
// server connection with the given PID, and keeps that one open until the
// returned func (or the end of the test): whoever comes next through the same
// PgBouncer pool is served by another server connection. The transaction is
// returned for the tests that need to act on that very backend.
func occupy(t *testing.T, holders *pgx.DB, pid int) (pgxv5.Tx, func()) {
	t.Helper()

	ctx := context.Background()

	var spare []pgxv5.Tx

	for range 8 {
		tx, err := holders.Pool().Begin(ctx)
		require.NoError(t, err)

		if backend(t, ctx, tx) != pid {
			spare = append(spare, tx)

			continue
		}

		for _, other := range spare {
			require.NoError(t, other.Rollback(ctx))
		}

		var once sync.Once

		release := func() { once.Do(func() { _ = tx.Rollback(ctx) }) }
		t.Cleanup(release)

		return tx, release
	}

	t.Fatalf("no transaction landed on backend %d", pid)

	return nil, nil
}

func sqlState(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}

	return ""
}

// The session timeouts travel as startup parameters, and PgBouncer does not
// pass startup parameters it does not know. Three outcomes, in both pooling
// modes:
//
//   - stock PgBouncer refuses the connection: loud, and safe;
//   - PgBouncer told to ignore them (ignore_startup_parameters — the fix most
//     search results suggest) accepts the connection and drops the timeouts.
//     Without a check that is a pool with no guard rails; pgx.New reads them
//     back and refuses to start;
//   - the timeouts set on the role, and none in the config: in effect on
//     every server connection, whatever the pooler does.
func TestBouncer_SessionTimeouts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, err := directDB(t).Pool().Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '`+guardedRole+`') THEN
			CREATE ROLE `+guardedRole+` LOGIN PASSWORD '`+guardedRole+`';
		END IF;
	END $$;
	ALTER ROLE `+guardedRole+` SET statement_timeout = '700ms';
	ALTER ROLE `+guardedRole+` SET lock_timeout = '300ms';
	ALTER ROLE `+guardedRole+` SET idle_in_transaction_session_timeout = '5s'`)
	require.NoError(t, err)

	withTimeouts := func(c *pgx.Config) { c.StatementTimeout = 700 * time.Millisecond }
	asGuarded := func(c *pgx.Config) {
		c.URL = strings.Replace(c.URL, "app:app@", guardedRole+":"+guardedRole+"@", 1)
	}

	for _, alias := range []string{"tx", "sess"} {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()

			_, err := openViaBouncer(t, stockBouncer, alias, withTimeouts)
			require.ErrorContains(t, err, "unsupported startup parameter: statement_timeout", "stock PgBouncer refuses the parameter")

			_, err = openViaBouncer(t, tunedBouncer, alias, withTimeouts)
			require.ErrorIs(t, err, pgx.ErrGuardRailOff, "accepted and dropped: New notices")
			require.ErrorContains(t, err, "statement_timeout")

			// Through the same PgBouncers, with the timeouts on the role.
			for _, cfg := range []bouncerConfig{stockBouncer, tunedBouncer} {
				db := viaBouncer(t, cfg, alias, asGuarded)

				var statement, lock string
				require.NoError(t, db.Pool().QueryRow(ctx, `SELECT current_setting('statement_timeout'), current_setting('lock_timeout')`).Scan(&statement, &lock))
				assert.Equal(t, "700ms", statement, cfg.name)
				assert.Equal(t, "300ms", lock, cfg.name)

				start := time.Now()
				_, err := db.Pool().Exec(ctx, `SELECT pg_sleep(5)`)
				assert.Equal(t, "57014", sqlState(err), "%s: the server cancels the runaway statement", cfg.name)
				assert.Less(t, time.Since(start), 3*time.Second)
			}

			// A config that agrees with the role passes the check where the
			// pooler ignores the parameter; one that asks for another value is
			// started with the server's.
			db := viaBouncer(t, tunedBouncer, alias, func(c *pgx.Config) {
				asGuarded(c)
				c.StatementTimeout = 30 * time.Second
			})

			var effective string
			require.NoError(t, db.Pool().QueryRow(ctx, `SHOW statement_timeout`).Scan(&effective))
			assert.Equal(t, "700ms", effective, "the role's value stands")
		})
	}
}

// pgx caches a prepared statement per connection and executes it by name. In
// transaction pooling the next execution may be served by another server
// connection, which never saw the PREPARE.
//
//nolint:paralleltest // serial: decides which server connection of a two-connection pool serves whom
func TestBouncer_PreparedStatementsInTransactionMode(t *testing.T) {
	ctx := context.Background()

	const query = `SELECT $1::int + 1`

	// answer runs the cached statement and reports the backend that served it.
	answer := func(db *pgx.DB, n int) (int, error) {
		var out int

		err := db.Pool().QueryRow(ctx, query, n).Scan(&out)

		return out, err
	}

	// onAnotherBackend warms the statement on one server connection, then
	// has that connection occupied and runs the statement again.
	onAnotherBackend := func(t *testing.T, cfg bouncerConfig, mut func(*pgx.Config)) error {
		t.Helper()

		client := viaBouncer(t, cfg, "tx_pair", mut)
		holders := viaBouncer(t, cfg, "tx_pair", func(c *pgx.Config) { c.MaxConns = 2 })

		got, err := answer(client, 1)
		require.NoError(t, err)
		require.Equal(t, 2, got)

		first := backend(t, ctx, client.Pool())
		_, release := occupy(t, holders, first)

		defer release()

		require.NotEqual(t, first, backend(t, ctx, client.Pool()), "the client is on the other server connection now")

		got, err = answer(client, 41)
		if err == nil {
			require.Equal(t, 42, got)
		}

		return err
	}

	t.Run("stock PgBouncer, pgx defaults: the statement is gone", func(t *testing.T) {
		err := onAnotherBackend(t, stockBouncer, nil)
		require.Error(t, err)
		assert.Equal(t, "26000", sqlState(err), "invalid_sql_statement_name: %v", err)
	})

	t.Run("stock PgBouncer, default_query_exec_mode=exec: nothing to lose", func(t *testing.T) {
		require.NoError(t, onAnotherBackend(t, stockBouncer, func(c *pgx.Config) { c.URL += "&default_query_exec_mode=exec" }))
	})

	t.Run("stock PgBouncer, simple protocol: nothing to lose", func(t *testing.T) {
		require.NoError(t, onAnotherBackend(t, stockBouncer, func(c *pgx.Config) { c.URL += "&default_query_exec_mode=simple_protocol" }))
	})

	t.Run("max_prepared_statements > 0, pgx defaults: PgBouncer re-prepares", func(t *testing.T) {
		require.NoError(t, onAnotherBackend(t, tunedBouncer, nil))
	})
}

// Session pooling gives a client its own server connection for as long as it
// is connected: prepared statements and session state are as without a
// pooler, and the connection is wiped before the next client gets it.
//
//nolint:paralleltest // serial: uses the one-connection session pool
func TestBouncer_SessionModeKeepsAndThenResetsSessionState(t *testing.T) {
	ctx := context.Background()

	first := viaBouncer(t, stockBouncer, "sess_one", nil)

	pid := backend(t, ctx, first.Pool())
	_, err := first.Pool().Exec(ctx, `SET lock_timeout = '1234ms'`)
	require.NoError(t, err)

	for range 3 {
		var n int
		require.NoError(t, first.Pool().QueryRow(ctx, `SELECT $1::int`, 7).Scan(&n), "the cached statement stays valid: same server connection")
		assert.Equal(t, pid, backend(t, ctx, first.Pool()))
	}

	var seen string
	require.NoError(t, first.Pool().QueryRow(ctx, `SHOW lock_timeout`).Scan(&seen))
	assert.Equal(t, "1234ms", seen, "a session SET lasts for the client's connection")

	first.Close()

	second := viaBouncer(t, stockBouncer, "sess_one", nil)
	assert.Equal(t, pid, backend(t, ctx, second.Pool()), "the next client gets the same server connection")
	require.NoError(t, second.Pool().QueryRow(ctx, `SHOW lock_timeout`).Scan(&seen))
	assert.Equal(t, "0", seen, "server_reset_query wiped what the previous client set")
}

// In transaction pooling a session-level SET outlives the statement on the
// server connection and belongs to nobody: the next client served by that
// connection inherits it. SET LOCAL inside a unit of work is the scoped form
// — it ends with the transaction.
//
//nolint:paralleltest // serial: uses the one-connection transaction pool
func TestBouncer_SessionSetLeaksInTransactionMode(t *testing.T) {
	ctx := context.Background()

	careless := viaBouncer(t, tunedBouncer, "tx_one", nil)
	bystander := viaBouncer(t, tunedBouncer, "tx_one", nil)

	t.Cleanup(func() { _, _ = bystander.Pool().Exec(ctx, `RESET lock_timeout`) })

	show := func(q pgx.Querier) string {
		var v string
		require.NoError(t, q.QueryRow(ctx, `SHOW lock_timeout`).Scan(&v))

		return v
	}

	require.Equal(t, "0", show(bystander.Pool()))

	// Scoped: visible inside the unit, gone with it.
	require.NoError(t, careless.InTx(ctx, func(ctx context.Context) error {
		if _, err := careless.Conn(ctx).Exec(ctx, `SET LOCAL lock_timeout = '250ms'`); err != nil {
			return err
		}

		assert.Equal(t, "250ms", show(careless.Conn(ctx)))

		return nil
	}))
	assert.Equal(t, "0", show(bystander.Pool()), "SET LOCAL ended with the transaction")

	// Unscoped: it stays on the server connection, for whoever is next.
	_, err := careless.Pool().Exec(ctx, `SET lock_timeout = '4321ms'`)
	require.NoError(t, err)
	assert.Equal(t, "4321ms", show(bystander.Pool()), "another client inherited the setting")
}

// A session advisory lock is taken on one server connection; in transaction
// pooling the unlock is served by whichever connection is free — here,
// another one. It reports false, and the lock stays held by a connection no
// client owns. The transaction-scoped lock cannot leak: it is released with
// the transaction, on the connection that took it — which is what Migrate
// uses.
//
//nolint:paralleltest // serial: decides which server connection of a two-connection pool serves whom
func TestBouncer_AdvisoryLocksInTransactionMode(t *testing.T) {
	ctx := context.Background()
	witness := directDB(t)
	key := time.Now().UnixNano()

	held := func() int {
		var n int
		require.NoError(t, witness.Pool().QueryRow(ctx,
			`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND ((classid::bigint << 32) | objid::bigint) = $1`, key).Scan(&n))

		return n
	}

	client := viaBouncer(t, tunedBouncer, "tx_pair", nil)
	holders := viaBouncer(t, tunedBouncer, "tx_pair", func(c *pgx.Config) { c.MaxConns = 2 })

	_, err := client.Pool().Exec(ctx, `SELECT pg_advisory_lock($1)`, key)
	require.NoError(t, err)
	require.Equal(t, 1, held())

	locker := backend(t, ctx, client.Pool())
	onLocker, release := occupy(t, holders, locker)

	var unlocked bool
	require.NoError(t, client.Pool().QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&unlocked))
	assert.False(t, unlocked, "the unlock went to a connection that does not hold the lock")
	assert.Equal(t, 1, held(), "and the lock is still held, by nobody in particular")

	// Clean up from the connection that holds it.
	_, err = onLocker.Exec(ctx, `SELECT pg_advisory_unlock_all()`)
	require.NoError(t, err)
	release()
	require.Zero(t, held())

	// Transaction-scoped: gone when the unit ends, whatever connection is next.
	require.NoError(t, client.InTx(ctx, func(ctx context.Context) error {
		_, err := client.Conn(ctx).Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key)

		assert.Equal(t, 1, held())

		return err
	}))
	assert.Zero(t, held())
}

// Migrate through transaction pooling, three runners at once: the versioned
// files are applied exactly once. The schema comes from search_path in the
// DSN, which works because PgBouncer tracks it (track_extra_parameters) —
// stock PgBouncer refuses it like any other unknown startup parameter.
func TestBouncer_MigrateThroughTransactionPooling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schema := freshSchema(t)
	inSchema := func(c *pgx.Config) {
		c.URL += "&search_path=" + schema
		c.MaxConns = 3
	}

	_, err := openViaBouncer(t, stockBouncer, "tx", inSchema)
	require.ErrorContains(t, err, "unsupported startup parameter: search_path")

	db := viaBouncer(t, tunedBouncer, "tx", inSchema)

	var runners sync.WaitGroup

	errs := make([]error, 3)

	for i := range errs {
		runners.Go(func() { errs[i] = pgx.Migrate(ctx, db.Pool(), widgets, nil) })
	}

	runners.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, []int64{1, 2}, appliedVersions(t, db))

	// Whichever server connection answers, the tracked search_path is on it.
	for range 8 {
		var count int
		require.NoError(t, db.Pool().QueryRow(ctx, `SELECT count(*) FROM widgets`).Scan(&count))
		assert.Equal(t, 2, count)
	}

	var where string
	require.NoError(t, directDB(t).Pool().QueryRow(ctx,
		`SELECT table_schema FROM information_schema.tables WHERE table_name = 'widgets' AND table_schema = $1`, schema).Scan(&where))
	assert.Equal(t, schema, where, "the tables are in the test's schema, not in public")
}

// A unit of work is one transaction, and PgBouncer keeps a transaction on one
// server connection in either mode: the unit, its savepoints, its hooks and
// its retries behave as on a direct connection.
func TestBouncer_UnitOfWork(t *testing.T) {
	t.Parallel()

	for _, alias := range []string{"tx", "sess"} {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			schema := freshSchema(t)
			db := viaBouncer(t, tunedBouncer, alias, func(c *pgx.Config) {
				c.URL += "&search_path=" + schema
				c.MaxConns = 3
			})
			repo := items{db: db}

			_, err := db.Pool().Exec(ctx, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
			require.NoError(t, err)

			var hooks []string

			require.NoError(t, db.InTx(ctx, func(ctx context.Context) error {
				pid := backend(t, ctx, db.Conn(ctx))

				require.NoError(t, repo.insert(ctx, 1, "outer"))
				assert.Equal(t, pid, backend(t, ctx, db.Conn(ctx)), "every statement of the unit is on one server connection")

				err := db.InTx(ctx, func(ctx context.Context) error {
					db.AfterCommit(ctx, func(context.Context) { hooks = append(hooks, "dropped") })
					require.NoError(t, repo.insert(ctx, 2, "undone"))

					return repo.insert(ctx, 1, "duplicate")
				}, pgx.WithSavepoint())
				require.Error(t, err)

				db.AfterCommit(ctx, func(context.Context) { hooks = append(hooks, "kept") })
				assert.Equal(t, pid, backend(t, ctx, db.Conn(ctx)))

				return repo.insert(ctx, 3, "after the savepoint")
			}))
			assert.Equal(t, []int{1, 3}, repo.committed(t))
			assert.Equal(t, []string{"kept"}, hooks)

			require.ErrorIs(t, db.InTx(ctx, func(ctx context.Context) error {
				require.NoError(t, repo.insert(ctx, 4, "rolled back"))

				return errBusiness
			}), errBusiness)
			assert.Equal(t, []int{1, 3}, repo.committed(t))

			// Batches and COPY inside a unit, through the pooler.
			require.NoError(t, db.InTx(ctx, func(ctx context.Context) error {
				batch := &pgxv5.Batch{}
				batch.Queue(`INSERT INTO items (id, name) VALUES ($1, $2)`, 10, "batched")
				batch.Queue(`INSERT INTO items (id, name) VALUES ($1, $2)`, 11, "batched")

				if err := db.Conn(ctx).SendBatch(ctx, batch).Close(); err != nil {
					return err
				}

				_, err := db.Conn(ctx).CopyFrom(ctx, pgxv5.Identifier{"items"}, []string{"id", "name"},
					pgxv5.CopyFromRows([][]any{{20, "copied"}, {21, "copied"}}))

				return err
			}))
			assert.Equal(t, []int{1, 3, 10, 11, 20, 21}, repo.committed(t))
		})
	}
}

// PgBouncer's pool is the real limit, not pgxpool's: with one server
// connection and a unit of work holding it, the next query waits in PgBouncer.
// A caller with a deadline leaves on time; one without is refused after
// query_wait_timeout — it never waits forever.
//
//nolint:paralleltest // serial: uses the one-connection transaction pool
func TestBouncer_ServerPoolExhausted(t *testing.T) {
	ctx := context.Background()

	holder := viaBouncer(t, tunedBouncer, "tx_one", nil)
	waiter := viaBouncer(t, tunedBouncer, "tx_one", nil)

	release := make(chan struct{})
	holding := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- holder.InTx(ctx, func(ctx context.Context) error {
			if _, err := holder.Conn(ctx).Exec(ctx, `SELECT 1`); err != nil {
				return err
			}

			close(holding)
			<-release

			return nil
		})
	}()

	<-holding

	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := waiter.Pool().Exec(short, `SELECT 1`)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	start = time.Now()
	_, err = waiter.Pool().Exec(ctx, `SELECT 1`)
	require.ErrorContains(t, err, "query_wait_timeout")
	// query_wait_timeout is two seconds here; pgx may spend it once per
	// address "localhost" resolves to.
	assert.GreaterOrEqual(t, time.Since(start), 1500*time.Millisecond, "it did wait for a server connection")
	assert.Less(t, time.Since(start), 10*time.Second, "and was refused, not left waiting")

	close(release)
	require.NoError(t, <-done)

	_, err = waiter.Pool().Exec(ctx, `SELECT 1`)
	require.NoError(t, err, "the connection is free again")
}

// Cancelling a query through PgBouncer cancels it on the server: the cancel
// request is forwarded to the right backend, which stops working — a context
// deadline is not just the client walking away from a query that runs on.
func TestBouncer_CancellationReachesTheServer(t *testing.T) {
	t.Parallel()

	for _, alias := range []string{"tx", "sess"} {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()

			db := viaBouncer(t, tunedBouncer, alias, nil)
			witness := directDB(t)
			marker := testx.Unique("cancel")

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			start := time.Now()
			_, err := db.Pool().Exec(ctx, `SELECT pg_sleep(30) /* `+marker+` */`)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Less(t, time.Since(start), 3*time.Second)

			assert.Eventually(t, func() bool {
				var running int

				err := witness.Pool().QueryRow(context.Background(),
					`SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%' || $1 || '%' AND pid <> pg_backend_pid()`,
					marker).Scan(&running)

				return err == nil && running == 0
			}, 5*time.Second, 50*time.Millisecond, "the thirty-second sleep was stopped on the server")

			_, err = db.Pool().Exec(context.Background(), `SELECT 1`)
			require.NoError(t, err, "and the pool works")
		})
	}
}
