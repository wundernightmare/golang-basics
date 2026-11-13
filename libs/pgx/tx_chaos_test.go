package pgx_test

import (
	"context"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// chaosItems is two views of one fresh schema in the shared Postgres: the
// repository under test, connected through Toxiproxy, and a witness on a
// direct connection that tells what the server really holds.
func chaosItems(t *testing.T) (repo, witness items, proxy *containers.Proxy, logs *testx.LogBuffer) {
	t.Helper()

	cfg, _ := schemaConfig(t)
	witness = items{db: newDB(t, cfg)}
	_, err := witness.db.Pool().Exec(context.Background(), `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	proxy = containers.Proxied(t, "postgres")

	through, err := url.Parse(cfg.URL)
	require.NoError(t, err)

	through.Host = proxy.Addr
	cfg.URL = through.String()
	cfg.MaxConns = 2

	logs = &testx.LogBuffer{}
	db, err := pgx.New(context.Background(), cfg, slog.New(slog.NewJSONHandler(logs, nil)))
	require.NoError(t, err)
	t.Cleanup(db.Close)

	return items{db: db}, witness, proxy, logs
}

// The connection dies in the middle of a unit: the unit fails promptly,
// nothing of it is committed, no hook runs, it is not retried (a lost
// connection is not a serialization failure), and the same pool serves the
// next unit once the database is back.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy in front of Postgres
func TestChaos_InTx_ConnectionLostMidUnit(t *testing.T) {
	repo, witness, proxy, logs := chaosItems(t)
	ctx := context.Background()

	calls, hookRan := 0, false
	start := time.Now()

	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		calls++

		repo.db.AfterCommit(ctx, func(context.Context) { hookRan = true })
		require.NoError(t, repo.insert(ctx, 1, "written before the cut"))

		proxy.Down() // every connection through the proxy is closed

		return repo.insert(ctx, 2, "never arrives")
	}, pgx.WithRetry(3))
	require.Error(t, err)

	var pgErr *pgconn.PgError

	assert.NotErrorAs(t, err, &pgErr, "a transport error, not an answer from the server")
	assert.Equal(t, 1, calls, "not retried")
	assert.False(t, hookRan)
	assert.Less(t, time.Since(start), 3*time.Second, "failed on the broken connection, did not wait for anything")
	assert.NotNil(t, logs.Find(t, map[string]any{"msg": "pgx: rollback failed", "level": "WARN"}))

	assert.Eventually(t, func() bool { return len(witness.committed(t)) == 0 }, 5*time.Second, 50*time.Millisecond,
		"the server rolled the orphaned transaction back")

	// While the database is unreachable a unit cannot even begin.
	bctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = repo.db.InTx(bctx, func(context.Context) error {
		t.Error("the function must not run without a transaction")

		return nil
	})

	cancel()
	require.ErrorContains(t, err, "pgx: begin")

	proxy.Up()

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 3, "after the outage") }))
	assert.Equal(t, []int{3}, witness.committed(t))
	assert.Zero(t, repo.db.Pool().Stat().AcquiredConns())
}

// The reply to COMMIT is lost: the server got the commit and applied it, the
// client never heard. InTx must report an error within the caller's deadline
// and must not run the hooks — and the test pins the uncomfortable part, that
// the data is there anyway. This is why a unit's effects have to be
// idempotent for whoever retries the whole operation.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy in front of Postgres
func TestChaos_InTx_CommitReplyLost(t *testing.T) {
	repo, witness, proxy, _ := chaosItems(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hookRan := false
	start := time.Now()

	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		repo.db.AfterCommit(ctx, func(context.Context) { hookRan = true })
		require.NoError(t, repo.insert(ctx, 1, "committed, unacknowledged"))

		// From here on the server's replies are dropped; what the client
		// sends still arrives.
		proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0})

		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "pgx: commit")
	assert.False(t, hookRan, "an unknown outcome is not a commit")
	assert.Less(t, time.Since(start), 4*time.Second, "bounded by the caller's deadline, and the rollback did not hang")

	assert.Eventually(t, func() bool { return len(witness.committed(t)) == 1 }, 5*time.Second, 50*time.Millisecond,
		"the server did commit: the error means unknown, not rolled back")

	proxy.Reset()

	require.NoError(t, repo.db.InTx(context.Background(), func(ctx context.Context) error {
		return repo.insert(ctx, 2, "the pool dropped the dead connection")
	}))
	assert.Equal(t, []int{1, 2}, witness.committed(t))
}

// A slow network and a caller with a deadline: the unit gives up in time and
// leaves nothing behind, at whichever statement the deadline hits.
//
//nolint:paralleltest // serial: chaos via the shared Toxiproxy proxy in front of Postgres
func TestChaos_InTx_DeadlineOnASlowLink(t *testing.T) {
	repo, witness, proxy, _ := chaosItems(t)

	proxy.Latency(400 * time.Millisecond) // per reply: BEGIN + INSERT + INSERT cannot fit in a second

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hookRan := false
	start := time.Now()

	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		repo.db.AfterCommit(ctx, func(context.Context) { hookRan = true })

		if err := repo.insert(ctx, 1, "slow"); err != nil {
			return err
		}

		return repo.insert(ctx, 2, "slower")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, hookRan)
	assert.Less(t, time.Since(start), 4*time.Second)

	proxy.Reset()

	assert.Eventually(t, func() bool { return len(witness.committed(t)) == 0 }, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, repo.db.InTx(context.Background(), func(ctx context.Context) error { return repo.insert(ctx, 3, "link restored") }))
	assert.Equal(t, []int{3}, witness.committed(t))
}
