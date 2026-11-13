package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

var errBusiness = errors.New("business rule says no")

// items is a repository the way a service writes one: it holds the DB, runs
// every statement on db.Conn(ctx) and knows nothing about transactions.
type items struct{ db *pgx.DB }

func (r items) insert(ctx context.Context, id int, name string) error {
	if _, err := r.db.Conn(ctx).Exec(ctx, `INSERT INTO items (id, name) VALUES ($1, $2)`, id, name); err != nil {
		return fmt.Errorf("insert item %d: %w", id, err)
	}

	return nil
}

func (r items) ids(ctx context.Context) ([]int, error) {
	rows, err := r.db.Conn(ctx).Query(ctx, `SELECT id FROM items ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("select items: %w", err)
	}

	return pgxv5.CollectRows(rows, pgxv5.RowTo[int])
}

// committed reads through the pool: what any other connection sees.
func (r items) committed(t testing.TB) []int {
	t.Helper()

	ids, err := r.ids(context.Background())
	require.NoError(t, err)

	return ids
}

func newItems(t testing.TB) items {
	t.Helper()

	cfg, _ := schemaConfig(t)
	db := newDB(t, cfg)
	_, err := db.Pool().Exec(context.Background(), `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	return items{db: db}
}

func TestInTx_CommitsWhatTheRepositoriesDid(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 1, "a"))
		require.NoError(t, repo.insert(ctx, 2, "b"))

		inside, err := repo.ids(ctx)
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2}, inside, "the unit reads its own writes")
		assert.Empty(t, repo.committed(t), "nobody else sees them before the commit")

		return nil
	}))

	assert.Equal(t, []int{1, 2}, repo.committed(t))
	require.NoError(t, repo.insert(ctx, 3, "c"), "outside a unit Conn is the pool")
	assert.Equal(t, []int{1, 2, 3}, repo.committed(t))
}

func TestInTx_RollsBackOnErrorPanicAndCancel(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 1, "a"))

		return errBusiness
	})
	require.ErrorIs(t, err, errBusiness)
	assert.Equal(t, errBusiness, err, "the error of fn comes back as it is")

	assert.PanicsWithValue(t, "boom", func() {
		_ = repo.db.InTx(ctx, func(ctx context.Context) error {
			require.NoError(t, repo.insert(ctx, 2, "b"))
			panic("boom")
		})
	})

	cctx, cancel := context.WithCancel(ctx)
	err = repo.db.InTx(cctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 3, "c"))
		cancel()

		return nil // the commit runs on a cancelled context and must not go through
	})
	require.ErrorIs(t, err, context.Canceled)

	assert.Empty(t, repo.committed(t))
	assert.Zero(t, repo.db.Pool().Stat().AcquiredConns(), "every connection went back to the pool")
}

func TestInTx_NestedJoinsTheOuterTransaction(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	// A use case that is itself a unit, composed into a larger one.
	addPair := func(ctx context.Context, first int) error {
		return repo.db.InTx(ctx, func(ctx context.Context) error {
			if err := repo.insert(ctx, first, "x"); err != nil {
				return err
			}

			return repo.insert(ctx, first+1, "y")
		})
	}

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, addPair(ctx, 1))
		assert.Empty(t, repo.committed(t), "the inner unit did not commit on its own")

		return addPair(ctx, 3)
	}))
	assert.Equal(t, []int{1, 2, 3, 4}, repo.committed(t))

	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, addPair(ctx, 10))

		return addPair(ctx, 3) // duplicate key: fails the whole unit
	})

	var pgErr *pgconn.PgError

	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "23505", pgErr.Code)
	assert.Equal(t, []int{1, 2, 3, 4}, repo.committed(t), "10 and 11 went with it")

	// Swallowing the failure of a joined unit does not save the transaction.
	err = repo.db.InTx(ctx, func(ctx context.Context) error {
		_ = addPair(ctx, 3)

		return nil
	})
	require.ErrorIs(t, err, pgxv5.ErrTxCommitRollback)
}

func TestInTx_SavepointContainsAFailure(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	var ran []string

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 1, "kept"))

		err := repo.db.InTx(ctx, func(ctx context.Context) error {
			repo.db.AfterCommit(ctx, func(context.Context) { ran = append(ran, "failed savepoint") })
			require.NoError(t, repo.insert(ctx, 2, "undone"))

			return repo.insert(ctx, 1, "duplicate")
		}, pgx.WithSavepoint())
		require.Error(t, err)

		require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
			repo.db.AfterCommit(ctx, func(context.Context) { ran = append(ran, "released savepoint") })

			return repo.insert(ctx, 3, "kept too")
		}, pgx.WithSavepoint()))

		return nil // the outer unit carries on and commits
	}))

	assert.Equal(t, []int{1, 3}, repo.committed(t))
	assert.Equal(t, []string{"released savepoint"}, ran, "hooks of a rolled-back savepoint are dropped")
}

func TestAfterCommit(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	var ran []string

	hook := func(name string) func(context.Context) {
		return func(ctx context.Context) {
			// The transaction is over: this read goes through the pool and
			// sees what was committed, and a new unit can be started.
			ids, err := repo.ids(ctx)
			assert.NoError(t, err)
			assert.NoError(t, repo.db.InTx(ctx, func(context.Context) error { return nil }))

			ran = append(ran, fmt.Sprintf("%s sees %v", name, ids))
		}
	}

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
		repo.db.AfterCommit(ctx, hook("first"))
		require.NoError(t, repo.insert(ctx, 1, "a"))

		require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
			repo.db.AfterCommit(ctx, hook("joined"))

			return nil
		}))

		assert.Empty(t, ran, "nothing runs before the commit")

		return nil
	}))
	assert.Equal(t, []string{"first sees [1]", "joined sees [1]"}, ran, "in registration order, after the commit")

	ran = nil
	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		repo.db.AfterCommit(ctx, hook("rolled back"))

		return errBusiness
	})
	require.ErrorIs(t, err, errBusiness)
	assert.Empty(t, ran, "a unit that rolls back runs no hooks")

	repo.db.AfterCommit(ctx, hook("no unit"))
	assert.Equal(t, []string{"no unit sees [1]"}, ran, "outside a unit there is nothing to wait for")
}

func TestInTx_Options(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	var level, readOnly string

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
		if err := repo.db.Conn(ctx).QueryRow(ctx, `SHOW transaction_isolation`).Scan(&level); err != nil {
			return err
		}

		return repo.db.Conn(ctx).QueryRow(ctx, `SHOW transaction_read_only`).Scan(&readOnly)
	}, pgx.WithIsolation(pgxv5.RepeatableRead), pgx.ReadOnly()))
	assert.Equal(t, "repeatable read", level)
	assert.Equal(t, "on", readOnly)

	err := repo.db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 1, "a") }, pgx.ReadOnly())

	var pgErr *pgconn.PgError

	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "25006", pgErr.Code, "read_only_sql_transaction")
}

// Two databases (two pools) never share a unit, even through one context.
func TestInTx_UnitBelongsToItsDB(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	first, second := newDB(t, cfg), newDB(t, cfg)
	ctx := context.Background()

	_, err := first.Pool().Exec(ctx, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	require.NoError(t, first.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, items{db: first}.insert(ctx, 1, "a"))

		seen, err := items{db: second}.ids(ctx)
		require.NoError(t, err)
		assert.Empty(t, seen, "the other DB runs on its own pool, outside this transaction")

		ran := false

		second.AfterCommit(ctx, func(context.Context) { ran = true })
		assert.True(t, ran, "and has no unit to defer a hook to")

		return nil
	}))
}

// A real serialization conflict: two SERIALIZABLE units read the table and
// then write to it. PostgreSQL lets one commit and fails the other with
// 40001; WithRetry runs it again, and both end up applied.
func TestInTx_RetriesASerializationFailure(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	var (
		bothRead sync.WaitGroup
		workers  sync.WaitGroup
		attempts atomic.Int32
		hooks    atomic.Int32
		errs     = make([]error, 2)
	)

	bothRead.Add(2)

	for worker := range 2 {
		workers.Go(func() {
			first := true

			errs[worker] = repo.db.InTx(ctx, func(ctx context.Context) error {
				attempts.Add(1)
				repo.db.AfterCommit(ctx, func(context.Context) { hooks.Add(1) })

				seen, err := repo.ids(ctx)
				if err != nil {
					return err
				}

				if first { // overlap the first attempts: both read before either writes
					first = false

					bothRead.Done()
					bothRead.Wait()
				}

				return repo.insert(ctx, 100*(worker+1)+len(seen), "from a count")
			}, pgx.WithIsolation(pgxv5.Serializable), pgx.WithRetry(5))
		})
	}

	workers.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Len(t, repo.committed(t), 2)
	assert.Equal(t, int32(3), attempts.Load(), "one of the two ran twice")
	assert.Equal(t, int32(2), hooks.Load(), "the hook of the failed attempt was dropped")
}

func TestInTx_RetryIsBoundedAndOnlyForRetryableErrors(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()
	serialization := &pgconn.PgError{Code: "40001", Message: "could not serialize access"}

	calls := 0
	err := repo.db.InTx(ctx, func(context.Context) error {
		calls++

		return fmt.Errorf("wrapped: %w", serialization)
	}, pgx.WithRetry(3))
	require.ErrorIs(t, err, serialization)
	assert.Equal(t, 3, calls, "attempts is the total")

	calls = 0
	err = repo.db.InTx(ctx, func(context.Context) error {
		calls++

		return &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
	})
	require.Error(t, err)
	assert.Equal(t, 1, calls, "no retry unless asked for")

	calls = 0
	err = repo.db.InTx(ctx, func(context.Context) error {
		calls++

		return errBusiness
	}, pgx.WithRetry(3))
	require.ErrorIs(t, err, errBusiness)
	assert.Equal(t, 1, calls, "a business error is not retried")

	cctx, cancel := context.WithCancel(ctx)
	calls = 0
	err = repo.db.InTx(cctx, func(context.Context) error {
		calls++

		cancel()

		return serialization
	}, pgx.WithRetry(3))
	require.ErrorIs(t, err, serialization)
	assert.Equal(t, 1, calls, "a cancelled caller is not retried for")
}

func TestInTx_BeginFailureIsReported(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	db := newDB(t, cfg)
	db.Close()

	called := false
	err := db.InTx(context.Background(), func(context.Context) error {
		called = true

		return nil
	})
	require.ErrorContains(t, err, "pgx: begin")
	assert.False(t, called)
}

func TestConn_BatchAndCopyInsideAUnit(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	err := repo.db.InTx(ctx, func(ctx context.Context) error {
		batch := &pgxv5.Batch{}
		batch.Queue(`INSERT INTO items (id, name) VALUES (1, 'a')`)
		batch.Queue(`INSERT INTO items (id, name) VALUES (2, 'b')`)

		if err := repo.db.Conn(ctx).SendBatch(ctx, batch).Close(); err != nil {
			return err
		}

		copied, err := repo.db.Conn(ctx).CopyFrom(ctx, pgxv5.Identifier{"items"}, []string{"id", "name"},
			pgxv5.CopyFromRows([][]any{{3, "c"}, {4, "d"}}))
		assert.Equal(t, int64(2), copied)

		if err != nil {
			return err
		}

		return errBusiness
	})
	require.ErrorIs(t, err, errBusiness)
	assert.Empty(t, repo.committed(t), "batch and copy were part of the unit")
}

// A nested unit cannot change the transaction it joins, so asking for more
// than it has is an error — not a weaker guarantee handed over silently.
func TestInTx_NestedCannotAskForMoreThanTheOuterGives(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()
	inner := func(called *bool) func(context.Context) error {
		return func(context.Context) error {
			*called = true

			return nil
		}
	}

	for name, opts := range map[string][]pgx.TxOption{
		"stricter isolation":             {pgx.WithIsolation(pgxv5.Serializable)},
		"stricter isolation, savepoint":  {pgx.WithIsolation(pgxv5.RepeatableRead), pgx.WithSavepoint()},
		"retry":                          {pgx.WithRetry(3)},
		"retry at the outer's isolation": {pgx.WithRetry(2), pgx.WithIsolation(pgxv5.ReadCommitted)},
	} {
		called := false
		err := repo.db.InTx(ctx, func(ctx context.Context) error {
			return repo.db.InTx(ctx, inner(&called), opts...)
		})
		require.ErrorIs(t, err, pgx.ErrTxOptions, name)
		assert.False(t, called, "%s: refused before the function runs", name)
	}

	// What the outer already gives, or less, is fine.
	for name, opts := range map[string][]pgx.TxOption{
		"same level":        {pgx.WithIsolation(pgxv5.Serializable)},
		"weaker level":      {pgx.WithIsolation(pgxv5.ReadCommitted)},
		"weakest level":     {pgx.WithIsolation(pgxv5.ReadUncommitted)},
		"one attempt":       {pgx.WithRetry(1)},
		"read only (joins)": {pgx.ReadOnly()},
	} {
		called := false

		require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
			return repo.db.InTx(ctx, inner(&called), opts...)
		}, pgx.WithIsolation(pgxv5.Serializable)), name)
		assert.True(t, called, name)
	}
}

// With no retry, a serialization conflict between two units that both wrote
// before either committed surfaces on COMMIT itself — the statement an
// application is least prepared to see fail.
func TestInTx_SerializationFailureAtCommit(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()

	var (
		bothWrote sync.WaitGroup
		workers   sync.WaitGroup
		hooks     atomic.Int32
		errs      = make([]error, 2)
	)

	bothWrote.Add(2)

	for worker := range 2 {
		workers.Go(func() {
			errs[worker] = repo.db.InTx(ctx, func(ctx context.Context) error {
				repo.db.AfterCommit(ctx, func(context.Context) { hooks.Add(1) })

				if _, err := repo.ids(ctx); err != nil {
					return err
				}

				err := repo.insert(ctx, worker+1, "write skew")

				bothWrote.Done()
				bothWrote.Wait()

				return err
			}, pgx.WithIsolation(pgxv5.Serializable))
		})
	}

	workers.Wait()

	failed := errs[0]
	if failed == nil {
		failed = errs[1]
	}

	var pgErr *pgconn.PgError

	require.ErrorAs(t, failed, &pgErr, "exactly one of the two loses: %v / %v", errs[0], errs[1])
	assert.Equal(t, "40001", pgErr.Code)
	require.ErrorContains(t, failed, "pgx: commit")
	assert.True(t, errs[0] == nil || errs[1] == nil, "the other one committed")
	assert.Len(t, repo.committed(t), 1)
	assert.Equal(t, int32(1), hooks.Load())
}

// A real deadlock: two units lock the same two rows in opposite order.
// PostgreSQL kills one after deadlock_timeout (40P01); WithRetry runs it
// again once the survivor is through.
func TestInTx_RetriesADeadlock(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()
	require.NoError(t, repo.insert(ctx, 1, "a"))
	require.NoError(t, repo.insert(ctx, 2, "b"))

	lock := func(ctx context.Context, id int) error {
		_, err := repo.db.Conn(ctx).Exec(ctx, `UPDATE items SET name = name || '!' WHERE id = $1`, id)

		return err
	}

	var (
		bothHoldOne sync.WaitGroup
		workers     sync.WaitGroup
		attempts    atomic.Int32
		errs        = make([]error, 2)
	)

	bothHoldOne.Add(2)

	for worker := range 2 {
		workers.Go(func() {
			first := true

			errs[worker] = repo.db.InTx(ctx, func(ctx context.Context) error {
				attempts.Add(1)

				if err := lock(ctx, 1+worker); err != nil {
					return err
				}

				if first {
					first = false

					bothHoldOne.Done()
					bothHoldOne.Wait()
				}

				return lock(ctx, 2-worker)
			}, pgx.WithRetry(3))
		})
	}

	workers.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Equal(t, int32(3), attempts.Load(), "the deadlock victim ran twice")

	var names []string

	rows, err := repo.db.Pool().Query(ctx, `SELECT name FROM items ORDER BY id`)
	require.NoError(t, err)
	names, err = pgxv5.CollectRows(rows, pgxv5.RowTo[string])
	require.NoError(t, err)
	assert.Equal(t, []string{"a!!", "b!!"}, names, "both units applied both updates, once each")
}

// The whole unit — joined and savepoint nesting included — runs on the one
// connection it took, and gives it back before the hooks run.
func TestInTx_NeedsOneConnection(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	cfg.MaxConns = 1
	repo := items{db: newDB(t, cfg)}

	// A second acquire would wait for the deadline instead of hanging the test.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := repo.db.Pool().Exec(ctx, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	hookRan := false

	require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 1, "outer"))
		require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 2, "joined") }))
		require.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 3, "savepoint") }, pgx.WithSavepoint()))

		repo.db.AfterCommit(ctx, func(ctx context.Context) {
			hookRan = true

			assert.Zero(t, repo.db.Pool().Stat().AcquiredConns(), "the unit's connection is back in the pool")
			assert.NoError(t, repo.db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 4, "from the hook") }))
		})

		return nil
	}))
	assert.True(t, hookRan)
	assert.Equal(t, []int{1, 2, 3, 4}, repo.committed(t))
}

// The server-side guard rails end a unit that overstays; the unit reports it,
// commits nothing, and the pool is usable right after.
func TestInTx_ServerTimeoutsEndTheUnit(t *testing.T) {
	t.Parallel()

	cfg, _ := schemaConfig(t)
	cfg.StatementTimeout = 300 * time.Millisecond
	cfg.IdleInTransactionSessionTimeout = 300 * time.Millisecond
	logs := &testx.LogBuffer{}
	db, err := pgx.New(context.Background(), cfg, slog.New(slog.NewJSONHandler(logs, nil)))
	require.NoError(t, err)
	t.Cleanup(db.Close)

	repo := items{db: db}
	ctx := context.Background()

	_, err = db.Pool().Exec(ctx, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	var pgErr *pgconn.PgError

	err = db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 1, "before the slow statement"))

		_, err := db.Conn(ctx).Exec(ctx, `SELECT pg_sleep(3)`)

		return err
	})
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "57014", pgErr.Code, "query_canceled by statement_timeout")

	err = db.InTx(ctx, func(ctx context.Context) error {
		require.NoError(t, repo.insert(ctx, 2, "before going idle"))
		time.Sleep(time.Second) // holding the transaction open without talking to the server

		return repo.insert(ctx, 3, "too late")
	})
	require.Error(t, err, "the server terminated the idle session")

	assert.Empty(t, repo.committed(t))
	assert.NotNil(t, logs.Find(t, map[string]any{"msg": "pgx: rollback failed", "level": "WARN"}),
		"the rollback of the terminated session could not be sent, and says so")
	require.NoError(t, db.InTx(ctx, func(ctx context.Context) error { return repo.insert(ctx, 4, "the pool recovered") }))
	assert.Equal(t, []int{4}, repo.committed(t))
}

func TestAfterCommit_PanicInAHookPropagates(t *testing.T) {
	t.Parallel()

	repo := newItems(t)
	ctx := context.Background()
	laterRan := false

	assert.PanicsWithValue(t, "hook", func() {
		_ = repo.db.InTx(ctx, func(ctx context.Context) error {
			repo.db.AfterCommit(ctx, func(context.Context) { panic("hook") })
			repo.db.AfterCommit(ctx, func(context.Context) { laterRan = true })

			return repo.insert(ctx, 1, "a")
		})
	})
	assert.False(t, laterRan, "the hooks after it do not run")
	assert.Equal(t, []int{1}, repo.committed(t), "the unit had committed: a hook cannot undo it")
}
