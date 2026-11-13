package pgx

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// rollbackTimeout bounds the rollback of a unit whose context is already
// cancelled: the rollback runs detached from it, and must not hang on a
// connection that is gone.
const rollbackTimeout = 5 * time.Second

// retryBackoff is the upper bound of the random pause before the first retry
// of a unit (see [WithRetry]); it grows linearly with the attempt.
const retryBackoff = 10 * time.Millisecond

// SQLSTATE codes after which the same transaction may succeed when run again.
const (
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
)

// ErrTxOptions is returned by a nested [DB.InTx] that asks for a guarantee
// the transaction it would join cannot give: a stricter isolation level than
// the outer unit runs at, or [WithRetry] (only the outermost unit can be run
// again). Failing is the point: the alternative is a use case that believes
// it is SERIALIZABLE and is not.
var ErrTxOptions = errors.New("pgx: nested unit of work asks for more than the outer transaction gives")

// Querier is what a repository runs its statements on: the part of the pgx
// API a pool and a transaction share. [DB.Conn] returns the right one for a
// context, so the same repository method works inside a unit of work and
// outside of one.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, rows pgx.CopyFromSource) (int64, error)
}

// TxOption configures a unit of work started by [DB.InTx].
type TxOption func(*txOptions)

type txOptions struct {
	tx        pgx.TxOptions
	attempts  int
	savepoint bool
}

// WithIsolation sets the isolation level of the transaction (the server
// default, READ COMMITTED, otherwise).
func WithIsolation(level pgx.TxIsoLevel) TxOption {
	return func(o *txOptions) { o.tx.IsoLevel = level }
}

// ReadOnly makes the transaction READ ONLY: a write in it is an error.
func ReadOnly() TxOption {
	return func(o *txOptions) { o.tx.AccessMode = pgx.ReadOnly }
}

// WithRetry runs the unit up to attempts times in total when it fails with a
// serialization failure or a deadlock (SQLSTATE 40001, 40P01) — the two
// errors PostgreSQL returns for a transaction that is correct and merely lost
// a race. Each attempt is a new transaction and a new call of the function,
// so everything the function does outside the database must be safe to do
// again; [DB.AfterCommit] hooks of a failed attempt are dropped.
func WithRetry(attempts int) TxOption {
	return func(o *txOptions) { o.attempts = attempts }
}

// WithSavepoint makes an [DB.InTx] that is called inside another unit run in
// a savepoint: when its function fails, only its own statements (and
// [DB.AfterCommit] hooks) are rolled back, and the outer unit may carry on.
// Without it a nested InTx simply joins the outer transaction. It has no
// effect on an outermost InTx.
func WithSavepoint() TxOption {
	return func(o *txOptions) { o.savepoint = true }
}

// isolationRank orders the levels by what they guarantee; the empty level is
// the server default, READ COMMITTED unless the database was told otherwise.
func isolationRank(level pgx.TxIsoLevel) int {
	const (
		readUncommitted = iota
		readCommitted
		repeatableRead
		serializable
	)

	switch level {
	case pgx.ReadUncommitted:
		return readUncommitted
	case pgx.RepeatableRead:
		return repeatableRead
	case pgx.Serializable:
		return serializable
	case pgx.ReadCommitted:
		return readCommitted
	default:
		return readCommitted
	}
}

// unit is one transaction (or savepoint) in flight and what has to happen
// once it is durable.
type unit struct {
	tx        pgx.Tx
	isolation pgx.TxIsoLevel // of the outermost transaction

	mu    sync.Mutex
	hooks []func(context.Context)
}

func (u *unit) add(hooks ...func(context.Context)) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.hooks = append(u.hooks, hooks...)
}

func (u *unit) take() []func(context.Context) {
	u.mu.Lock()
	defer u.mu.Unlock()

	hooks := u.hooks
	u.hooks = nil

	return hooks
}

// unitKey is per DB: a unit of one database is invisible to another.
type unitKey struct{ db *DB }

func (db *DB) unit(ctx context.Context) *unit {
	u, _ := ctx.Value(unitKey{db: db}).(*unit)

	return u
}

// Conn returns what to run a statement on: the transaction of the unit of
// work ctx belongs to (see [DB.InTx]), or the pool when there is none.
// Repositories call it for every statement instead of holding a pool or
// taking a pgx.Tx parameter:
//
//	func (r *Tasks) Insert(ctx context.Context, t Task) error {
//		_, err := r.db.Conn(ctx).Exec(ctx, `INSERT INTO tasks …`, t.ID, t.Title)
//		return err
//	}
func (db *DB) Conn(ctx context.Context) Querier {
	if u := db.unit(ctx); u != nil {
		return u.tx
	}

	return db.pool
}

// InTx runs work as one unit of work: everything it does through [DB.Conn]
// with the context it is given happens in a single transaction, committed
// when work returns nil and rolled back when it returns an error or panics.
//
//	err := db.InTx(ctx, func(ctx context.Context) error {
//		if err := tasks.Insert(ctx, task); err != nil {
//			return err
//		}
//
//		return outbox.Enqueue(ctx, event) // same transaction
//	})
//
// The error of work is returned as it is. Called inside another unit of the
// same DB, InTx joins it: there is one transaction, the outermost InTx
// commits it. The inner call cannot change the transaction, so one that asks
// for more than it has — a stricter [WithIsolation], or [WithRetry] — fails
// with [ErrTxOptions] before its function runs; [ReadOnly] on an inner call
// is not enforced. An error the inner function returns rolls everything back only if the
// outer function returns it too — which it must, unless the inner call used
// [WithSavepoint]: a failed statement leaves a PostgreSQL transaction
// aborted, and its commit fails with [pgx.ErrTxCommitRollback].
//
// The context of a unit belongs to one goroutine at a time: a transaction is
// one connection, and pgx does not allow concurrent use of it.
//
// When Commit itself fails because the connection was lost, the outcome is
// unknown — the server may have committed. Hooks do not run in that case.
func (db *DB) InTx(ctx context.Context, work func(ctx context.Context) error, opts ...TxOption) error {
	var applied txOptions
	for _, opt := range opts {
		opt(&applied)
	}

	if parent := db.unit(ctx); parent != nil {
		switch {
		case applied.attempts > 1:
			return fmt.Errorf("%w: WithRetry inside a unit", ErrTxOptions)
		case isolationRank(applied.tx.IsoLevel) > isolationRank(parent.isolation):
			return fmt.Errorf("%w: isolation %q inside %q", ErrTxOptions, applied.tx.IsoLevel, parent.isolation)
		case !applied.savepoint:
			return work(ctx)
		}

		hooks, err := db.run(ctx, parent.isolation, parent.tx.Begin, work)
		parent.add(hooks...) // none on error

		return err
	}

	begin := func(ctx context.Context) (pgx.Tx, error) { return db.pool.BeginTx(ctx, applied.tx) }

	for attempt := 1; ; attempt++ {
		hooks, err := db.run(ctx, applied.tx.IsoLevel, begin, work)
		if err == nil {
			for _, hook := range hooks {
				hook(ctx)
			}

			return nil
		}

		if attempt >= applied.attempts || !retryable(err) || !pause(ctx, attempt) {
			return err
		}
	}
}

// AfterCommit registers fn to run once the unit of work ctx belongs to has
// committed — the place for what must not happen for a transaction that
// rolls back and cannot be rolled back itself: invalidating a cache, waking
// a relay, publishing a notification. Hooks run in registration order, on
// the goroutine that called [DB.InTx], with the context InTx was called
// with: the transaction is over, [DB.Conn] is the pool again. A hook cannot
// fail the unit — it has committed — so it handles its own errors; a hook
// that panics takes the hooks after it with it.
//
// Outside a unit of work there is nothing to wait for and fn runs at once,
// so a repository can call AfterCommit without knowing how it is used.
//
// A hook is not durable: a process that dies between the commit and the hook
// never runs it. What must happen goes into the transaction (an outbox row).
func (db *DB) AfterCommit(ctx context.Context, fn func(ctx context.Context)) {
	if u := db.unit(ctx); u != nil {
		u.add(fn)

		return
	}

	fn(ctx)
}

// run executes work in the transaction begin opens and returns the hooks it
// registered, for the caller to run (outermost) or pass up (savepoint).
func (db *DB) run(
	ctx context.Context,
	isolation pgx.TxIsoLevel,
	begin func(context.Context) (pgx.Tx, error),
	work func(context.Context) error,
) ([]func(context.Context), error) {
	tx, err := begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgx: begin: %w", err)
	}

	defer func() { // a no-op after a commit; also what a panic in work goes through
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()

		// ErrTxClosed is the no-op after a commit. Anything else is a
		// connection that could not be told to roll back: the server ends
		// the transaction when it notices, but it is worth a line.
		if err := tx.Rollback(rctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			db.log.WarnContext(ctx, "pgx: rollback failed", "error", err)
		}
	}()

	current := &unit{tx: tx, isolation: isolation}

	if err := work(context.WithValue(ctx, unitKey{db: db}, current)); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("pgx: commit: %w", err)
	}

	return current.take(), nil
}

// retryable reports whether err is PostgreSQL telling a correct transaction
// to try again.
func retryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	return pgErr.Code == codeSerializationFailure || pgErr.Code == codeDeadlockDetected
}

// pause waits a random time before retry number attempt, so that the units
// that collided do not collide again in step; false when ctx ended first.
func pause(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(rand.N(time.Duration(attempt) * retryBackoff)) //nolint:gosec // jitter, not a secret
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
