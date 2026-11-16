package pgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// bound returns ctx under the operation timeout (DB_OP_TIMEOUT), or ctx
// itself when there is none.
func (db *DB) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if db.opTimeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, db.opTimeout)
}

// conn is the [Querier] of [DB.Conn]. It gives every call the operation
// deadline, when there is one, and counts the calls that fail before they
// reach a connection (see [attempt]).
//
// pgx reads a result from the connection after the call that asked for it
// has returned, under the context of that call — so the deadline is released
// by the result, not by the call: when Rows are closed or exhausted, a Row is
// scanned, BatchResults are closed. A result left unread holds its timer
// until the deadline.
type conn struct {
	q  Querier
	db *DB
}

func (c conn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctx, cancel := c.db.bound(ctx)
	defer cancel()

	ctx, try := watch(ctx)
	defer c.db.metrics.missed(try, queryOperation(ctx, sql))

	return c.q.Exec(ctx, sql, args...)
}

func (c conn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx, cancel := c.db.bound(ctx)
	ctx, try := watch(ctx)

	rows, err := c.q.Query(ctx, sql, args...)
	c.db.metrics.missed(try, queryOperation(ctx, sql))

	if err != nil {
		cancel()

		return rows, err
	}

	return &boundedRows{Rows: rows, cancel: cancel}, nil
}

func (c conn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx, cancel := c.db.bound(ctx)
	ctx, try := watch(ctx)

	row := c.q.QueryRow(ctx, sql, args...)
	c.db.metrics.missed(try, queryOperation(ctx, sql))

	return boundedRow{row: row, cancel: cancel}
}

func (c conn) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	ctx, cancel := c.db.bound(ctx)
	ctx, try := watch(ctx)

	results := c.q.SendBatch(ctx, batch)
	// pgx does not trace a batch with nothing in it: that one is not a miss.
	if batch.Len() > 0 {
		c.db.metrics.missed(try, operation(ctx, "batch"))
	}

	return &boundedBatch{BatchResults: results, cancel: cancel}
}

func (c conn) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, rows pgx.CopyFromSource) (int64, error) {
	ctx, cancel := c.db.bound(ctx)
	defer cancel()

	ctx, try := watch(ctx)
	defer c.db.metrics.missed(try, operation(ctx, "copy"))

	return c.q.CopyFrom(ctx, table, columns, rows)
}

type boundedRows struct {
	pgx.Rows

	cancel context.CancelFunc
}

func (r *boundedRows) Close() {
	r.Rows.Close()
	r.cancel()
}

// Next releases the deadline at the end of the rows: pgx closes them there
// itself, and a caller that ranges to the end need not call Close.
func (r *boundedRows) Next() bool {
	if r.Rows.Next() {
		return true
	}

	r.cancel()

	return false
}

type boundedRow struct {
	row    pgx.Row
	cancel context.CancelFunc
}

func (r boundedRow) Scan(dest ...any) error {
	defer r.cancel()

	return r.row.Scan(dest...)
}

type boundedBatch struct {
	pgx.BatchResults

	cancel context.CancelFunc
}

func (b *boundedBatch) Close() error {
	defer b.cancel()

	return b.BatchResults.Close()
}
