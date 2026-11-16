package pgx

import (
	"context"
	"time"

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

// bounded is a [Querier] that gives every call a deadline of its own. pgx
// reads a result from the connection after the call that asked for it has
// returned, under the context of that call — so the deadline is released by
// the result, not by the call: when Rows are closed or exhausted, a Row is
// scanned, BatchResults are closed. A result left unread holds its timer
// until the deadline.
type bounded struct {
	q       Querier
	timeout time.Duration
}

func (b bounded) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	return b.q.Exec(ctx, sql, args...)
}

func (b bounded) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)

	rows, err := b.q.Query(ctx, sql, args...)
	if err != nil {
		cancel()

		return rows, err
	}

	return &boundedRows{Rows: rows, cancel: cancel}, nil
}

func (b bounded) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)

	return boundedRow{row: b.q.QueryRow(ctx, sql, args...), cancel: cancel}
}

func (b bounded) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)

	return &boundedBatch{BatchResults: b.q.SendBatch(ctx, batch), cancel: cancel}
}

func (b bounded) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, rows pgx.CopyFromSource) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	return b.q.CopyFrom(ctx, table, columns, rows)
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
