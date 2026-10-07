package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/testx/contract"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/testsupport"
)

func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

type outboxRow struct {
	EventID, AggregateID, Type string
	Payload                    []byte
	Headers                    map[string]string
}

func outboxRows(t testing.TB, db *pgx.DB) []outboxRow {
	t.Helper()
	rows, err := db.Pool().Query(context.Background(),
		`SELECT event_id::text, aggregate_id, event_type, payload, headers FROM outbox ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		var headers []byte
		require.NoError(t, rows.Scan(&r.EventID, &r.AggregateID, &r.Type, &r.Payload, &headers))
		require.NoError(t, json.Unmarshal(headers, &r.Headers))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

func newID(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }

func TestCreate_TaskAndEventCommitTogether(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	testx.Recorder(t) // a real propagator + provider, so the trace context is captured
	ctx, span := otel.Tracer("test").Start(context.Background(), "POST /tasks")
	defer span.End()

	before := time.Now()
	task, replayed, err := st.Create(ctx, newID(1), "ship it", nil)
	require.NoError(t, err)
	assert.False(t, replayed)
	assert.Equal(t, domain.Task{ID: newID(1), Title: "ship it", Version: 1, CreatedAt: task.CreatedAt}, task)
	assert.WithinDuration(t, before, task.CreatedAt, 5*time.Second, "created_at comes from the database clock")

	got, err := st.Get(context.Background(), task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.True(t, task.CreatedAt.Equal(got.CreatedAt))

	events := outboxRows(t, db)
	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, outbox.TypeTaskCreated, e.Type)
	assert.Equal(t, task.ID, e.AggregateID)
	contract.LoadJSONSchema(t, "jsonschema/TaskCreatedEvent.json").Validate(t, e.Payload)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(e.Payload, &payload))
	assert.Equal(t, e.EventID, payload["event_id"], "the payload names its event id")
	assert.Equal(t, float64(1), payload["version"])
	assert.Contains(t, e.Headers["traceparent"], span.SpanContext().TraceID().String(), "the request's trace is stored with the event")
}

func TestCreate_FailedTransactionLeavesNoEvent(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	ctx := context.Background()
	_, _, err := st.Create(ctx, newID(1), "first", nil)
	require.NoError(t, err)

	_, _, err = st.Create(ctx, newID(1), "duplicate id", nil)
	require.Error(t, err)
	assert.Len(t, outboxRows(t, db), 1, "the failed create wrote no event")
}

func TestCreate_IdempotencyKey(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	ctx := context.Background()
	key := &domain.IdempotencyKey{Key: "k", Hash: "h1"}

	first, replayed, err := st.Create(ctx, newID(1), "once", key)
	require.NoError(t, err)
	assert.False(t, replayed)

	again, replayed, err := st.Create(ctx, newID(2), "once", key)
	require.NoError(t, err)
	assert.True(t, replayed)
	assert.Equal(t, first, again, "the first request's task")

	_, _, err = st.Create(ctx, newID(3), "other", &domain.IdempotencyKey{Key: "k", Hash: "h2"})
	require.ErrorIs(t, err, domain.ErrIdempotencyKeyReused)

	assert.Len(t, outboxRows(t, db), 1, "replays and rejections write no event")
	_, err = st.Get(ctx, newID(2))
	require.ErrorIs(t, err, domain.ErrNotFound, "the replayed attempt's row was rolled back")

	// Deleting the task releases the key.
	require.NoError(t, st.Delete(ctx, first.ID, nil))
	fresh, replayed, err := st.Create(ctx, newID(4), "once", key)
	require.NoError(t, err)
	assert.False(t, replayed)
	assert.Equal(t, newID(4), fresh.ID)
}

// Concurrent requests with one key: exactly one task and one event; every
// caller gets that task.
func TestCreate_IdempotencyKeyUnderConcurrency(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	key := &domain.IdempotencyKey{Key: "race", Hash: "h"}

	const n = 8
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			task, _, err := st.Create(context.Background(), newID(100+i), "raced", key)
			ids[i], errs[i] = task.ID, err
		})
	}
	wg.Wait()
	for i := range n {
		require.NoError(t, errs[i])
		assert.Equal(t, ids[0], ids[i])
	}
	page, err := st.List(context.Background(), domain.PageRequest{Limit: 50})
	require.NoError(t, err)
	assert.Len(t, page.Tasks, 1)
	assert.Len(t, outboxRows(t, db), 1)
}

func TestUpdate_VersionsAndEvents(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	ctx := context.Background()
	task, _, err := st.Create(ctx, newID(1), "draft", nil)
	require.NoError(t, err)

	title, done := "final", true
	updated, err := st.Update(ctx, task.ID, domain.Patch{Title: &title}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), updated.Version)
	assert.Equal(t, "final", updated.Title)
	assert.False(t, updated.Done)
	assert.True(t, task.CreatedAt.Equal(updated.CreatedAt), "created_at is not touched")

	v2 := int64(2)
	updated, err = st.Update(ctx, task.ID, domain.Patch{Done: &done}, &v2)
	require.NoError(t, err)
	assert.Equal(t, int64(3), updated.Version)
	assert.True(t, updated.Done)
	assert.Equal(t, "final", updated.Title)

	_, err = st.Update(ctx, task.ID, domain.Patch{Done: &done}, &v2)
	require.ErrorIs(t, err, domain.ErrVersionMismatch, "version 2 is gone")
	_, err = st.Update(ctx, newID(9), domain.Patch{Done: &done}, nil)
	require.ErrorIs(t, err, domain.ErrNotFound)
	_, err = st.Update(ctx, newID(9), domain.Patch{Done: &done}, &v2)
	require.ErrorIs(t, err, domain.ErrNotFound, "unknown beats mismatch")

	events := outboxRows(t, db)
	require.Len(t, events, 3, "created + two updates; failed updates write nothing")
	schema := contract.LoadJSONSchema(t, "jsonschema/TaskUpdatedEvent.json")
	for i, e := range events[1:] {
		assert.Equal(t, outbox.TypeTaskUpdated, e.Type)
		schema.Validate(t, e.Payload)
		var p map[string]any
		require.NoError(t, json.Unmarshal(e.Payload, &p))
		assert.Equal(t, float64(i+2), p["version"])
		assert.Equal(t, e.EventID, p["event_id"])
	}
}

func TestDelete_VersionsAndEvent(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	ctx := context.Background()
	task, _, err := st.Create(ctx, newID(1), "doomed", nil)
	require.NoError(t, err)

	stale := int64(5)
	require.ErrorIs(t, st.Delete(ctx, task.ID, &stale), domain.ErrVersionMismatch)
	current := int64(1)
	require.NoError(t, st.Delete(ctx, task.ID, &current))
	require.ErrorIs(t, st.Delete(ctx, task.ID, nil), domain.ErrNotFound)
	_, err = st.Get(ctx, task.ID)
	require.ErrorIs(t, err, domain.ErrNotFound)

	events := outboxRows(t, db)
	require.Len(t, events, 2)
	assert.Equal(t, outbox.TypeTaskDeleted, events[1].Type)
	contract.LoadJSONSchema(t, "jsonschema/TaskDeletedEvent.json").Validate(t, events[1].Payload)
}

// Keyset pages walk (created_at DESC, id DESC) exactly once each, including
// ties on created_at, and the query is served by the matching index.
func TestList_KeysetPagination(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	ctx := context.Background()

	// Seven tasks, three sharing one timestamp (the id breaks the tie).
	_, err := db.Pool().Exec(ctx, `
		INSERT INTO tasks (id, title, created_at) VALUES
		  ('a', 'a', '2026-01-01T00:00:01Z'), ('b', 'b', '2026-01-01T00:00:02Z'),
		  ('c', 'c', '2026-01-01T00:00:03Z'), ('d', 'd', '2026-01-01T00:00:03Z'),
		  ('e', 'e', '2026-01-01T00:00:03Z'), ('f', 'f', '2026-01-01T00:00:04Z'),
		  ('g', 'g', '2026-01-01T00:00:05.123456Z')`)
	require.NoError(t, err)

	var seen []string
	req := domain.PageRequest{Limit: 2}
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10)
		page, err := st.List(ctx, req)
		require.NoError(t, err)
		for _, task := range page.Tasks {
			seen = append(seen, task.ID)
		}
		if page.Next == nil {
			break
		}
		// Through the wire form, as a client would.
		c, err := domain.DecodeCursor(page.Next.Encode())
		require.NoError(t, err)
		req.Cursor = &c
	}
	assert.Equal(t, []string{"g", "f", "e", "d", "c", "b", "a"}, seen)

	page, err := st.List(ctx, domain.PageRequest{Limit: 7})
	require.NoError(t, err)
	assert.Len(t, page.Tasks, 7)
	assert.Nil(t, page.Next, "an exactly full last page has no next cursor")

	conn, err := db.Pool().Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, `SET enable_seqscan = off`)
	require.NoError(t, err)
	rows, err := conn.Query(ctx, `EXPLAIN SELECT id, title, done, version, created_at FROM tasks WHERE (created_at, id) < ($1, $2)
		ORDER BY created_at DESC, id DESC LIMIT 3`, time.Now(), "x")
	require.NoError(t, err)
	lines, err := pgxv5.CollectRows(rows, pgxv5.RowTo[string])
	require.NoError(t, err)
	plan := strings.Join(lines, "\n")
	assert.Contains(t, plan, "Index Scan using tasks_created_at_id_idx")
	assert.Contains(t, plan, "Index Cond: (ROW(created_at, id) < ROW(", "the cursor is the index range, not a filter")
}

func TestPurgeIdempotencyKeys(t *testing.T) {
	db := testsupport.DB(t, containers.Postgres(t))
	st := store.New(db.Pool())
	ctx := context.Background()
	_, _, err := st.Create(ctx, newID(1), "old", &domain.IdempotencyKey{Key: "old", Hash: "h"})
	require.NoError(t, err)
	_, _, err = st.Create(ctx, newID(2), "new", &domain.IdempotencyKey{Key: "new", Hash: "h"})
	require.NoError(t, err)
	_, err = db.Pool().Exec(ctx, `UPDATE idempotency_keys SET created_at = now() - interval '2 days' WHERE key = 'old'`)
	require.NoError(t, err)

	n, err := st.PurgeIdempotencyKeys(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	var left []string
	rows, err := db.Pool().Query(ctx, `SELECT key FROM idempotency_keys`)
	require.NoError(t, err)
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		left = append(left, k)
	}
	assert.Equal(t, []string{"new"}, left)
}

// Re-running the migrations (a second replica booting) is a no-op.
func TestMigrationsAreIdempotent(t *testing.T) {
	dsn := testsupport.SchemaDSN(t, containers.Postgres(t), "")
	testsupport.MigratedDB(t, dsn)
	db := testsupport.MigratedDB(t, dsn)
	var n int
	require.NoError(t, db.Pool().QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n))
	assert.Equal(t, 4, n)
}
