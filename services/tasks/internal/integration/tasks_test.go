package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/contracts/tasksapi"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/contract"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// A task lives through create → cached read → update → list → delete; every
// change reaches Kafka through the outbox, in order, with its headers, in the
// shape of the event schemas; and the trace, the logs and the metrics
// describe what happened.
func TestEndToEnd(t *testing.T) {
	sr := testx.Recorder(t) // before wiring: the instrumentation captures the provider then
	d := directDeps(t)
	w := wire(t, d)

	// Create.
	r := w.do(t, http.MethodPost, "/tasks", `{"title":"ship it"}`)
	require.Equal(t, http.StatusCreated, r.status, string(r.body))
	var task tasksapi.Task
	r.decode(t, &task)
	assert.Equal(t, "ship it", task.Title)
	assert.Equal(t, int64(1), task.Version)
	assert.Equal(t, `"1"`, r.header.Get("ETag"))

	// Read: warmed on create.
	r = w.do(t, http.MethodGet, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, "hit", r.header.Get("X-Cache"))

	// Update with the version just read.
	r = w.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"done":true}`, "If-Match", `"1"`)
	require.Equal(t, http.StatusOK, r.status, string(r.body))
	assert.Equal(t, `"2"`, r.header.Get("ETag"))
	r = w.do(t, http.MethodGet, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusOK, r.status)
	var got tasksapi.Task
	r.decode(t, &got)
	assert.True(t, got.Done, "the update is visible at once: the old cache entry was invalidated")
	assert.Equal(t, "miss", r.header.Get("X-Cache"), "tombstoned: served from Postgres")

	// Stale write.
	r = w.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"title":"lost"}`, "If-Match", `"1"`)
	require.Equal(t, http.StatusPreconditionFailed, r.status)

	// List.
	r = w.do(t, http.MethodGet, "/tasks?limit=10", "")
	require.Equal(t, http.StatusOK, r.status)
	var list tasksapi.TaskList
	r.decode(t, &list)
	require.Len(t, list.Tasks, 1)
	assert.Equal(t, task.Id, list.Tasks[0].Id)

	// Delete; then it is gone, from the cache too.
	r = w.do(t, http.MethodDelete, "/tasks/"+task.Id, "", "If-Match", `"2"`)
	require.Equal(t, http.StatusNoContent, r.status)
	r = w.do(t, http.MethodGet, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusNotFound, r.status)
	_, err := w.store.Get(context.Background(), task.Id)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// The three events, in order, on the task's key.
	events := readEvents(t, d.brokers, w.topic, 3, 30*time.Second)
	require.Len(t, events, 3)
	schemas := map[string]*contract.JSONSchema{
		"task.created": contract.LoadJSONSchema(t, "jsonschema/TaskCreatedEvent.json"),
		"task.updated": contract.LoadJSONSchema(t, "jsonschema/TaskUpdatedEvent.json"),
		"task.deleted": contract.LoadJSONSchema(t, "jsonschema/TaskDeletedEvent.json"),
	}
	versions := []float64{1, 2, 2} // a delete carries the last version the task had
	for i, want := range []string{"task.created", "task.updated", "task.deleted"} {
		e := events[i]
		assert.Equal(t, task.Id, e.key, "keyed by task id")
		assert.Equal(t, want, e.headers[kafka.HeaderEventType])
		assert.Equal(t, "application/json", e.headers[kafka.HeaderContentType])
		assert.NotEmpty(t, e.headers[kafka.HeaderTraceparent])
		schemas[want].Validate(t, e.value)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(e.value, &payload))
		assert.Equal(t, e.headers[kafka.HeaderEventID], payload["event_id"], "header and payload agree on the event id")
		assert.Equal(t, versions[i], payload["version"], want)
	}
	assert.Eventually(t, func() bool { return w.pending(t) == 0 }, 10*time.Second, 50*time.Millisecond, "the outbox drains")

	// One trace: the POST's server span, its INSERT, its cache fill, and —
	// later, from the relay — the publish of its event.
	var post trace.SpanContext
	for _, sp := range sr.Ended() {
		if sp.Name() == "POST /tasks" && sp.SpanKind() == trace.SpanKindServer {
			post = sp.SpanContext()
		}
	}
	require.True(t, post.IsValid(), "server span for the POST")
	kinds := map[string]bool{}
	for _, sp := range sr.Ended() {
		if sp.SpanContext().TraceID() != post.TraceID() || sp.SpanKind() == trace.SpanKindServer {
			continue
		}
		name := strings.ToUpper(sp.Name())
		switch {
		case strings.HasPrefix(name, "INSERT"):
			kinds["insert"] = true
		case strings.HasPrefix(name, "SET"):
			kinds["cache-set"] = true
		case strings.HasPrefix(name, "OUTBOX RELAY"):
			kinds["relay"] = true
		case sp.SpanKind() == trace.SpanKindProducer:
			kinds["publish"] = true
		}
	}
	assert.Equal(t, map[string]bool{"insert": true, "cache-set": true, "relay": true, "publish": true}, kinds)
	assert.Contains(t, events[0].headers[kafka.HeaderTraceparent], post.TraceID().String(), "the record continues the request's trace")

	line := w.log.Find(t, map[string]any{"msg": "request", "route": "/tasks", "method": "POST"})
	require.NotNil(t, line, "access-log line for the POST")
	assert.Equal(t, post.TraceID().String(), line["trace_id"])
	assert.Equal(t, float64(201), line["status"])

	reg := w.srv.Metrics.Registry
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "POST", "route": "/tasks", "status": "201"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "http_requests_total", map[string]string{"method": "PATCH", "route": "/tasks/{id}", "status": "412"}))
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "hit"}))
	assert.Equal(t, 3.0, testx.Metric(t, reg, "outbox_published_total", map[string]string{"result": "ok"}))
	assert.Equal(t, 3.0, testx.Metric(t, reg, "kafka_publish_total", map[string]string{"topic": w.topic, "result": "ok"}))
	assert.GreaterOrEqual(t, testx.Metric(t, reg, "pgx_query_duration_seconds", map[string]string{"operation": "tasks.create"}), 1.0)
	assert.Greater(t, testx.Metric(t, reg, "pgxpool_acquire_total", nil), 0.0)
	testx.LintMetrics(t, reg)

	code, body := w.readyz(t)
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `"status":"ready"`)
}

func TestIdempotentCreateOverHTTP(t *testing.T) {
	d := directDeps(t)
	w := wire(t, d)

	var wg sync.WaitGroup
	statuses := make([]int, 6)
	ids := make([]string, 6)
	for i := range statuses {
		wg.Go(func() {
			r := w.do(t, http.MethodPost, "/tasks", `{"title":"once"}`, "Idempotency-Key", "retry-me")
			statuses[i] = r.status
			var task tasksapi.Task
			if json.Unmarshal(r.body, &task) == nil {
				ids[i] = task.Id
			}
		})
	}
	wg.Wait()
	created := 0
	for i, s := range statuses {
		require.Contains(t, []int{http.StatusCreated, http.StatusOK}, s)
		if s == http.StatusCreated {
			created++
		}
		assert.Equal(t, ids[0], ids[i])
	}
	assert.Equal(t, 1, created, "one request creates, the others replay")

	r := w.do(t, http.MethodPost, "/tasks", `{"title":"different"}`, "Idempotency-Key", "retry-me")
	assert.Equal(t, http.StatusUnprocessableEntity, r.status)

	events := readEvents(t, d.brokers, w.topic, 2, 5*time.Second)
	assert.Len(t, events, 1, "exactly one task.created")
}

func TestPaginationOverHTTP(t *testing.T) {
	w := wire(t, directDeps(t))
	var want []string
	for i := range 7 {
		r := w.do(t, http.MethodPost, "/tasks", fmt.Sprintf(`{"title":"t%d"}`, i))
		require.Equal(t, http.StatusCreated, r.status)
		var task tasksapi.Task
		r.decode(t, &task)
		want = append([]string{task.Id}, want...) // newest first
	}
	var seen []string
	path := "/tasks?limit=3"
	for {
		r := w.do(t, http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, r.status)
		var page tasksapi.TaskList
		r.decode(t, &page)
		for _, task := range page.Tasks {
			seen = append(seen, task.Id)
		}
		if page.NextCursor == nil {
			break
		}
		path = "/tasks?limit=3&cursor=" + *page.NextCursor
	}
	assert.Equal(t, want, seen)
}

// Two writers with the same If-Match: exactly one wins, the other gets 412
// and nothing is lost silently.
func TestConcurrentUpdatesOneWins(t *testing.T) {
	w := wire(t, directDeps(t))
	r := w.do(t, http.MethodPost, "/tasks", `{"title":"contended"}`)
	require.Equal(t, http.StatusCreated, r.status)
	var task tasksapi.Task
	r.decode(t, &task)

	const n = 8
	var wg sync.WaitGroup
	statuses := make([]int, n)
	for i := range n {
		wg.Go(func() {
			statuses[i] = w.do(t, http.MethodPatch, "/tasks/"+task.Id,
				fmt.Sprintf(`{"title":"writer %d"}`, i), "If-Match", `"1"`).status
		})
	}
	wg.Wait()
	ok, failed := 0, 0
	for _, s := range statuses {
		switch s {
		case http.StatusOK:
			ok++
		case http.StatusPreconditionFailed:
			failed++
		default:
			t.Errorf("unexpected status %d", s)
		}
	}
	assert.Equal(t, 1, ok)
	assert.Equal(t, n-1, failed)
	stored, err := w.store.Get(context.Background(), task.Id)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stored.Version)
}
