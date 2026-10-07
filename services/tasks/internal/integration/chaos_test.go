package integration_test

// Chaos: the dependencies fail, slow down or hang, and the service does what
// its design promises — serves without the cache, keeps accepting writes
// while the broker is gone (the outbox holds the events and the relay
// delivers them when it is back), degrades instead of dying, and turns
// not-ready only when Postgres, the one critical dependency, is gone.
// Toxiproxy sits in front of Postgres and Valkey; Kafka is frozen. Every
// scenario restores the dependency on cleanup; the scenarios are not
// parallel with each other.

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/contracts/tasksapi"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// proxied wires the service through Toxiproxy for Postgres and Valkey.
func proxied(t *testing.T) (*wired, *containers.Proxy, *containers.Proxy) {
	t.Helper()
	pg := containers.Proxied(t, "postgres")
	vk := containers.Proxied(t, "valkey")
	w := wire(t, deps{
		postgres: "postgres://app:app@" + pg.Addr + "/app?sslmode=disable",
		valkey:   "valkey://" + vk.Addr,
		brokers:  containers.Kafka(t),
	})
	return w, pg, vk
}

func TestChaos_ValkeyDown(t *testing.T) {
	w, _, vk := proxied(t)
	r := w.do(t, http.MethodPost, "/tasks", `{"title":"survives the cache"}`)
	require.Equal(t, http.StatusCreated, r.status)
	var task tasksapi.Task
	r.decode(t, &task)

	vk.Down()

	start := time.Now()
	r = w.do(t, http.MethodGet, "/tasks/"+task.Id, "")
	require.Equal(t, http.StatusOK, r.status, "reads fall through to Postgres")
	assert.Equal(t, "miss", r.header.Get("X-Cache"))
	assert.Less(t, time.Since(start), 3*time.Second, "bounded by VALKEY_OP_TIMEOUT, not hanging")

	r = w.do(t, http.MethodPost, "/tasks", `{"title":"no cache"}`)
	require.Equal(t, http.StatusCreated, r.status, "writes succeed; the cache warm-up is best-effort")
	assert.NotNil(t, w.log.Find(t, map[string]any{"msg": "cache write failed"}), "a warn line, not an error")

	r = w.do(t, http.MethodPatch, "/tasks/"+task.Id, `{"done":true}`)
	require.Equal(t, http.StatusOK, r.status, "a failed invalidation does not fail the committed change")

	body := w.waitReadyz(t, "degraded")
	code, _ := w.readyz(t)
	assert.Equal(t, http.StatusOK, code, "an optional dependency never pulls the pod")
	assert.Contains(t, body, `"postgres":"ok"`)
	assert.Equal(t, 0.0, testx.Metric(t, w.srv.Metrics.Registry, "health_check_up", map[string]string{"check": "valkey", "critical": "false"}))
	assert.Greater(t, testx.Metric(t, w.srv.Metrics.Registry, "cache_command_errors_total", map[string]string{"op": "get"}), 0.0)

	vk.Up()
	w.waitReadyz(t, "ready")
}

func TestChaos_PostgresSlowThenDown(t *testing.T) {
	w, pg, _ := proxied(t)
	w.waitReadyz(t, "ready")

	pg.Latency(3 * time.Second) // > the 900ms check timeout: the check gives up, it does not hang
	w.waitReadyz(t, "not_ready")
	start := time.Now()
	code, body := w.readyz(t)
	assert.Less(t, time.Since(start), time.Second, "probes answer from the cached result")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Contains(t, body, "postgres")

	pg.Reset()
	pg.Down()
	r := w.do(t, http.MethodGet, "/tasks", "")
	assert.Equal(t, http.StatusInternalServerError, r.status)
	assert.Equal(t, "application/problem+json", r.header.Get("Content-Type"))
	line := w.log.Find(t, map[string]any{"msg": "request failed", "detail": "could not list tasks"})
	require.NotNil(t, line, "the 5xx is logged with its cause")
	assert.NotEmpty(t, line["err"])
	w.waitReadyz(t, "not_ready")

	pg.Up()
	w.waitReadyz(t, "ready")
	assert.Eventually(t, func() bool {
		return w.do(t, http.MethodGet, "/tasks", "").status == http.StatusOK
	}, 20*time.Second, 200*time.Millisecond, "requests succeed again once Postgres is back")
}

// The broker hangs: writes stay fast (no publish on the request path), the
// outbox backlog grows and is visible, readiness degrades; once the broker is
// back the relay delivers every event.
func TestChaos_KafkaPaused(t *testing.T) {
	d := directDeps(t)
	w := wire(t, d)
	w.waitReadyz(t, "ready")

	unpause := containers.Pause(t, "kafka")
	const n = 5
	for i := range n {
		start := time.Now()
		r := w.do(t, http.MethodPost, "/tasks", `{"title":"broker hangs"}`)
		require.Equal(t, http.StatusCreated, r.status, "request %d", i)
		assert.Less(t, time.Since(start), time.Second, "the request does not wait on Kafka")
	}
	require.Eventually(t, func() bool {
		return testx.Metric(t, w.srv.Metrics.Registry, "outbox_pending", nil) >= n
	}, 20*time.Second, 100*time.Millisecond, "the backlog is visible on /metrics")
	assert.Greater(t, testx.Metric(t, w.srv.Metrics.Registry, "outbox_published_total", map[string]string{"result": "error"}), 0.0)
	body := w.waitReadyz(t, "degraded")
	assert.Contains(t, body, "kafka")

	unpause()
	assert.Eventually(t, func() bool { return w.pending(t) == 0 }, 60*time.Second, 200*time.Millisecond,
		"the relay catches up once the broker is back")
	events := readEvents(t, d.brokers, w.topic, n, 30*time.Second)
	assert.GreaterOrEqual(t, len(events), n, "every event delivered (at least once)")
	assert.Equal(t, 0.0, testx.Metric(t, w.srv.Metrics.Registry, "outbox_pending", nil))
	w.waitReadyz(t, "ready")
}
