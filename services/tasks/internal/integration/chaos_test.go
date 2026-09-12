package integration_test

// Chaos: the dependencies fail, slow down or hang, and the service does what
// its design promises — keeps serving without the cache, publishes best-
// effort, degrades instead of dying, and turns not-ready only when Postgres,
// the one critical dependency, is gone. Toxiproxy sits in front of Postgres
// and Valkey (libs/testx), Kafka is frozen with a container pause. Every
// scenario restores the dependency on cleanup, and the suite is not parallel
// with the rest of the package.

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/testo"
	allure "github.com/ozontech/testo-allure"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

type ChaosSuite struct{ testo.Suite[testx.T] }

func TestChaos(t *testing.T) {
	testo.RunSuite(t, new(ChaosSuite), testx.Options("tasks", "integration", "chaos", testx.Meta{
		Epic: "golang-basics", Feature: "tasks resilience", Owner: "@team-platform",
	})...)
}

// proxied wires the service through Toxiproxy for Postgres and Valkey.
func proxied(t testx.T) (wired, *testx.Proxy, *testx.Proxy) {
	t.Helper()
	pg := testx.Proxied(t, "postgres")
	vk := testx.Proxied(t, "valkey")
	w := wireWith(t, deps{
		db:      "postgres://app:app@" + pg.Addr + "/app?sslmode=disable",
		valkey:  "valkey://" + vk.Addr,
		brokers: testx.Kafka(t),
	})
	return w, pg, vk
}

func waitReadyz(t testx.T, w wired, wantStatus string) string {
	t.Helper()
	var body string
	require.Eventually(t, func() bool {
		_, body = readyz(t, w.srv)
		return contains(body, `"status":"`+wantStatus+`"`)
	}, 40*time.Second, 100*time.Millisecond, "readyz never reported %q (last: %s)", wantStatus, body)
	return body
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func stringsReader(s string) io.Reader { return strings.NewReader(s) }

func (ChaosSuite) TestValkeyDown(t testx.T) {
	testx.Case(t, "GB-201", "the cache is optional") // sample TestOps id — replace with your project\'s
	t.Title("Valkey unreachable: reads fall through to Postgres, writes succeed, readiness degrades but stays 200")
	t.Severity(allure.SeverityCritical)
	w, _, vk := proxied(t)

	id := ""
	testx.Step(t, "a task exists (created while healthy)", func(t testx.T) {
		created := postJSON(t, w.ts, "/tasks", `{"title":"survives the cache"}`, http.StatusCreated)
		id, _ = created["id"].(string)
		t.Require().NotEmpty(id)
	})

	vk.Down()

	testx.Step(t, "GET /tasks/{id} is served from Postgres with X-Cache: miss", func(t testx.T) {
		resp := get(t, w.ts, "/tasks/"+id)
		_ = resp.Body.Close()
		t.Require().Equal(http.StatusOK, resp.StatusCode)
		t.Assert().Equal("miss", resp.Header.Get("X-Cache"))
	})
	testx.Step(t, "POST /tasks still creates (cache write is best-effort, logged at warn)", func(t testx.T) {
		created := postJSON(t, w.ts, "/tasks", `{"title":"no cache"}`, http.StatusCreated)
		t.Assert().NotEmpty(created["id"])
		t.Assert().NotNil(w.log.Find(t, map[string]any{"msg": "cache write failed"}), "the failed cache write is a warn line, not an error")
	})
	testx.Step(t, "readyz: 200 degraded, valkey failing, postgres and kafka ok", func(t testx.T) {
		body := waitReadyz(t, w, "degraded")
		t.Attach("readyz", allure.Bytes(body).As(allure.DocumentJSON))
		code, _ := readyz(t, w.srv)
		t.Assert().Equal(http.StatusOK, code, "an optional dependency never pulls the pod")
		t.Assert().Contains(body, `"postgres":"ok"`)
		t.Assert().Equal(0.0, testx.Metric(t, w.srv.Metrics.Registry, "health_check_up", map[string]string{"check": "valkey", "critical": "false"}))
	})

	vk.Up()
	testx.Step(t, "readiness recovers on its own", func(t testx.T) {
		waitReadyz(t, w, "ready")
	})
}

func (ChaosSuite) TestPostgresSlowAndDown(t testx.T) {
	testx.Case(t, "GB-202", "Postgres is the critical dependency") // sample TestOps id — replace with your project\'s
	t.Title("Postgres slow beyond the check timeout, then down: readiness turns 503 within the timeout and recovers")
	t.Severity(allure.SeverityCritical)
	w, pg, _ := proxied(t)

	testx.Step(t, "healthy first", func(t testx.T) { waitReadyz(t, w, "ready") })

	pg.Latency(3 * time.Second) // > HealthTimeout (2s): the check must give up, not hang
	testx.Step(t, "slow Postgres: readyz is 503 not_ready and the probe itself stays fast", func(t testx.T) {
		waitReadyz(t, w, "not_ready")
		start := time.Now()
		code, body := readyz(t, w.srv)
		t.Assert().Less(time.Since(start), time.Second, "probes answer from the cached result")
		t.Assert().Equal(http.StatusServiceUnavailable, code)
		t.Assert().Contains(body, "postgres")
		t.Assert().Equal(0.0, testx.Metric(t, w.srv.Metrics.Registry, "health_check_up", map[string]string{"check": "postgres", "critical": "true"}))
	})

	pg.Reset()
	pg.Down()
	testx.Step(t, "Postgres down: requests fail with a 500 problem, readiness stays 503", func(t testx.T) {
		resp := get(t, w.ts, "/tasks")
		_ = resp.Body.Close()
		t.Assert().Equal(http.StatusInternalServerError, resp.StatusCode)
		t.Assert().Contains(resp.Header.Get("Content-Type"), "application/problem+json")
		waitReadyz(t, w, "not_ready")
	})

	pg.Up()
	testx.Step(t, "Postgres back: readiness recovers and requests succeed", func(t testx.T) {
		waitReadyz(t, w, "ready")
		resp := get(t, w.ts, "/tasks")
		_ = resp.Body.Close()
		t.Assert().Equal(http.StatusOK, resp.StatusCode)
	})
}

func (ChaosSuite) TestKafkaHangs(t testx.T) {
	testx.Case(t, "GB-203", "events are best-effort") // sample TestOps id — replace with your project\'s
	t.Title("Kafka stops answering: POST /tasks still creates, the publish is a warn line, readiness degrades and recovers")
	t.Severity(allure.SeverityCritical)
	w := wire(t)
	testx.Step(t, "healthy first", func(t testx.T) { waitReadyz(t, w, "ready") })

	unpause := testx.Pause(t, "kafka")
	testx.Step(t, "POST /tasks is 201 although the broker hangs", func(t testx.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.ts.URL+"/tasks", stringsReader(`{"title":"broker hangs"}`))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := w.ts.Client().Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		t.Require().Equal(http.StatusCreated, resp.StatusCode)
		t.Attach("latency", allure.Bytes(time.Since(start).String()).As(allure.TextPlain))
		t.Assert().Less(time.Since(start), 10*time.Second, "bounded by KAFKA_PUBLISH_TIMEOUT (5s), not by the request")
		t.Assert().NotNil(w.log.Find(t, map[string]any{"msg": "event publish failed"}), "publish failure is a warn line")
	})
	testx.Step(t, "readyz: 200 degraded with kafka failing", func(t testx.T) {
		body := waitReadyz(t, w, "degraded")
		t.Assert().Contains(body, "kafka")
		code, _ := readyz(t, w.srv)
		t.Assert().Equal(http.StatusOK, code)
	})

	unpause()
	testx.Step(t, "broker back: readiness recovers", func(t testx.T) { waitReadyz(t, w, "ready") })
}
