package httpx_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func probe(srv *httpx.Server) (int, string) {
	rec := get(srv.Admin(), "/readyz")
	return rec.Code, rec.Body.String()
}

func TestHealthz_AlwaysOK(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Health.Register("db", func(context.Context) error { return errors.New("down") })
	// /livez is the same handler under the Kubernetes-style name; liveness
	// never depends on the checks or the gate.
	for _, path := range []string{"/healthz", "/livez"} {
		rec := get(srv.Admin(), path)
		assert.Equal(t, http.StatusOK, rec.Code, path)
		assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String(), path)
	}
}

func TestReadyz_GateClosedThenOpen(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})

	code, body := probe(srv)
	assert.Equal(t, http.StatusServiceUnavailable, code, "closed before SetReady")
	assert.Contains(t, body, `"status":"not_ready"`)

	srv.Health.SetReady(true)
	code, body = probe(srv)
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `"status":"ready"`)

	srv.Health.SetReady(false)
	code, _ = probe(srv)
	assert.Equal(t, http.StatusServiceUnavailable, code, "closed again at shutdown")
}

func TestReadyz_CriticalFailureIsNotReady_OptionalIsDegraded(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Health.SetReady(true)
	srv.Health.Register("db", func(context.Context) error { return errors.New("connection refused") })
	srv.Health.Register("cache", func(context.Context) error { return nil }, httpx.Optional())

	code, body := probe(srv)
	assert.Equal(t, http.StatusServiceUnavailable, code, "a failing critical check pulls the pod")
	assert.Contains(t, body, `"status":"not_ready"`)
	assert.Contains(t, body, "connection refused")
	assert.Contains(t, body, `"cache":"ok"`)

	// Swap: db healthy, cache (optional) failing → still ready, reported degraded.
	srv.Health.Register("db", func(context.Context) error { return nil })
	srv.Health.Register("cache", func(context.Context) error { return errors.New("timeout") }, httpx.Optional())
	code, body = probe(srv)
	assert.Equal(t, http.StatusOK, code, "an optional dependency must not pull the pod")
	assert.Contains(t, body, `"status":"degraded"`)
	assert.Contains(t, body, `"cache":"timeout"`)
}

// The probe must answer from cache: many probes inside one interval cost the
// dependency exactly one check, and concurrent probes on a cold cache trigger
// a single refresh rather than a stampede.
func TestReadyz_ProbesDoNotHitDependenciesMoreThanOncePerInterval(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{HealthInterval: time.Hour})
	srv.Health.SetReady(true)

	var calls atomic.Int32
	srv.Health.Register("db", func(context.Context) error {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond) // wide enough for the probes below to overlap
		return nil
	})

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			code, _ := probe(srv)
			assert.Equal(t, http.StatusOK, code)
		})
	}
	wg.Wait()
	for range 50 {
		code, _ := probe(srv)
		require.Equal(t, http.StatusOK, code)
	}
	assert.Equal(t, int32(1), calls.Load(), "100 probes, one dependency call")
}

// A check that hangs is bounded by the timeout and reported as failing; the
// probe itself never hangs, even when the check ignores its context.
func TestReadyz_HangingCheckIsBoundedByTimeout(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{HealthInterval: time.Hour, HealthTimeout: 30 * time.Millisecond})
	srv.Health.SetReady(true)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	srv.Health.Register("stuck", func(context.Context) error {
		<-release // ignores its context, like a client blocked on a frozen broker
		return nil
	})

	start := time.Now()
	code, body := probe(srv)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Contains(t, body, "deadline exceeded")
}

// A check whose previous evaluation has not returned is not started again:
// no goroutine piles onto the same hung call, however often the loop ticks.
func TestHealth_StuckCheckIsNotRunTwice(t *testing.T) {
	h := httpx.NewHealth(5*time.Millisecond, 2*time.Millisecond)
	var calls atomic.Int32
	release := make(chan struct{})
	h.Register("stuck", func(context.Context) error {
		calls.Add(1)
		<-release
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	time.Sleep(100 * time.Millisecond) // ~20 ticks
	assert.Equal(t, int32(1), calls.Load(), "one evaluation in flight at a time")

	close(release) // the dependency answers; the next tick runs the check again
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 2*time.Second, 5*time.Millisecond)
}

// Run refreshes in the background: a dependency that recovers is seen on the
// next tick without any probe having to pay for the check.
func TestReadyz_BackgroundLoopTracksRecovery(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{HealthInterval: 20 * time.Millisecond, HealthTimeout: 10 * time.Millisecond})
	srv.Health.SetReady(true)

	var healthy atomic.Bool
	srv.Health.Register("db", func(context.Context) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("down")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Health.Run(ctx)

	require.Eventually(t, func() bool { c, _ := probe(srv); return c == http.StatusServiceUnavailable },
		2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 0.0, testx.Metric(t, srv.Metrics.Registry, "health_check_up", map[string]string{"check": "db", "critical": "true"}))
	healthy.Store(true)
	require.Eventually(t, func() bool { c, _ := probe(srv); return c == http.StatusOK },
		2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 1.0, testx.Metric(t, srv.Metrics.Registry, "health_check_up", map[string]string{"check": "db", "critical": "true"}),
		"the metric agrees with the endpoint")
}

func TestHealth_DurationHistogram(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{HealthInterval: time.Hour})
	srv.Health.SetReady(true)
	srv.Health.Register("db", func(context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	probe(srv)

	h := histogram(t, srv.Metrics.Registry, "health_check_duration_seconds", map[string]string{"check": "db"})
	require.NotNil(t, h, "each evaluation is observed")
	assert.Equal(t, uint64(1), h.GetSampleCount())
	assert.GreaterOrEqual(t, h.GetSampleSum(), 0.01, "the latency, in seconds")
}

// Re-registering a check drops its old series: a check that turns from
// critical to optional must not leave health_check_up{critical="true"} 0
// behind to fire an alert forever.
func TestHealth_ReRegisterClearsStaleSeries(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{HealthInterval: time.Hour})
	srv.Health.SetReady(true)
	reg := srv.Metrics.Registry

	srv.Health.Register("cache", func(context.Context) error { return errors.New("down") })
	probe(srv)
	assert.Equal(t, 0.0, testx.Metric(t, reg, "health_check_up", map[string]string{"check": "cache", "critical": "true"}))

	srv.Health.Register("cache", func(context.Context) error { return errors.New("down") }, httpx.Optional())
	assert.Equal(t, -1.0, testx.Metric(t, reg, "health_check_up", map[string]string{"check": "cache", "critical": "true"}),
		"the critical series is gone as soon as the check is replaced")

	code, _ := probe(srv)
	assert.Equal(t, http.StatusOK, code, "fresh evaluation: optional now, so degraded not unready")
	assert.Equal(t, 0.0, testx.Metric(t, reg, "health_check_up", map[string]string{"check": "cache", "critical": "false"}))
	assert.Len(t, series(t, reg, "health_check_up"), 1)
}

func TestHealth_StandaloneCollectorsLint(t *testing.T) {
	h := httpx.NewHealth(0, 0) // defaults
	reg := prometheus.NewRegistry()
	reg.MustRegister(h.Collectors()...)
	h.SetReady(true)
	h.Register("db", func(context.Context) error { return nil })
	rec := get(h.ReadyHandler(), "/readyz")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1.0, testx.Metric(t, reg, "health_check_up", map[string]string{"check": "db"}))
	testx.LintMetrics(t, reg)
}
