package httpx_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func probe(t testing.TB, srv *httpx.Server) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec.Code, rec.Body.String()
}

// The probe must answer from cache: many probes inside one interval cost the
// dependency exactly one check, and concurrent probes on a cold cache trigger
// a single refresh rather than a stampede.
func TestReadyz_ProbesDoNotHitDependenciesMoreThanOncePerInterval(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"})
		srv := httpx.NewServer(httpx.Config{Service: "t", HealthInterval: time.Hour}, log)
		srv.Health.SetReady(true)

		var calls atomic.Int32
		srv.Health.Register("db", func(context.Context) error {
			calls.Add(1)
			time.Sleep(20 * time.Millisecond) // wide enough for the probes below to overlap
			return nil
		})

		var wg sync.WaitGroup
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				code, _ := probe(t, srv)
				assert.Equal(t, http.StatusOK, code)
			}()
		}
		wg.Wait()
		for range 50 {
			code, _ := probe(t, srv)
			require.Equal(t, http.StatusOK, code)
		}
		assert.Equal(t, int32(1), calls.Load(), "100 probes, one dependency call")
	}, "httpx", "unit")
}

// A check that hangs is bounded by the timeout and reported as failing; the
// probe itself never hangs.
func TestReadyz_HangingCheckIsBoundedByTimeout(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"})
		srv := httpx.NewServer(httpx.Config{Service: "t", HealthInterval: time.Hour, HealthTimeout: 30 * time.Millisecond}, log)
		srv.Health.SetReady(true)
		srv.Health.Register("slow", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		start := time.Now()
		code, body := probe(t, srv)
		assert.Less(t, time.Since(start), 2*time.Second)
		assert.Equal(t, http.StatusServiceUnavailable, code)
		assert.Contains(t, body, "deadline exceeded")
	}, "httpx", "unit")
}

// Run refreshes in the background: a dependency that recovers is seen on the
// next tick without any probe having to pay for the check.
func TestReadyz_BackgroundLoopTracksRecovery(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"})
		srv := httpx.NewServer(httpx.Config{Service: "t", HealthInterval: 20 * time.Millisecond}, log)
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

		require.Eventually(t, func() bool { c, _ := probe(t, srv); return c == http.StatusServiceUnavailable },
			2*time.Second, 5*time.Millisecond)
		healthy.Store(true)
		require.Eventually(t, func() bool { c, _ := probe(t, srv); return c == http.StatusOK },
			2*time.Second, 5*time.Millisecond)

		// And the metric agrees with the endpoint.
		assert.Equal(t, 1.0, gaugeValue(t, srv, "health_check_up", map[string]string{"check": "db", "critical": "true"}))
	}, "httpx", "unit")
}

// gaugeValue reads one series of a gauge off the server's registry.
func gaugeValue(t testing.TB, srv *httpx.Server, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := srv.Metrics.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			for k, v := range labels {
				found := false
				for _, l := range m.GetLabel() {
					if l.GetName() == k && l.GetValue() == v {
						found = true
					}
				}
				if !found {
					continue next
				}
			}
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("no %s%v in registry", name, labels)
	return 0
}

// The exposition itself is contract: names, label sets and types are what
// dashboards and alerts are written against, so lint them the way Prometheus
// would and pin the ones every service must have.
func TestMetrics_ExpositionIsLintCleanAndComplete(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text", SampleInitial: 100, SampleThereafter: 100})
		srv := httpx.NewServer(httpx.Config{Service: "test", Addr: ":0"}, log)
		srv.Health.SetReady(true)
		srv.Health.Register("db", func(context.Context) error { return nil })
		probe(t, srv)
		// Vectors only expose series that were observed: drive one API request.
		srv.Engine().GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })
		srv.Engine().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

		problems, err := testutil.GatherAndLint(srv.Metrics.Registry)
		require.NoError(t, err)
		assert.Empty(t, problems, "promlint findings")

		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		body := rec.Body.String()
		for _, want := range []string{
			"# TYPE build_info gauge",
			"# TYPE http_requests_total counter",
			"# TYPE http_request_duration_seconds histogram",
			"# TYPE http_requests_in_flight gauge",
			"# TYPE log_dropped_total counter",
			"# TYPE health_check_up gauge",
			`health_check_up{check="db",critical="true"} 1`,
			"# TYPE go_goroutines gauge",
			"# TYPE process_cpu_seconds_total counter",
		} {
			assert.Containsf(t, body, want, "exposition must contain %q", want)
		}
	}, "httpx", "unit")
}
