package httpx_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// limited builds a server with the given limits and one route, and returns a
// function that sends a request from a client address and reports the status
// and the Retry-After it got.
func limited(t *testing.T, mut func(*httpx.Config)) (*httpx.Server, func(client string) (int, string)) {
	t.Helper()

	cfg := httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0", LogSampleInitial: 0}
	mut(&cfg)

	srv, err := httpx.NewServer(cfg, nil)
	require.NoError(t, err)
	srv.Mux().HandleFunc("GET /work", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	return srv, func(client string) (int, string) {
		req := httptest.NewRequest(http.MethodGet, "/work", http.NoBody)
		req.RemoteAddr = client + ":40000"

		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		return rec.Code, rec.Header().Get("Retry-After")
	}
}

// The global bucket on a virtual clock: the burst passes, the next request is
// refused with the time until a token, and time — not luck — brings the
// tokens back at exactly the configured rate.
func TestRateLimit_GlobalTokenBucket(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		srv, send := limited(t, func(c *httpx.Config) { c.RateLimit, c.RateBurst = 2, 3 }) // 2 rps, bucket of 3

		for range 3 {
			code, _ := send("10.0.0.1")
			require.Equal(t, http.StatusNoContent, code, "the burst")
		}

		code, retryAfter := send("10.0.0.2")
		require.Equal(t, http.StatusTooManyRequests, code, "the bucket is everyone's: another client is refused too")
		assert.Equal(t, "1", retryAfter, "half a second to the next token, rounded up to a whole second")

		time.Sleep(499 * time.Millisecond)

		code, _ = send("10.0.0.1")
		assert.Equal(t, http.StatusTooManyRequests, code, "a millisecond early")

		time.Sleep(time.Millisecond)

		code, _ = send("10.0.0.1")
		assert.Equal(t, http.StatusNoContent, code, "one token after half a second at two per second")

		code, _ = send("10.0.0.1")
		assert.Equal(t, http.StatusTooManyRequests, code, "and only one")

		time.Sleep(time.Minute)

		passed := 0

		for range 10 {
			if code, _ := send("10.0.0.1"); code == http.StatusNoContent {
				passed++
			}
		}

		assert.Equal(t, 3, passed, "a long idle time refills the bucket to its depth, not beyond")

		// A refused request is a request: counted by status like any other,
		// and by scope in the limiter's own counter.
		assert.InDelta(t, 10, testx.Metric(t, srv.Metrics.Registry, "http_requests_total", map[string]string{"status": "429"}), 0)
		assert.InDelta(t, 10, testx.Metric(t, srv.Metrics.Registry, "http_rate_limited_total", map[string]string{"scope": "global"}), 0)
	})
}

// A bucket per client address: one client over its share is refused while
// the others are served, and its own bucket refills on its own clock.
func TestRateLimit_PerClient(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		srv, send := limited(t, func(c *httpx.Config) { c.RateLimitPerClient, c.RateBurstPerClient = 1, 2 })

		for range 2 {
			code, _ := send("10.0.0.1")
			require.Equal(t, http.StatusNoContent, code)
		}

		code, retryAfter := send("10.0.0.1")
		require.Equal(t, http.StatusTooManyRequests, code)
		assert.Equal(t, "1", retryAfter)

		for range 2 {
			code, _ := send("10.0.0.2")
			assert.Equal(t, http.StatusNoContent, code, "the noisy neighbour does not cost this client anything")
		}

		time.Sleep(time.Second)

		code, _ = send("10.0.0.1")
		assert.Equal(t, http.StatusNoContent, code)
		assert.InDelta(t, 1, testx.Metric(t, srv.Metrics.Registry, "http_rate_limited_total", map[string]string{"scope": "client"}), 0)
	})
}

// A client over its own share must not spend the global bucket: its refused
// requests leave the shared tokens for everyone else.
func TestRateLimit_ARefusedClientDoesNotDrainTheGlobalBucket(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		_, send := limited(t, func(c *httpx.Config) {
			c.RateLimit, c.RateBurst = 1, 3
			c.RateLimitPerClient, c.RateBurstPerClient = 1, 1
		})

		code, _ := send("10.0.0.1")
		require.Equal(t, http.StatusNoContent, code)

		for range 50 {
			code, _ := send("10.0.0.1")
			require.Equal(t, http.StatusTooManyRequests, code)
		}

		for _, other := range []string{"10.0.0.2", "10.0.0.3"} {
			code, _ := send(other)
			assert.Equal(t, http.StatusNoContent, code, "two global tokens were still there")
		}
	})
}

// The table of client buckets is bounded: it forgets clients whose bucket has
// refilled (nothing is lost by that), and when every slot belongs to a client
// still in debt a new address is shed rather than remembered — a table that
// grows with the number of source addresses is the attack.
func TestRateLimit_ClientTableIsBounded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		_, send := limited(t, func(c *httpx.Config) {
			c.RateLimitPerClient, c.RateBurstPerClient = 1, 1
			c.RateClientsMax = 3
		})

		for i := range 3 {
			code, _ := send("10.0.0." + strconv.Itoa(i+1))
			require.Equal(t, http.StatusNoContent, code)
		}

		code, _ := send("10.0.0.99")
		require.Equal(t, http.StatusTooManyRequests, code, "three clients in debt hold the three slots")

		time.Sleep(time.Second) // their buckets are full again: forgettable

		code, _ = send("10.0.0.99")
		assert.Equal(t, http.StatusNoContent, code, "the newcomer takes a slot of a client that owes nothing")
	})
}

func TestRateLimit_OffByDefaultAndValidated(t *testing.T) {
	t.Parallel()

	srv, send := limited(t, func(*httpx.Config) {})

	for range 200 {
		code, _ := send("10.0.0.1")
		require.Equal(t, http.StatusNoContent, code)
	}

	assert.Negative(t, testx.Metric(t, srv.Metrics.Registry, "http_rate_limited_total", nil), "no limiter, no series")

	_, err := httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0", RateLimit: -1}, nil)
	require.ErrorContains(t, err, "HTTP_RATE_")
}

var errTask = errors.New("the task's own error")

// A panic in a background task becomes an error — with the stack in the log
// and a tick of the counter — so whoever supervises the tasks (an errgroup,
// in the services) stops the service in order instead of the runtime killing
// it mid-flight.
func TestGuard_TurnsAPanicIntoAnOrderlyStop(t *testing.T) {
	t.Parallel()

	logs := &testx.LogBuffer{}
	srv, err := httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0"},
		httpx.NewLogger(httpx.LogConfig{Writer: logs}))
	require.NoError(t, err)

	// What an errgroup does with it: the first error cancels the others.
	ctx, cancel := context.WithCancelCause(context.Background())
	stopped := make(chan struct{})

	go func() {
		<-ctx.Done() // what a server's Run does when a sibling fails
		close(stopped)
	}()

	err = srv.Guard("relay", ctx, func(context.Context) error {
		var samples []int

		_ = samples[3] // an index out of range: a runtime panic, not a call to panic()

		return nil
	})()
	cancel(err)

	require.ErrorIs(t, err, httpx.ErrPanic)
	require.ErrorContains(t, err, `background task "relay"`)
	require.ErrorContains(t, err, "index out of range")
	<-stopped

	line := logs.Find(t, map[string]any{"msg": "panic in background task", "task": "relay", "level": "ERROR"})
	require.NotNil(t, line)
	assert.Contains(t, line["stack"], "ratelimit_test.go", "the stack points at the code that panicked")
	assert.InDelta(t, 1, testx.Metric(t, srv.Metrics.Registry, "background_panics_total", map[string]string{"task": "relay"}), 0)

	// A task that returns — an error or nil — is passed through untouched.
	require.ErrorIs(t, srv.Guard("relay", ctx, func(context.Context) error { return errTask })(), errTask)
	require.NoError(t, srv.Guard("relay", ctx, func(context.Context) error { return nil })())
	assert.InDelta(t, 1, testx.Metric(t, srv.Metrics.Registry, "background_panics_total", map[string]string{"task": "relay"}), 0)
}
