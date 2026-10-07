package httpx_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func TestAccessLog_LevelsAndFields(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{SlowRequest: 20 * time.Millisecond})
	srv.Mux().HandleFunc("GET /ok/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "hello") })
	srv.Mux().HandleFunc("GET /bad", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusBadRequest, "no"))
	})
	srv.Mux().HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	srv.Mux().HandleFunc("GET /err", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })

	for _, p := range []string{"/ok/7?q=1", "/bad", "/slow", "/err"} {
		get(srv.Handler(), p)
	}
	lines := buf.Lines(t)
	require.Len(t, lines, 4)

	ok := lines[0]
	assert.Equal(t, "request", ok["msg"])
	assert.Equal(t, "INFO", ok["level"])
	assert.Equal(t, "GET", ok["method"])
	assert.Equal(t, "/ok/7", ok["path"], "the path, without the query string")
	assert.Equal(t, "/ok/{id}", ok["route"])
	assert.Equal(t, float64(200), ok["status"])
	assert.Equal(t, float64(5), ok["bytes"])
	assert.Equal(t, "192.0.2.1", ok["client_ip"])
	assert.Contains(t, ok, "latency_ms")
	assert.NotEmpty(t, ok["request_id"])

	assert.Equal(t, "INFO", lines[1]["level"], "4xx are info: warn is never sampled, and scanners exist")
	assert.Equal(t, "slow request", lines[2]["msg"])
	assert.Equal(t, "WARN", lines[2]["level"])
	assert.Equal(t, "ERROR", lines[3]["level"])
	assert.Equal(t, float64(502), lines[3]["status"])
}

func TestWithMiddleware_SeesRequestIDAndRoute(t *testing.T) {
	var sawID, sawRoute string
	var hadDeadline bool
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sawID = httpx.RequestIDFromContext(r.Context())
			_, hadDeadline = r.Context().Deadline()
			next.ServeHTTP(w, r)
			sawRoute = httpx.RouteFromContext(r.Context()) // set once routing ran
		})
	}
	srv, _ := loggedServer(t, httpx.Config{RequestTimeout: time.Minute}, httpx.WithMiddleware(mw))
	var handlerRoute string
	srv.Mux().HandleFunc("GET /tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		handlerRoute = httpx.RouteFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	rec := get(srv.Handler(), "/tasks/9")
	assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), sawID)
	assert.True(t, hadDeadline, "the per-request deadline is applied before extra middleware")
	assert.Equal(t, "/tasks/{id}", sawRoute)
	assert.Empty(t, handlerRoute, "the route is recorded after the handler returns")
}

func TestRequestTimeout_IsTheHandlersDeadline(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{RequestTimeout: 20 * time.Millisecond})
	srv.Mux().HandleFunc("GET /wait", func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		httpx.WriteError(w, r, r.Context().Err())
	})
	start := time.Now()
	rec := get(srv.Handler(), "/wait")
	assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	assert.Less(t, time.Since(start), 2*time.Second)

	srv, _ = loggedServer(t, httpx.Config{RequestTimeout: -1})
	srv.Mux().HandleFunc("GET /dl", func(w http.ResponseWriter, r *http.Request) {
		_, ok := r.Context().Deadline()
		assert.False(t, ok, "a non-positive timeout means no deadline")
	})
	get(srv.Handler(), "/dl")
}

func TestNewServer_NilLoggerIsDiscarded(t *testing.T) {
	srv, err := httpx.NewServer(httpx.Config{Addr: ":0"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, srv.Logger())
	assert.Nil(t, srv.LogLevel)
	rec := get(srv.Handler(), "/nope")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// The shutdown sequence: readiness flips first, requests are still served
// during the drain delay, then Run returns nil once the listeners are closed.
func TestRun_GracefulShutdownSequence(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{
		Addr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0",
		ShutdownDelay: 300 * time.Millisecond, ShutdownTimeout: 2 * time.Second,
	})
	srv.Mux().HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "pong") })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	require.Eventually(t, func() bool { return srv.ListenAddr() != "" && srv.AdminListenAddr() != "" }, 2*time.Second, 5*time.Millisecond)
	api, adm := "http://"+srv.ListenAddr(), "http://"+srv.AdminListenAddr()

	require.Eventually(t, func() bool { return status(t, adm+"/readyz") == http.StatusOK }, 2*time.Second, 5*time.Millisecond)
	resp, err := http.Get(api + "/ping")
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, "pong", string(b))

	cancel()
	require.Eventually(t, func() bool { return status(t, adm+"/readyz") == http.StatusServiceUnavailable },
		time.Second, 5*time.Millisecond, "readiness flips as soon as shutdown starts")
	assert.Equal(t, http.StatusOK, status(t, api+"/ping"), "…while the API still serves during the delay")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "shutdown requested, draining"}))
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "servers stopped cleanly"}))
	_, err = http.Get(api + "/ping") //nolint:bodyclose // expected to fail
	assert.Error(t, err, "the listener is closed")
}

func TestRun_PortInUseIsReturned(t *testing.T) {
	busy := httptest.NewServer(http.NotFoundHandler())
	defer busy.Close()
	srv, _ := loggedServer(t, httpx.Config{Addr: busy.Listener.Addr().String(), AdminAddr: "127.0.0.1:0"})
	err := srv.Run(context.Background())
	require.ErrorContains(t, err, "bind API listener")
}

func status(t testing.TB, url string) int {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec,noctx // test URL on a loopback listener
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}
