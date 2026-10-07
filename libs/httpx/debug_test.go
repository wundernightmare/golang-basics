package httpx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// debugServer is a server at level info with a debug token and a route that
// logs one debug line with the request context.
func debugServer(t testing.TB, token string) (*httpx.Server, *testx.LogBuffer) {
	t.Helper()
	srv, buf := loggedServer(t, httpx.Config{DebugToken: token})
	srv.Mux().HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) {
		srv.Logger().DebugContext(r.Context(), "handler detail", "step", 1)
		w.WriteHeader(http.StatusNoContent)
	})
	return srv, buf
}

func TestDebugToken_TurnsOnDebugForOneRequest(t *testing.T) {
	srv, buf := debugServer(t, "dbg")

	// Without the header: info level, the debug line is not written.
	rec := get(srv.Handler(), "/work")
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Header().Get(httpx.DebugLoggingHeader))
	assert.Nil(t, buf.Find(t, map[string]any{"msg": "handler detail"}))

	// With it: the debug line lands, tagged with the request id, and the
	// response says so.
	req := httptest.NewRequest(http.MethodGet, "/work", nil)
	req.Header.Set(httpx.DebugTokenHeader, "dbg")
	rec = do(srv.Handler(), req)
	assert.Equal(t, "on", rec.Header().Get(httpx.DebugLoggingHeader))
	line := buf.Find(t, map[string]any{"msg": "handler detail"})
	require.NotNil(t, line)
	assert.Equal(t, "DEBUG", line["level"])
	assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), line["request_id"])

	// The next request is back to normal — it was a switch for one request.
	get(srv.Handler(), "/work")
	assert.Len(t, filterMsg(buf.Lines(t), "handler detail"), 1)
}

func TestDebugToken_WrongTokenIsIgnoredSilently(t *testing.T) {
	srv, buf := debugServer(t, "dbg")
	for _, token := range []string{"guess", "dbg ", "DBG", "dbgdbg"} {
		req := httptest.NewRequest(http.MethodGet, "/work", nil)
		req.Header.Set(httpx.DebugTokenHeader, token)
		rec := do(srv.Handler(), req)
		assert.Equal(t, http.StatusNoContent, rec.Code, "the request itself is unaffected")
		assert.Empty(t, rec.Header().Get(httpx.DebugLoggingHeader), token)
	}
	assert.Nil(t, buf.Find(t, map[string]any{"msg": "handler detail"}))
	for _, l := range buf.Lines(t) {
		assert.NotContains(t, l["msg"], "token", "a wrong token is not an oracle, not even in the log")
	}
}

func TestDebugToken_UnsetMeansTheHeaderDoesNothing(t *testing.T) {
	srv, buf := debugServer(t, "")
	for _, token := range []string{"", "anything"} {
		req := httptest.NewRequest(http.MethodGet, "/work", nil)
		req.Header.Set(httpx.DebugTokenHeader, token)
		rec := do(srv.Handler(), req)
		assert.Empty(t, rec.Header().Get(httpx.DebugLoggingHeader))
	}
	assert.Nil(t, buf.Find(t, map[string]any{"msg": "handler detail"}))
}

func TestWithDebugLogging_BypassesLevelAndSampling(t *testing.T) {
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{
		Level: "error", Format: "json", Writer: buf,
		SampleInitial: 1, SampleThereafter: 1000, SampleTick: time.Minute,
	})
	ctx := httpx.WithDebugLogging(context.Background())
	assert.True(t, httpx.DebugLogging(ctx))
	assert.False(t, httpx.DebugLogging(context.Background()))
	for range 20 {
		log.DebugContext(ctx, "traced")
		log.Debug("untraced")
	}
	assert.Len(t, filterMsg(buf.Lines(t), "traced"), 20, "every line of the debugged unit of work, unsampled")
	assert.Empty(t, filterMsg(buf.Lines(t), "untraced"), "the level still applies to everything else")
}
