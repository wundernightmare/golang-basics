package httpx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// debugServer is a server at level info with a debug token and a route that
// logs one debug line with the request context.
func debugServer(t testing.TB, token string) (*httpx.Server, *testx.LogBuffer) {
	t.Helper()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	srv := httpx.NewServer(httpx.Config{Service: "test", DebugToken: token}, log)
	srv.Engine().GET("/work", func(c *gin.Context) {
		log.DebugContext(c.Request.Context(), "handler detail", "step", 1)
		c.Status(http.StatusNoContent)
	})
	return srv, buf
}

func TestDebugToken_TurnsOnDebugForOneRequest(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := debugServer(t, "dbg")

		// Without the header: info level, the debug line is not written.
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/work", nil))
		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, rec.Header().Get(httpx.DebugLoggingHeader))
		assert.Nil(t, buf.Find(t, map[string]any{"msg": "handler detail"}))

		// With it: the debug line lands, tagged with the request id, and the
		// response says so.
		req := httptest.NewRequest(http.MethodGet, "/work", nil)
		req.Header.Set(httpx.DebugTokenHeader, "dbg")
		rec = httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, req)
		assert.Equal(t, "on", rec.Header().Get(httpx.DebugLoggingHeader))
		line := buf.Find(t, map[string]any{"msg": "handler detail"})
		require.NotNil(t, line)
		assert.Equal(t, "DEBUG", line["level"])
		assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), line["request_id"])

		// The next request is back to normal — it was a switch for one request.
		rec = httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/work", nil))
		assert.Len(t, filterMsg(buf.Lines(t), "handler detail"), 1)
	}, "httpx", "unit")
}

func TestDebugToken_WrongTokenIsIgnoredSilently(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := debugServer(t, "dbg")
		req := httptest.NewRequest(http.MethodGet, "/work", nil)
		req.Header.Set(httpx.DebugTokenHeader, "guess")
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNoContent, rec.Code, "the request itself is unaffected")
		assert.Empty(t, rec.Header().Get(httpx.DebugLoggingHeader))
		assert.Nil(t, buf.Find(t, map[string]any{"msg": "handler detail"}))
		for _, l := range buf.Lines(t) {
			assert.NotContains(t, l["msg"], "token", "a wrong token is not an oracle, not even in the log")
		}
	}, "httpx", "unit")
}

func TestDebugToken_UnsetMeansTheHeaderDoesNothing(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := debugServer(t, "")
		req := httptest.NewRequest(http.MethodGet, "/work", nil)
		req.Header.Set(httpx.DebugTokenHeader, "")
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, req)
		assert.Empty(t, rec.Header().Get(httpx.DebugLoggingHeader))
		assert.Nil(t, buf.Find(t, map[string]any{"msg": "handler detail"}))
	}, "httpx", "unit")
}

func TestWithDebugLogging_BypassesLevelAndSampling(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		buf := &testx.LogBuffer{}
		log := httpx.NewLogger(httpx.LogConfig{
			Level: "error", Format: "json", Writer: buf,
			SampleInitial: 1, SampleThereafter: 1000, SampleTick: time.Minute,
		})
		ctx := httpx.WithDebugLogging(context.Background())
		for range 20 {
			log.DebugContext(ctx, "traced")
			log.Debug("untraced")
		}
		assert.Len(t, filterMsg(buf.Lines(t), "traced"), 20, "every line of the debugged unit of work, unsampled")
		assert.Empty(t, filterMsg(buf.Lines(t), "untraced"), "the level still applies to everything else")
	}, "httpx", "unit")
}

func filterMsg(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}
