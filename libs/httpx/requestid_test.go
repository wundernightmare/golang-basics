package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func requestIDServer(t testing.TB) (*httpx.Server, *testx.LogBuffer) {
	t.Helper()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	srv := httpx.NewServer(httpx.Config{Service: "test"}, log)
	srv.Engine().GET("/ok", func(c *gin.Context) {
		log.InfoContext(c.Request.Context(), "in handler")
		c.String(http.StatusOK, httpx.RequestIDFromContext(c.Request.Context()))
	})
	srv.Engine().GET("/fail", func(c *gin.Context) {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusConflict, "nope"))
	})
	return srv, buf
}

func TestRequestID_GeneratedEchoedAndLogged(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := requestIDServer(t)
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ok", nil))

		id := rec.Header().Get(httpx.RequestIDHeader)
		require.Len(t, id, 16, "16 hex chars")
		assert.Equal(t, id, rec.Body.String(), "the handler sees the same id through the context")
		assert.Equal(t, id, buf.Find(t, map[string]any{"msg": "in handler"})["request_id"])
		assert.Equal(t, id, buf.Find(t, map[string]any{"msg": "request"})["request_id"], "the access-log line too")

		rec = httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ok", nil))
		assert.NotEqual(t, id, rec.Header().Get(httpx.RequestIDHeader), "fresh per request")
	}, "httpx", "unit")
}

func TestRequestID_InboundIsHonouredWhenSane(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := requestIDServer(t)
		for in, kept := range map[string]bool{
			"01J9ZK4Q7W1X2Y3Z4A5B6C7D8E":           true,  // ULID
			"7f3c4d5e-1234-4abc-9def-0123456789ab": true,  // UUID
			"has space":                            false, // not a token
			"bad\x7f":                              false, // control char
			strings.Repeat("x", 129):               false, // too long
		} {
			req := httptest.NewRequest(http.MethodGet, "/ok", nil)
			req.Header.Set(httpx.RequestIDHeader, in)
			rec := httptest.NewRecorder()
			srv.Engine().ServeHTTP(rec, req)
			out := rec.Header().Get(httpx.RequestIDHeader)
			if kept {
				assert.Equal(t, in, out)
			} else {
				assert.NotEqual(t, in, out, "%q must be replaced", in)
				assert.Len(t, out, 16)
			}
		}
	}, "httpx", "unit")
}

func TestRequestID_InProblemBody(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := requestIDServer(t)
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/fail", nil))
		require.Equal(t, http.StatusConflict, rec.Code)
		var m map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
		assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), m["request_id"])
		assert.Equal(t, "/fail", m["instance"], "instance defaults to the request path")
	}, "httpx", "unit")
}

func TestAbortProblem_DoesNotMutateCallerExtensions(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := requestIDServer(t)
		ext := map[string]any{"code": "x"}
		srv.Engine().GET("/ext", func(c *gin.Context) {
			httpx.AbortProblem(c, httpx.Problem{Status: 400, Instance: "/custom", Extensions: ext})
		})
		rec := httptest.NewRecorder()
		srv.Engine().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ext", nil))
		var m map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
		assert.Equal(t, "/custom", m["instance"], "an explicit instance wins")
		assert.NotEmpty(t, m["request_id"])
		assert.NotContains(t, ext, "request_id", "the caller's map is left alone")
	}, "httpx", "unit")
}
