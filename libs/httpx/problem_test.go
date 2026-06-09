package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func TestProblemMarshalDefaults(t *testing.T) {
	b, err := json.Marshal(httpx.NewProblem(http.StatusNotFound, "task 7 does not exist"))
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	require.Equal(t, "about:blank", m["type"])
	require.Equal(t, "Not Found", m["title"])
	require.Equal(t, float64(404), m["status"])
	require.Equal(t, "task 7 does not exist", m["detail"])
	require.NotContains(t, m, "instance")
}

func TestProblemMarshalExtensions(t *testing.T) {
	p := httpx.Problem{
		Type:       "https://errors.example/conflict",
		Title:      "Conflict",
		Status:     http.StatusConflict,
		Instance:   "/tasks/7",
		Extensions: map[string]any{"code": "task_archived", "trace_id": "abc123"},
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	require.Equal(t, "https://errors.example/conflict", m["type"])
	require.Equal(t, "/tasks/7", m["instance"])
	require.Equal(t, "task_archived", m["code"], "extension members are inlined at the top level")
	require.Equal(t, "abc123", m["trace_id"])
}

func TestAbortProblemWritesProblemJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	// Aborting in middleware must stop the route handler from running at all.
	e.Use(func(c *gin.Context) {
		httpx.AbortProblem(c, httpx.NewProblem(http.StatusBadRequest, "bad input"))
	})
	e.GET("/boom", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"unreachable": true})
	})

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
	require.Equal(t, "bad input", m["detail"])
	require.NotContains(t, m, "unreachable", "Abort must stop the rest of the handler from writing")
}
