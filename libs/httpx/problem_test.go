package httpx_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func TestProblem_MarshalDefaults(t *testing.T) {
	b, err := json.Marshal(httpx.NewProblem(http.StatusNotFound, "task 7 does not exist"))
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Equal(t, "about:blank", m["type"])
	assert.Equal(t, "Not Found", m["title"])
	assert.Equal(t, float64(404), m["status"])
	assert.Equal(t, "task 7 does not exist", m["detail"])
	assert.NotContains(t, m, "instance")
}

func TestProblem_MarshalExtensions(t *testing.T) {
	p := httpx.Problem{
		Type:       "https://errors.example/conflict",
		Title:      "Conflict",
		Status:     http.StatusConflict,
		Instance:   "/tasks/7",
		Extensions: map[string]any{"code": "task_archived", "trace_id": "abc123", "status": 999},
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Equal(t, "https://errors.example/conflict", m["type"])
	assert.Equal(t, "/tasks/7", m["instance"])
	assert.Equal(t, "task_archived", m["code"], "extension members are inlined at the top level")
	assert.Equal(t, "abc123", m["trace_id"])
	assert.Equal(t, float64(409), m["status"], "an extension cannot override a standard member")
}

func TestProblem_IsAnError(t *testing.T) {
	cause := errors.New("pgconn: connection refused")
	p := httpx.Internal("could not persist", cause)
	assert.Equal(t, "Internal Server Error: could not persist (pgconn: connection refused)", p.Error())
	require.ErrorIs(t, p, cause, "Unwrap exposes the cause")

	wrapped := fmt.Errorf("store: %w", httpx.NewProblem(http.StatusConflict, "exists"))
	var got httpx.Problem
	require.ErrorAs(t, wrapped, &got)
	assert.Equal(t, http.StatusConflict, got.Status)
	assert.Equal(t, "Conflict: exists", got.Error())

	assert.Equal(t, "Internal Server Error", httpx.Problem{}.Error(), "a zero problem reads as a 500")

	b, err := json.Marshal(p)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "pgconn", "the cause is never serialised")
}

func TestWriteProblem_InvalidStatusDegradesTo500(t *testing.T) {
	// Regression for the FuzzProblemJSON crasher kept in testdata/fuzz: an
	// out-of-range status must produce a 500 problem, not a WriteHeader panic.
	srv, _ := loggedServer(t, httpx.Config{})
	for _, status := range []int{0, -1, 99, 1000, 1005} {
		srv.Mux().HandleFunc(fmt.Sprintf("GET /s%d", status), func(w http.ResponseWriter, r *http.Request) {
			httpx.WriteProblem(w, r, httpx.NewProblem(status, "x"))
		})
		rec := get(srv.Handler(), fmt.Sprintf("/s%d", status))
		require.Equal(t, http.StatusInternalServerError, rec.Code, "status %d", status)
		require.Contains(t, rec.Body.String(), `"status":500`)
	}
}

func TestWriteProblem_FillsInstanceAndRequestID(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	ext := map[string]any{"code": "x"}
	srv.Mux().HandleFunc("GET /fail", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusConflict, "nope"))
	})
	srv.Mux().HandleFunc("GET /ext", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.Problem{Status: http.StatusBadRequest, Instance: "/custom", Extensions: ext})
	})
	srv.Mux().HandleFunc("GET /own-id", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.Problem{Status: http.StatusBadRequest, Extensions: map[string]any{"request_id": "mine"}})
	})

	rec := get(srv.Handler(), "/fail")
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	m := decode(t, rec)
	assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), m["request_id"])
	assert.Equal(t, "/fail", m["instance"], "instance defaults to the request path")

	m = decode(t, get(srv.Handler(), "/ext"))
	assert.Equal(t, "/custom", m["instance"], "an explicit instance wins")
	assert.NotEmpty(t, m["request_id"])
	assert.Equal(t, "x", m["code"])
	assert.NotContains(t, ext, "request_id", "the caller's map is left alone")

	m = decode(t, get(srv.Handler(), "/own-id"))
	assert.Equal(t, "mine", m["request_id"], "an explicit request_id extension wins")
}

func TestWriteProblem_OutsideTheServer(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteProblem(rec, httptest.NewRequest(http.MethodGet, "/x", nil), httpx.NewProblem(http.StatusTeapot, ""))
	assert.Equal(t, http.StatusTeapot, rec.Code)
	m := decode(t, rec)
	assert.Equal(t, "/x", m["instance"])
	assert.NotContains(t, m, "request_id", "no request id outside a server request")
	assert.NotContains(t, m, "detail", "an empty detail is omitted")

	rec = httptest.NewRecorder()
	httpx.WriteProblem(rec, nil, httpx.NewProblem(http.StatusBadRequest, "no request"))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "a nil request is tolerated")
}

func TestWriteError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantDetail string
		logged     bool // 5xx: the cause goes to the log
	}{
		{"problem as is", httpx.NewProblem(http.StatusNotFound, "no such task"), http.StatusNotFound, "no such task", false},
		{"wrapped problem", fmt.Errorf("svc: %w", httpx.NewProblem(http.StatusConflict, "archived")), http.StatusConflict, "archived", false},
		{"deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), http.StatusGatewayTimeout, "the request took too long", true},
		{"anything else", errors.New("pgconn: connection refused"), http.StatusInternalServerError, "internal error", true},
		{"5xx problem with cause", httpx.Internal("could not persist", errors.New("disk full")), http.StatusInternalServerError, "could not persist", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, buf := loggedServer(t, httpx.Config{})
			srv.Mux().HandleFunc("GET /x", func(w http.ResponseWriter, r *http.Request) { httpx.WriteError(w, r, tc.err) })
			rec := get(srv.Handler(), "/x")
			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
			m := decode(t, rec)
			assert.Equal(t, tc.wantDetail, m["detail"])

			line := buf.Find(t, map[string]any{"msg": "request failed"})
			if !tc.logged {
				assert.Nil(t, line, "a 4xx is not an error to log")
				return
			}
			require.NotNil(t, line)
			assert.Equal(t, "ERROR", line["level"])
			assert.Equal(t, float64(tc.wantStatus), line["status"])
			assert.Equal(t, rec.Header().Get(httpx.RequestIDHeader), line["request_id"])
			assert.NotEmpty(t, line["err"], "the cause is logged")
			assert.NotContains(t, rec.Body.String(), line["err"], "…and never sent")
		})
	}
}

func TestDecodeJSON(t *testing.T) {
	type in struct {
		Title string `json:"title"`
	}
	for _, tc := range []struct {
		name, body string
		wantStatus int
	}{
		{"ok", `{"title":"x"}`, 0},
		{"ok with trailing whitespace", "{\"title\":\"x\"}\n", 0},
		{"syntax error", `{"title":`, http.StatusBadRequest},
		{"wrong type", `{"title":7}`, http.StatusBadRequest},
		{"trailing data", `{"title":"x"}{}`, http.StatusBadRequest},
		{"empty", ``, http.StatusBadRequest},
		{"too big", `{"title":"` + strings.Repeat("x", 100) + `"}`, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			req.Body = http.MaxBytesReader(rec, req.Body, 64)
			var v in
			err := httpx.DecodeJSON(req, &v)
			if tc.wantStatus == 0 {
				require.NoError(t, err)
				assert.Equal(t, "x", v.Title)
				return
			}
			var p httpx.Problem
			require.ErrorAs(t, err, &p)
			assert.Equal(t, tc.wantStatus, p.Status)
		})
	}
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteJSON(rec, http.StatusCreated, map[string]int{"n": 1})
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	body, _ := io.ReadAll(rec.Body)
	assert.JSONEq(t, `{"n":1}`, string(body))
}

// Every error the API port emits is a problem: an unknown route is a 404
// problem and a known route with the wrong method a 405 problem with Allow.
func TestServer_NoRouteAndNoMethodAreProblems(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("GET /known", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv.Mux().HandleFunc("POST /known", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	rec := get(srv.Handler(), "/nope")
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	assert.Equal(t, "no route for GET /nope", decode(t, rec)["detail"])

	rec = do(srv.Handler(), httptest.NewRequest(http.MethodTrace, "/known", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Equal(t, httpx.ProblemContentType, rec.Header().Get("Content-Type"))
	assert.Equal(t, "GET, HEAD, POST", rec.Header().Get("Allow"))
	m := decode(t, rec)
	assert.Equal(t, float64(405), m["status"])
	assert.Equal(t, "TRACE is not allowed on /known", m["detail"])
}
