package httpx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// spanView is what these tests read from the server span.
type spanView struct {
	name        string
	attrs       map[string]any
	status      codes.Code
	description string
}

// tracedServer returns a server and a reader of the span that ended last.
func tracedServer(t *testing.T) (*httpx.Server, func() spanView) {
	t.Helper()

	spans := testx.Recorder(t)
	srv, _ := loggedServer(t, httpx.Config{TrustedProxies: []string{"192.0.2.0/24"}},
		httpx.WithTracerProvider(spans.Provider), httpx.WithPropagator(spans.Propagator))

	return srv, func() spanView {
		ended := spans.Ended()
		require.NotEmpty(t, ended)

		span := ended[len(ended)-1]
		view := spanView{name: span.Name(), attrs: map[string]any{}, status: span.Status().Code, description: span.Status().Description}

		for _, attr := range span.Attributes() {
			view.attrs[string(attr.Key)] = attr.Value.AsInterface()
		}

		return view
	}
}

// The server span says what the request was — and nothing it was not: an
// attribute the request does not have is absent, not empty.
func TestServerSpan_DescribesTheRequest(t *testing.T) {
	t.Parallel()

	srv, last := tracedServer(t)
	srv.Mux().HandleFunc("/items/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	req := httptest.NewRequest("BREW", "/items/7?page=2", http.NoBody)
	req.Header.Set("User-Agent", "kettle/1")
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	do(srv.Handler(), req)

	span := last()
	attrs := span.attrs
	assert.Equal(t, "_OTHER /items/{id}", span.name, "method and route, never the raw path")
	assert.Equal(t, "_OTHER", attrs["http.request.method"])
	assert.Equal(t, "BREW", attrs["http.request.method_original"])
	assert.Equal(t, "/items/{id}", attrs["http.route"])
	assert.Equal(t, "/items/7", attrs["url.path"])
	assert.Equal(t, "page=2", attrs["url.query"])
	assert.Equal(t, "http", attrs["url.scheme"])
	assert.Equal(t, "kettle/1", attrs["user_agent.original"])
	assert.Equal(t, "203.0.113.9", attrs["client.address"])
	assert.EqualValues(t, http.StatusNoContent, attrs["http.response.status_code"])
	assert.Equal(t, codes.Unset, span.status)

	do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/items/7", http.NoBody))

	span = last()
	attrs = span.attrs
	assert.Equal(t, "GET /items/{id}", span.name)
	assert.Equal(t, "GET", attrs["http.request.method"])
	assert.NotContains(t, attrs, "http.request.method_original")
	assert.NotContains(t, attrs, "url.query")
	assert.NotContains(t, attrs, "user_agent.original")

	do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/nowhere", http.NoBody))

	span = last()
	assert.Equal(t, "GET", span.name, "no route matched: the method alone")
	assert.NotContains(t, span.attrs, "http.route")
}

// 500 is where a response becomes the server's failure: the span is an error
// from exactly there, and 4xx — the caller's mistake — is not.
func TestServerSpan_IsAnErrorFrom500(t *testing.T) {
	t.Parallel()

	srv, last := tracedServer(t)
	srv.Mux().HandleFunc("GET /status/{code}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("code") {
		case "499":
			w.WriteHeader(499)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/status/499", http.NoBody))
	assert.Equal(t, codes.Unset, last().status)

	do(srv.Handler(), httptest.NewRequest(http.MethodGet, "/status/500", http.NoBody))
	assert.Equal(t, codes.Error, last().status)
	assert.Equal(t, "Internal Server Error", last().description)
}

// A handler that panics after it started the response: the status is on the
// wire, and a problem document appended to its body would corrupt it.
func TestPanic_AfterTheResponseStartedAddsNothing(t *testing.T) {
	t.Parallel()

	srv, logs := loggedServer(t, httpx.Config{})
	srv.Mux().HandleFunc("GET /half", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("partial"))

		panic("late")
	})

	rec := get(srv.Handler(), "/half")
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "partial", rec.Body.String())
	require.NotNil(t, logs.Find(t, map[string]any{"msg": "panic in handler"}))
}

// Without HTTP_REQUEST_TIMEOUT and without the header a request has no
// deadline at all — not a deadline of zero.
func TestRequestWithoutABudgetHasNoDeadline(t *testing.T) {
	t.Parallel()

	srv, logs := loggedServer(t, httpx.Config{})

	var (
		hasDeadline bool
		ctxErr      error
	)

	srv.Mux().HandleFunc("GET /free", func(_ http.ResponseWriter, r *http.Request) {
		_, hasDeadline = r.Context().Deadline()
		ctxErr = r.Context().Err()

		time.Sleep(time.Millisecond) // a latency the access log cannot round to zero
	})

	rec := get(srv.Handler(), "/free")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, hasDeadline)
	require.NoError(t, ctxErr)

	access := logs.Find(t, map[string]any{"msg": "request"})
	require.NotNil(t, access)
	assert.GreaterOrEqual(t, access["latency_ms"], float64(1), "milliseconds: the handler slept for one")
	assert.Less(t, access["latency_ms"], float64(1000), "and nowhere near a second")
}
