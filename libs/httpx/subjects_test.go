package httpx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// One tenant under investigation, on a virtual clock. Its requests are logged
// at debug level — past the info level and the sampler — and their span is
// marked for the tail sampler; everyone else's are not. The switch turns
// itself off when its ttl is over.
func TestDebugSubject_OneTenantGetsDebugLogsAndAMarkedTrace(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		logs := &testx.LogBuffer{}
		spans := testx.Recorder(t)

		srv, err := httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0", LogLevelMaxTTL: time.Hour},
			httpx.NewLogger(httpx.LogConfig{Writer: logs, Level: "info"}),
			httpx.WithTracerProvider(spans.Provider), httpx.WithPropagator(spans.Propagator))
		require.NoError(t, err)

		// The service's side: it knows the tenant (here from a header, in
		// life from its authenticator) and tells the server.
		srv.Mux().HandleFunc("GET /orders", func(w http.ResponseWriter, r *http.Request) {
			ctx := srv.DebugSubject(r.Context(), r.Header.Get("X-Tenant"))
			srv.Logger().DebugContext(ctx, "pricing rules evaluated", "tenant", r.Header.Get("X-Tenant"))
			w.WriteHeader(http.StatusNoContent)
		})

		call := func(tenant string) {
			req := httptest.NewRequest(http.MethodGet, "/orders", http.NoBody)
			req.Header.Set("X-Tenant", tenant)
			srv.Handler().ServeHTTP(httptest.NewRecorder(), req)
		}
		admin := func(method, target string) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			srv.Admin().ServeHTTP(rec, httptest.NewRequest(method, target, http.NoBody))

			return rec
		}
		debugLines := func(tenant string) int {
			n := 0

			for _, line := range logs.Lines(t) {
				if line["msg"] == "pricing rules evaluated" && line["tenant"] == tenant {
					n++
				}
			}

			return n
		}
		marked := func() []string {
			var subjects []string

			for _, span := range spans.Ended() {
				for _, attr := range span.Attributes() {
					if string(attr.Key) == httpx.DebugSubjectAttribute {
						subjects = append(subjects, attr.Value.AsString())
					}
				}
			}

			return subjects
		}

		call("acme")
		require.Zero(t, debugLines("acme"), "at info level a debug record is not written")

		require.Equal(t, http.StatusBadRequest, admin(http.MethodPut, "/admin/debug-subjects/acme").Code, "a ttl is required")
		require.Equal(t, http.StatusOK, admin(http.MethodPut, "/admin/debug-subjects/acme?ttl=15m").Code)
		assert.Contains(t, admin(http.MethodGet, "/admin/debug-subjects").Body.String(), `"subject":"acme"`)

		call("acme")
		call("acme")
		call("globex")

		assert.Equal(t, 2, debugLines("acme"), "the tenant under investigation is logged at debug")
		assert.Zero(t, debugLines("globex"), "nobody else is")
		assert.Equal(t, []string{"acme", "acme"}, marked(), "and only its spans carry the attribute the tail sampler keeps")

		time.Sleep(15 * time.Minute) // the ttl, on the bubble's clock

		call("acme")
		assert.Equal(t, 2, debugLines("acme"), "the switch turned itself off")
		assert.NotContains(t, admin(http.MethodGet, "/admin/debug-subjects").Body.String(), "acme")

		// Capped: a ttl beyond LOG_LEVEL_MAX_TTL is cut to it; DELETE ends it now.
		require.Equal(t, http.StatusOK, admin(http.MethodPut, "/admin/debug-subjects/acme?ttl=48h").Code)
		time.Sleep(61 * time.Minute)
		call("acme")
		assert.Equal(t, 2, debugLines("acme"), "an hour is the most this server allows")

		require.Equal(t, http.StatusOK, admin(http.MethodPut, "/admin/debug-subjects/acme?ttl=5m").Code)
		require.Equal(t, http.StatusOK, admin(http.MethodDelete, "/admin/debug-subjects/acme").Code)
		call("acme")
		assert.Equal(t, 2, debugLines("acme"))
	})
}

// The list is a debugging switch, not a store: it is bounded, and a subject
// that is not under investigation costs a map lookup and nothing else.
func TestDebugSubject_BoundedAndCheapWhenUnused(t *testing.T) {
	t.Parallel()

	srv, err := httpx.NewServer(httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0"}, nil)
	require.NoError(t, err)

	put := func(subject string) int {
		rec := httptest.NewRecorder()
		srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/admin/debug-subjects/"+subject+"?ttl=1m", http.NoBody))

		return rec.Code
	}

	for i := range 100 {
		require.Equal(t, http.StatusOK, put("tenant-"+string(rune('a'+i%26))+string(rune('a'+i/26))))
	}

	assert.Equal(t, http.StatusConflict, put("one-too-many"))
	assert.Equal(t, http.StatusOK, put("tenant-aa"), "extending a subject already listed is not a new entry")

	ctx := context.Background()
	assert.Equal(t, ctx, srv.DebugSubject(ctx, "nobody"), "an uninvestigated subject gets its context back untouched")
	assert.Equal(t, ctx, srv.DebugSubject(ctx, ""))
	assert.False(t, httpx.DebugLogging(srv.DebugSubject(ctx, "nobody")))
	assert.True(t, httpx.DebugLogging(srv.DebugSubject(ctx, "tenant-aa")))
}
