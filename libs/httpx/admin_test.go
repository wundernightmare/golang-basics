package httpx_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// admin serves one admin request and returns the status and the decoded body.
func admin(t testing.TB, srv *httpx.Server, method, target string, headers ...string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := do(srv.Admin(), req)
	var body map[string]any
	if rec.Body.Len() > 0 && strings.Contains(rec.Header().Get("Content-Type"), "json") {
		body = decode(t, rec)
	}
	return rec.Code, body
}

func TestLogLevel_PutChangesWhatTheLoggerEmits(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{})
	log := srv.Logger()

	log.Debug("before")
	code, body := admin(t, srv, http.MethodPut, "/admin/log-level?level=debug&ttl=1h")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "debug", body["level"])
	assert.Equal(t, "info", body["previous"])
	assert.Equal(t, "info", body["base"])
	assert.NotNil(t, body["expires_at"])
	log.Debug("after")

	assert.Nil(t, buf.Find(t, map[string]any{"msg": "before"}), "debug was off")
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "after"}), "debug is on")
	change := buf.Find(t, map[string]any{"msg": "log level changed"})
	require.NotNil(t, change, "the change itself is logged")
	assert.Equal(t, "WARN", change["level"])
	assert.Equal(t, "info", change["from"])
	assert.Equal(t, "debug", change["to"])
	assert.Equal(t, "1h0m0s", change["ttl"])

	code, body = admin(t, srv, http.MethodGet, "/admin/log-level")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "debug", body["level"])
}

func TestLogLevel_RevertsAfterTTL(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{})
	code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=debug&ttl=20ms")
	require.Equal(t, http.StatusOK, code)
	level, expires := srv.LogLevel.Level()
	assert.Equal(t, slog.LevelDebug, level)
	assert.False(t, expires.IsZero())

	require.Eventually(t, func() bool {
		l, _ := srv.LogLevel.Level()
		return l == slog.LevelInfo
	}, 2*time.Second, 5*time.Millisecond, "the level must revert on its own")
	_, expires = srv.LogLevel.Level()
	assert.True(t, expires.IsZero())
	srv.Logger().Debug("late")
	assert.Nil(t, buf.Find(t, map[string]any{"msg": "late"}))
}

func TestLogLevel_NewerChangeIsNotRevertedByAnOlderTimer(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	srv.LogLevel.Set(slog.LevelDebug, 20*time.Millisecond)
	srv.LogLevel.Set(slog.LevelWarn, time.Hour)
	time.Sleep(60 * time.Millisecond)
	level, _ := srv.LogLevel.Level()
	assert.Equal(t, slog.LevelWarn, level, "the first change's timer must not undo the second")
}

func TestLogLevel_TTLIsCappedAndDefaulted(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{LogLevelMaxTTL: time.Minute})

	// No ttl → the cap; an over-long ttl → the cap. Either way it expires.
	for _, target := range []string{"/admin/log-level?level=debug", "/admin/log-level?level=debug&ttl=48h"} {
		code, body := admin(t, srv, http.MethodPut, target)
		require.Equal(t, http.StatusOK, code, target)
		assert.Equal(t, "1m0s", body["max_ttl"])
		raw, ok := body["expires_at"].(string)
		require.True(t, ok, target)
		exp, err := time.Parse(time.RFC3339Nano, raw)
		require.NoError(t, err)
		assert.WithinDuration(t, exp, time.Now().Add(time.Minute), 5*time.Second, target)
	}
}

func TestLogLevel_DeleteResetsToBaseNow(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{})
	code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=error&ttl=1h")
	require.Equal(t, http.StatusOK, code)

	code, body := admin(t, srv, http.MethodDelete, "/admin/log-level")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "info", body["level"])
	assert.Equal(t, "error", body["previous"])
	assert.Nil(t, body["expires_at"])
	// Logged even though the level was error when the reset happened.
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "log level reset"}))
}

func TestLogLevel_RejectsBadInput(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	for _, target := range []string{
		"/admin/log-level",                   // no level
		"/admin/log-level?level=verbose",     // not a level
		"/admin/log-level?level=debug&ttl=0", // ttl must be positive
		"/admin/log-level?level=debug&ttl=-1m",
		"/admin/log-level?level=debug&ttl=soon",
	} {
		code, body := admin(t, srv, http.MethodPut, target)
		assert.Equal(t, http.StatusBadRequest, code, target)
		assert.Equal(t, float64(400), body["status"], "problem+json body")
	}
	level, _ := srv.LogLevel.Level()
	assert.Equal(t, slog.LevelInfo, level, "a rejected request changes nothing")
}

func TestLogLevel_NotAvailableForForeignLogger(t *testing.T) {
	srv, err := httpx.NewServer(httpx.Config{Service: "test", Addr: ":0"}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	assert.Nil(t, srv.LogLevel)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		code, body := admin(t, srv, method, "/admin/log-level?level=debug")
		assert.Equal(t, http.StatusNotImplemented, code, method)
		assert.Equal(t, float64(501), body["status"])
	}
}

func TestAdminToken_GuardsAdminAndDebugButNotProbes(t *testing.T) {
	srv, buf := loggedServer(t, httpx.Config{Addr: ":0", AdminAddr: ":0", AdminToken: "s3cret"})

	for _, path := range []string{"/healthz", "/livez", "/version", "/metrics"} {
		code, _ := admin(t, srv, http.MethodGet, path)
		assert.Equal(t, http.StatusOK, code, "%s is open", path)
	}

	rejected := [][]string{
		nil,                                    // no header
		{"Authorization", "Basic czNjcmV0"},    // wrong scheme
		{"Authorization", "Bearer nope"},       // wrong token
		{"Authorization", "Bearer s3cret-ish"}, // a prefix match is not a match
		{"Authorization", "Bearer"},            // no token at all
	}
	for _, hdr := range rejected {
		for _, tc := range []struct{ method, target string }{
			{http.MethodGet, "/admin/config"},
			{http.MethodGet, "/admin/log-level"},
			{http.MethodPut, "/admin/log-level?level=debug"},
			{http.MethodDelete, "/admin/log-level"},
			{http.MethodGet, "/debug/pprof/"},
			{http.MethodGet, "/debug/pprof/cmdline"},
		} {
			code, body := admin(t, srv, tc.method, tc.target, hdr...)
			assert.Equal(t, http.StatusUnauthorized, code, "%s %s %v", tc.method, tc.target, hdr)
			assert.Equal(t, float64(401), body["status"])
		}
	}
	level, _ := srv.LogLevel.Level()
	assert.Equal(t, slog.LevelInfo, level, "no rejected mutation took effect")
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "admin: rejected unauthenticated request"}))

	for _, scheme := range []string{"Bearer", "bearer", "BEARER"} {
		code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=debug", "Authorization", scheme+" s3cret")
		assert.Equal(t, http.StatusOK, code, "scheme %q is accepted", scheme)
	}
	code, _ := admin(t, srv, http.MethodGet, "/debug/pprof/", "Authorization", "Bearer s3cret")
	assert.Equal(t, http.StatusOK, code)
}

func TestAdminToken_EmptyMeansOpenOnLoopback(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0"})
	code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=warn")
	assert.Equal(t, http.StatusOK, code, "no token configured → no guard (loopback default)")
}

func TestAdminConfig_ShowsRedactedConfig(t *testing.T) {
	// Default: the effective httpx.Config itself, with its tokens hidden and
	// the defaults applied.
	srv, _ := loggedServer(t, httpx.Config{Addr: ":1234", AdminAddr: ":0", AdminToken: "s3cret", DebugToken: "dbg"})
	code, body := admin(t, srv, http.MethodGet, "/admin/config", "Authorization", "Bearer s3cret")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, ":1234", body["http_addr"], "named by the yaml tag")
	assert.Equal(t, "[redacted]", body["admin_token"])
	assert.Equal(t, "[redacted]", body["debug_token"])
	assert.Equal(t, "24h0m0s", body["log_level_max_ttl"], "durations read as durations, defaults applied")
	assert.Equal(t, "30s", body["read_timeout"])

	// WithConfig: the service's own struct, same treatment.
	type svcConfig struct {
		DatabaseURL string `yaml:"database_url"`
		APIKey      string `yaml:"api_key"`
		Workers     int    `yaml:"workers"`
	}
	srv, _ = loggedServer(t, httpx.Config{},
		httpx.WithConfig(svcConfig{DatabaseURL: "postgres://app:hunter2@db:5432/app", APIKey: "k", Workers: 4}))
	code, body = admin(t, srv, http.MethodGet, "/admin/config")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "postgres://app:xxxxx@db:5432/app", body["database_url"])
	assert.Equal(t, "[redacted]", body["api_key"])
	assert.Equal(t, float64(4), body["workers"])
	assert.NotContains(t, body, "http_addr", "the service config replaces the default view")
}

func TestVersion_CarriesBuildStartTimeAndUptime(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	code, body := admin(t, srv, http.MethodGet, "/version")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "test", body["service"])
	assert.Equal(t, httpx.Version, body["version"])
	assert.NotEmpty(t, body["go_version"])
	assert.NotEmpty(t, body["revision"])
	raw, ok := body["started_at"].(string)
	require.True(t, ok)
	started, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), started, 5*time.Second)
	assert.GreaterOrEqual(t, body["uptime_seconds"], float64(0))
}

func TestAdmin_PprofOnAdminOnly(t *testing.T) {
	srv, _ := loggedServer(t, httpx.Config{})
	rec := get(srv.Admin(), "/debug/pprof/")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "goroutine")

	for _, path := range []string{"/debug/pprof/", "/metrics", "/healthz", "/admin/config"} {
		rec = get(srv.Handler(), path)
		assert.Equal(t, http.StatusNotFound, rec.Code, "%s must not leak onto the API port", path)
	}
}

func TestAdminRun_AnnouncesAuthMode(t *testing.T) {
	for _, tc := range []struct {
		token, want string
	}{{"", "off"}, {"s3cret", "bearer"}} {
		t.Run(tc.want, func(t *testing.T) {
			srv, buf := loggedServer(t, httpx.Config{AdminAddr: "127.0.0.1:0", AdminToken: tc.token, ShutdownTimeout: time.Second})
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- srv.Run(ctx) }()
			require.Eventually(t, func() bool {
				return strings.Contains(buf.String(), "admin server listening")
			}, 3*time.Second, 10*time.Millisecond)
			assert.Empty(t, srv.ListenAddr(), "a worker has no API listener")
			assert.NotEmpty(t, srv.AdminListenAddr())
			cancel()
			require.NoError(t, <-done)
			line := buf.Find(t, map[string]any{"msg": "admin server listening"})
			require.NotNil(t, line)
			assert.Equal(t, tc.want, line["auth"], "the admin auth mode is said out loud")
		})
	}
}
