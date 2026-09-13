package httpx_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// adminServer builds a server whose logger writes JSON into buf, so tests
// can assert both the admin responses and what the change did to the log.
func adminServer(t testing.TB, cfg httpx.Config) (*httpx.Server, *testx.LogBuffer) {
	t.Helper()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
	cfg.Service = "test"
	return httpx.NewServer(cfg, log), buf
}

func admin(t testing.TB, srv *httpx.Server, method, target string, headers ...string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	srv.Admin().ServeHTTP(rec, req)
	var body map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	}
	return rec.Code, body
}

func TestLogLevel_PutChangesWhatTheLoggerEmits(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := adminServer(t, httpx.Config{})
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
	}, "httpx", "unit")
}

func TestLogLevel_RevertsAfterTTL(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := adminServer(t, httpx.Config{})
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
	}, "httpx", "unit")
}

func TestLogLevel_TTLIsCappedAndDefaulted(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := adminServer(t, httpx.Config{LogLevelMaxTTL: time.Minute})

		// No ttl → the cap; an over-long ttl → the cap. Either way it expires.
		for _, target := range []string{"/admin/log-level?level=debug", "/admin/log-level?level=debug&ttl=48h"} {
			code, body := admin(t, srv, http.MethodPut, target)
			require.Equal(t, http.StatusOK, code, target)
			assert.Equal(t, "1m0s", body["max_ttl"])
			exp, err := time.Parse(time.RFC3339Nano, body["expires_at"].(string))
			require.NoError(t, err)
			assert.WithinDuration(t, exp, time.Now().Add(time.Minute), 5*time.Second, target)
		}
	}, "httpx", "unit")
}

func TestLogLevel_DeleteResetsToBaseNow(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := adminServer(t, httpx.Config{})
		code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=error&ttl=1h")
		require.Equal(t, http.StatusOK, code)

		code, body := admin(t, srv, http.MethodDelete, "/admin/log-level")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, "info", body["level"])
		assert.Equal(t, "error", body["previous"])
		assert.Nil(t, body["expires_at"])
		// Logged even though the level was error when the reset happened.
		assert.NotNil(t, buf.Find(t, map[string]any{"msg": "log level reset"}))
	}, "httpx", "unit")
}

func TestLogLevel_RejectsBadInput(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := adminServer(t, httpx.Config{})
		for _, target := range []string{
			"/admin/log-level",                   // no level
			"/admin/log-level?level=verbose",     // not a level
			"/admin/log-level?level=debug&ttl=0", // ttl must be positive
			"/admin/log-level?level=debug&ttl=soon",
		} {
			code, body := admin(t, srv, http.MethodPut, target)
			assert.Equal(t, http.StatusBadRequest, code, target)
			assert.Equal(t, float64(400), body["status"], "problem+json body")
		}
		level, _ := srv.LogLevel.Level()
		assert.Equal(t, slog.LevelInfo, level, "a rejected request changes nothing")
	}, "httpx", "unit")
}

func TestLogLevel_NotAvailableForForeignLogger(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		log := slog.New(slog.DiscardHandler)
		srv := httpx.NewServer(httpx.Config{Service: "test"}, log)
		assert.Nil(t, srv.LogLevel)
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			code, _ := admin(t, srv, method, "/admin/log-level?level=debug")
			assert.Equal(t, http.StatusNotImplemented, code, method)
		}
	}, "httpx", "unit")
}

func TestAdminToken_GuardsMutationsOnly(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, buf := adminServer(t, httpx.Config{AdminToken: "s3cret"})

		// Reads and probes stay open.
		for _, path := range []string{"/admin/log-level", "/admin/config", "/healthz", "/livez", "/version", "/metrics"} {
			rec := httptest.NewRecorder()
			srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			assert.Equal(t, http.StatusOK, rec.Code, path)
		}

		// Mutations: no header, wrong scheme, wrong token → 401 and nothing changes.
		for _, hdr := range [][]string{nil, {"Authorization", "Basic czNjcmV0"}, {"Authorization", "Bearer nope"}} {
			code, body := admin(t, srv, http.MethodPut, "/admin/log-level?level=debug", hdr...)
			assert.Equal(t, http.StatusUnauthorized, code, hdr)
			assert.Equal(t, float64(401), body["status"])
			code, _ = admin(t, srv, http.MethodDelete, "/admin/log-level", hdr...)
			assert.Equal(t, http.StatusUnauthorized, code, hdr)
		}
		level, _ := srv.LogLevel.Level()
		assert.Equal(t, slog.LevelInfo, level)
		assert.NotNil(t, buf.Find(t, map[string]any{"msg": "admin: rejected unauthenticated mutation"}))

		code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=debug", "Authorization", "Bearer s3cret")
		assert.Equal(t, http.StatusOK, code)
	}, "httpx", "unit")
}

func TestAdminToken_EmptyMeansOpen(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := adminServer(t, httpx.Config{})
		code, _ := admin(t, srv, http.MethodPut, "/admin/log-level?level=warn")
		assert.Equal(t, http.StatusOK, code, "no token configured → no guard (local default)")
	}, "httpx", "unit")
}

func TestAdminConfig_ShowsRedactedConfig(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		// Default: the httpx.Config itself, with its tokens hidden.
		srv, _ := adminServer(t, httpx.Config{AdminToken: "s3cret", DebugToken: "dbg", Addr: ":1234"})
		code, body := admin(t, srv, http.MethodGet, "/admin/config")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, ":1234", body["Addr"])
		assert.Equal(t, "[redacted]", body["AdminToken"])
		assert.Equal(t, "[redacted]", body["DebugToken"])
		assert.Equal(t, "24h0m0s", body["LogLevelMaxTTL"], "durations read as durations")

		// WithConfig: the service's own struct, same treatment.
		type svcConfig struct {
			DatabaseURL string `yaml:"database_url"`
			APIKey      string `yaml:"api_key"`
			Workers     int    `yaml:"workers"`
		}
		log := httpx.NewLogger(httpx.LogConfig{Level: "error", Format: "text"})
		srv = httpx.NewServer(httpx.Config{Service: "test"}, log,
			httpx.WithConfig(svcConfig{DatabaseURL: "postgres://app:hunter2@db:5432/app", APIKey: "k", Workers: 4}))
		code, body = admin(t, srv, http.MethodGet, "/admin/config")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, "postgres://app:xxxxx@db:5432/app", body["database_url"])
		assert.Equal(t, "[redacted]", body["api_key"])
		assert.Equal(t, float64(4), body["workers"])
		assert.NotContains(t, body, "Addr", "the service config replaces the default view")
	}, "httpx", "unit")
}

func TestVersion_CarriesStartTimeAndUptime(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		srv, _ := adminServer(t, httpx.Config{})
		code, body := admin(t, srv, http.MethodGet, "/version")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, "test", body["service"])
		started, err := time.Parse(time.RFC3339Nano, body["started_at"].(string))
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), started, 5*time.Second)
		assert.GreaterOrEqual(t, body["uptime_seconds"], float64(0))
	}, "httpx", "unit")
}

func TestAdminRun_AnnouncesAuthMode(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		buf := &testx.LogBuffer{}
		log := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: buf})
		srv := httpx.NewServer(httpx.Config{Service: "t", Addr: "", AdminAddr: "127.0.0.1:19098", ShutdownTimeout: time.Second}, log)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- srv.Run(ctx) }()
		require.Eventually(t, func() bool {
			return strings.Contains(buf.String(), "admin server listening")
		}, 3*time.Second, 10*time.Millisecond)
		cancel()
		require.NoError(t, <-done)
		line := buf.Find(t, map[string]any{"msg": "admin server listening"})
		assert.Equal(t, "off", line["auth"], "an unset ADMIN_TOKEN is said out loud")
	}, "httpx", "unit")
}
