//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What a real process shows and nothing a lower layer already asserts: the
// binary boots from its env, binds what the harness told it to, answers on
// both listeners, reports its build stamp, guards its admin surface and
// shuts down cleanly on SIGTERM. Handler logic, problem bodies and the admin
// endpoints' semantics are covered in libs/httpx and each service's tests.

func pingSpec() spec { return spec{name: "ping", prefix: "PING_", api: true} }

func heartbeatSpec() spec {
	// A fast tick so the counter assertion does not wait on the 5s default.
	return spec{name: "heartbeat", prefix: "HEARTBEAT_", env: map[string]string{"INTERVAL": "100ms"}}
}

func TestPing(t *testing.T) {
	t.Parallel()
	s := start(t, pingSpec())

	t.Run("ready on the admin port", func(t *testing.T) {
		t.Parallel()
		code, body, err := s.get(s.adminURL+"/readyz", "")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, code, "%s", body)
	})

	t.Run("version carries the build stamp", func(t *testing.T) {
		t.Parallel()
		assertBuildStamp(t, s)
	})

	t.Run("echoes msg", func(t *testing.T) {
		t.Parallel()
		code, body, err := s.get(s.apiURL+"/ping?msg=x", "")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code, "%s", body)
		var pong struct {
			Message string `json:"message"`
			Echo    string `json:"echo"`
		}
		require.NoError(t, json.Unmarshal(body, &pong), "%s", body)
		assert.Equal(t, "pong", pong.Message)
		assert.Equal(t, "x", pong.Echo)
	})

	t.Run("admin surface is not on the API port", func(t *testing.T) {
		t.Parallel()
		code, _, err := s.get(s.apiURL+"/metrics", "")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, code)
	})

	t.Run("admin config requires the token", func(t *testing.T) {
		t.Parallel()
		assertAdminGuarded(t, s)
	})
}

func TestHeartbeat(t *testing.T) {
	t.Parallel()
	s := start(t, heartbeatSpec())

	t.Run("version carries the build stamp", func(t *testing.T) {
		t.Parallel()
		assertBuildStamp(t, s)
	})

	t.Run("beats counter increases", func(t *testing.T) {
		t.Parallel()
		first := s.metric(t, "heartbeat_beats_total")
		eventually(t, 10*time.Second, "heartbeat_beats_total never advanced", func(c *assert.CollectT) {
			assert.Greater(c, s.metric(t, "heartbeat_beats_total"), first)
		})
	})

	t.Run("admin config requires the token", func(t *testing.T) {
		t.Parallel()
		assertAdminGuarded(t, s)
	})
}

// TestGracefulShutdown: SIGTERM → exit 0 within a few seconds, and the log
// ends by saying the servers stopped cleanly.
func TestGracefulShutdown(t *testing.T) {
	t.Parallel()
	for _, sp := range []spec{pingSpec(), heartbeatSpec()} {
		t.Run(sp.name, func(t *testing.T) {
			t.Parallel()
			s := start(t, sp)
			require.True(t, s.terminate(5*time.Second), "%s did not exit within 5s of SIGTERM", sp.name)
			assertCleanExit(t, s)
		})
	}
}

// assertCleanExit checks an exited process: status 0, nothing at error
// level, "servers stopped cleanly" logged, and the last line a component's
// own "… stopped" — the shutdown of the HTTP server and of the worker loops
// interleave (a consumer's partitions are revoked while the server drains),
// so only the final line is required to be a clean stop.
func assertCleanExit(t *testing.T, s *service) {
	t.Helper()
	require.Equal(t, 0, s.exitCode(), "%s exit status (%v)", s.name, s.err)
	lines := s.logLines()
	require.NotEmpty(t, lines, "%s logged nothing", s.name)
	stopped := false
	for _, l := range lines {
		assert.NotEqual(t, "ERROR", l["level"], "%s logged an error: %v", s.name, l)
		if l["msg"] == "servers stopped cleanly" {
			stopped = true
		}
	}
	require.True(t, stopped, "%s never logged \"servers stopped cleanly\"", s.name)
	last, _ := lines[len(lines)-1]["msg"].(string)
	assert.Contains(t, last, "stopped", "%s's last log line is not a clean stop", s.name)
}

// assertBuildStamp checks the /version fields a release depends on are set.
func assertBuildStamp(t *testing.T, s *service) {
	t.Helper()
	bi := s.version(t)
	assert.Equal(t, s.name, bi.Service)
	assert.NotEmpty(t, bi.Version, "version")
	assert.NotEmpty(t, bi.Revision, "revision")
	assert.NotEmpty(t, bi.GoVersion, "go_version")
	assert.NotEmpty(t, bi.StartedAt, "started_at")
	if assert.NotNil(t, bi.UptimeSeconds, "uptime_seconds") {
		assert.GreaterOrEqual(t, *bi.UptimeSeconds, int64(0))
	}
}

// assertAdminGuarded: the harness starts every service with an ADMIN_TOKEN,
// so /admin/config refuses a missing or wrong bearer token and serves the
// right one — without echoing the token back.
func assertAdminGuarded(t *testing.T, s *service) {
	t.Helper()
	url := s.adminURL + "/admin/config"

	code, _, err := s.get(url, "")
	require.NoError(t, err)
	assert.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, code, "no token")

	code, _, err = s.get(url, "not-"+s.token)
	require.NoError(t, err)
	assert.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, code, "wrong token")

	code, body, err := s.get(url, s.token)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, code, "%s", body)
	assert.NotContains(t, string(body), s.token, "/admin/config leaks the admin token")
}
