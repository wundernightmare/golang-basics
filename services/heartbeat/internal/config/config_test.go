package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/heartbeat/internal/config"
)

// environ returns kv as a HEARTBEAT_-prefixed environment: exactly these
// variables (never nil, so never the process environment). A key mapped to
// "" is set-to-empty; a key left out is unset.
func environ(kv map[string]string) map[string]string {
	out := make(map[string]string, len(kv))
	for k, v := range kv {
		out[config.Prefix+k] = v
	}

	return out
}

func TestLoad_Defaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(environ(map[string]string{"ADMIN_TOKEN": "s3cret"}))
	require.NoError(t, err)
	assert.Equal(t, "heartbeat", cfg.Service)
	assert.Equal(t, ":9081", cfg.AdminAddr, "off ping's default admin port")
	assert.Empty(t, cfg.Addr, "a worker has no API listener")
	assert.Equal(t, 5*time.Second, cfg.Interval)
	// The shared keys come with the libs/httpx defaults.
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, 2*time.Second, cfg.ShutdownDelay)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.Equal(t, 100, cfg.LogSampleInitial)
	assert.Equal(t, 10*time.Second, cfg.HealthInterval)
}

//nolint:paralleltest // exercises the process-environment path; t.Setenv in a loop, which paralleltest does not see
func TestLoad_Overrides(t *testing.T) {
	for k, v := range map[string]string{
		"ADMIN_ADDR":            "127.0.0.1:9999",
		"HTTP_ADDR":             ":8080", // ignored: no API listener
		"INTERVAL":              "250ms",
		"LOG_LEVEL":             "debug",
		"LOG_FORMAT":            "text",
		"HTTP_SHUTDOWN_DELAY":   "0s",
		"HTTP_SHUTDOWN_TIMEOUT": "3s",
	} {
		t.Setenv(config.Prefix+k, v)
	}

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9999", cfg.AdminAddr)
	assert.Empty(t, cfg.Addr)
	assert.Equal(t, 250*time.Millisecond, cfg.Interval)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "text", cfg.LogFormat)
	assert.Equal(t, time.Duration(0), cfg.ShutdownDelay)
	assert.Equal(t, 3*time.Second, cfg.ShutdownTimeout)
}

func TestLoad_Rejects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"open admin on every interface", map[string]string{}, "ADMIN_TOKEN is empty"},
		{"unparsable interval", map[string]string{"ADMIN_INSECURE": "true", "INTERVAL": "often"}, "parse config"},
		{"non-positive interval", map[string]string{"ADMIN_INSECURE": "true", "INTERVAL": "0s"}, "INTERVAL must be positive"},
		{"unknown log level", map[string]string{"ADMIN_INSECURE": "true", "LOG_LEVEL": "loud"}, "LOG_LEVEL"},
		{"no admin listener either", map[string]string{"ADMIN_ADDR": ""}, "neither HTTP_ADDR nor ADMIN_ADDR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := config.LoadFrom(environ(tc.env))
			require.ErrorContains(t, err, tc.want)
		})
	}

	_, err := config.LoadFrom(environ(map[string]string{"ADMIN_INSECURE": "true"}))
	require.NoError(t, err, "an explicitly insecure admin listener is accepted")
}

func TestHTTP_IsTheEmbeddedConfig(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(environ(map[string]string{"ADMIN_TOKEN": "s3cret", "DEBUG_TOKEN": "dbg"}))
	require.NoError(t, err)

	h := cfg.HTTP()
	assert.Equal(t, cfg.Config, h, "a projection, not a hand copy")
	assert.Equal(t, "s3cret", h.AdminToken)
	assert.Equal(t, "dbg", h.DebugToken)
}
