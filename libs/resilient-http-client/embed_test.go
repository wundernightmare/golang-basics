package resilient_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
)

// serviceConfig is how a service carries its outbound HTTP targets: the
// lib's Config under a key of the service's config file, loaded by the
// workspace's one loader (httpx.LoadYAML) next to everything else.
type serviceConfig struct {
	httpx.Config `yaml:",inline"`

	HTTPClient resilient.Config `yaml:"http_client"`
}

// loadService loads yaml the way a service does. The transport defaults are
// preset before the call — the loader's rule for a default that is not a
// struct tag: what dst already holds is the default, the file replaces it.
func loadService(t *testing.T, yaml string, environ map[string]string) (serviceConfig, error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	if environ == nil {
		environ = map[string]string{}
	}

	cfg := serviceConfig{HTTPClient: resilient.DefaultConfig()}
	err := httpx.LoadYAMLFrom(path, "SVC_", environ, &cfg)

	return cfg, err //nolint:wrapcheck // the error under test
}

func TestEmbedded_LoadsLikeLoadConfig(t *testing.T) {
	t.Parallel()

	const clientYAML = `
max_idle_conns_per_host: 8
http2_ping_interval: 0s
targets:
  - name: billing
    timeout: 2s
    rate_limit: 2.5
    breaker_failure_ratio: 0
  - name: search
`

	want, err := resilient.LoadConfig([]byte(clientYAML))
	require.NoError(t, err)

	cfg, err := loadService(t, `
http_addr: ":7000"
http_client:
  max_idle_conns_per_host: 8
  http2_ping_interval: 0s
  targets:
    - name: billing
      timeout: 2s
      rate_limit: 2.5
      breaker_failure_ratio: 0
    - name: search
`, map[string]string{"SVC_ADMIN_ADDR": ":7001"})
	require.NoError(t, err)

	assert.Equal(t, want, cfg.HTTPClient, "embedded in a service config, the same keys mean the same as in LoadConfig")
	assert.Equal(t, 100, cfg.HTTPClient.MaxIdleConns, "an omitted transport key keeps its (preset) default")
	assert.Zero(t, cfg.HTTPClient.HTTP2PingInterval, "an explicit 0 disables")
	assert.Equal(t, ":7000", cfg.Addr, "the rest of the file loads as before")
	assert.Equal(t, ":7001", cfg.AdminAddr, "and so does the environment overlay")
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout, "and the tag defaults")

	client, err := resilient.New(cfg.HTTPClient)
	require.NoError(t, err)
	require.NoError(t, client.Shutdown(t.Context()))
}

func TestEmbedded_NoKeyIsTheDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, "http_addr: \":7000\"\n", nil)
	require.NoError(t, err)
	assert.Equal(t, resilient.DefaultConfig(), cfg.HTTPClient)
	require.NoError(t, cfg.HTTPClient.Validate())
}

// Without the preset the transport keys have no default at all: they are not
// struct tags, so the loader has nothing to fill them from.
func TestEmbedded_WithoutThePresetTransportDefaultsAreZero(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("http_client:\n  targets:\n    - name: a\n"), 0o600))

	var cfg serviceConfig
	require.NoError(t, httpx.LoadYAMLFrom(path, "SVC_", map[string]string{}, &cfg))

	assert.Zero(t, cfg.HTTPClient.MaxIdleConnsPerHost, "net/http's 2 per host, not the documented 32")
	assert.Equal(t, resilient.DefaultTarget("a"), cfg.HTTPClient.Targets[0], "targets default themselves either way")
}

func TestEmbedded_UnknownKeysAreRejected(t *testing.T) {
	t.Parallel()

	_, err := loadService(t, "http_client:\n  targets:\n    - name: a\n      retry_max_atempts: 3\n", nil)
	require.ErrorContains(t, err, "retry_max_atempts", "a typo in a target is an error, as everywhere else in the file")

	_, err = loadService(t, "http_client:\n  max_idle_cons: 1\n", nil)
	require.ErrorContains(t, err, "max_idle_cons")
}

// The loader parses; it does not validate. A service calls Validate on the
// embedded config in its own Validate (resilient.New does it too, as a
// backstop).
func TestEmbedded_ValidationIsTheServicesCall(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, "http_client:\n  max_redirects: -1\n  targets:\n    - name: a\n      timeout: -1s\n", nil)
	require.NoError(t, err, "LoadYAML parses a config that is not valid")

	err = cfg.HTTPClient.Validate()
	require.ErrorContains(t, err, "max_redirects")
	require.ErrorContains(t, err, "timeouts must not be negative")

	_, err = resilient.New(cfg.HTTPClient)
	require.Error(t, err)
}

// The client's keys have no environment form (no env tags; targets are a
// list): the environment overlays the service's own scalar keys only.
func TestEmbedded_EnvironmentDoesNotReachTheClient(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, "http_client:\n  targets:\n    - name: a\n", map[string]string{
		"SVC_MAX_IDLE_CONNS":                "1",
		"SVC_HTTP_CLIENT_MAX_IDLE_CONNS":    "1",
		"SVC_HTTP_CLIENT_TARGETS_0_TIMEOUT": "1s",
		"SVC_TIMEOUT":                       "1s",
	})
	require.NoError(t, err)
	assert.Equal(t, resilient.DefaultConfig("a"), cfg.HTTPClient)
}
