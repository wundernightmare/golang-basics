package grpcx_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// serviceConfig is how a service carries its outbound gRPC targets: the
// lib's Config under a key of the service's config file, loaded by the
// workspace's one loader (httpx.LoadYAML) next to everything else.
type serviceConfig struct {
	httpx.Config `yaml:",inline"`

	GRPC grpcx.Config `yaml:"grpc"`
}

func loadService(t *testing.T, yaml string, environ map[string]string) (serviceConfig, error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	if environ == nil {
		environ = map[string]string{}
	}

	var cfg serviceConfig

	err := httpx.LoadYAMLFrom(path, "SVC_", environ, &cfg)

	return cfg, err //nolint:wrapcheck // the error under test
}

func TestEmbedded_TargetsLoadThroughTheServiceLoader(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, `
http_addr: ":7000"
grpc:
  targets:
    - name: billing
      address: dns:///billing.internal:443
      timeout: 2s
      breaker_failure_ratio: 0
    - name: search
      address: dns:///search.internal:443
      retry_max_attempts: 0
`, map[string]string{"SVC_ADMIN_ADDR": ":7001"})
	require.NoError(t, err)

	billing := grpcx.DefaultTarget("billing", "dns:///billing.internal:443")
	billing.Timeout = 2 * time.Second
	billing.FailureRatio = 0

	search := grpcx.DefaultTarget("search", "dns:///search.internal:443")
	search.RetryMaxAttempts = 0

	assert.Equal(t, grpcx.DefaultConfig(billing, search), cfg.GRPC,
		"each target is laid over DefaultTarget: omitted keys keep their defaults, an explicit 0 disables")
	assert.Equal(t, ":7000", cfg.Addr, "the rest of the file loads as before")
	assert.Equal(t, ":7001", cfg.AdminAddr, "and so does the environment overlay")
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout, "and the tag defaults")

	require.NoError(t, cfg.GRPC.Validate())

	client, err := grpcx.New(cfg.GRPC)
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestEmbedded_NoTargetsIsValid(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, "http_addr: \":7000\"\n", nil)
	require.NoError(t, err)
	assert.Empty(t, cfg.GRPC.Targets)
	require.NoError(t, cfg.GRPC.Validate())
}

func TestEmbedded_UnknownTargetKeyIsRejected(t *testing.T) {
	t.Parallel()

	_, err := loadService(t, "grpc:\n  targets:\n    - name: a\n      address: x\n      retry_max_atempts: 3\n", nil)
	require.ErrorContains(t, err, "retry_max_atempts", "a typo in a target is an error, as everywhere else in the file")

	_, err = loadService(t, "grpc:\n  target: []\n", nil)
	require.ErrorContains(t, err, "target")
}

// The loader parses; it does not validate. A service calls Validate on the
// embedded config in its own Validate (grpcx.New does it too, as a backstop).
func TestEmbedded_ValidationIsTheServicesCall(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, "grpc:\n  targets:\n    - name: a\n      retry_max_attempts: 9\n", nil)
	require.NoError(t, err, "LoadYAML parses a config that is not valid")

	err = cfg.GRPC.Validate()
	require.ErrorContains(t, err, "address is empty")
	require.ErrorContains(t, err, "retry_max_attempts")

	_, err = grpcx.New(cfg.GRPC)
	require.Error(t, err)
}

// Targets are a list, and a list has no environment form: the environment
// overlays the service's scalar keys only. A per-environment address goes
// into that environment's config file.
func TestEmbedded_EnvironmentDoesNotReachTargets(t *testing.T) {
	t.Parallel()

	cfg, err := loadService(t, "grpc:\n  targets:\n    - name: a\n      address: dns:///a:443\n", map[string]string{
		"SVC_GRPC_TARGETS_0_ADDRESS": "dns:///other:443",
		"SVC_ADDRESS":                "dns:///other:443",
		"SVC_TIMEOUT":                "1s",
	})
	require.NoError(t, err)
	assert.Equal(t, grpcx.DefaultConfig(grpcx.DefaultTarget("a", "dns:///a:443")), cfg.GRPC)
}
