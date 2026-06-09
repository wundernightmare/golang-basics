package httpx_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// sampleConfig follows the LoadYAML contract: yaml + env tags, no envDefault, so
// env overlays YAML rather than being reset to a default.
type sampleConfig struct {
	Addr    string `yaml:"addr" env:"HTTP_ADDR"`
	Workers int    `yaml:"workers" env:"WORKERS"`
}

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestLoadYAMLFileOnly(t *testing.T) {
	path := writeYAML(t, "addr: \":9000\"\nworkers: 4\n")

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(path, "APP_", &cfg))
	require.Equal(t, ":9000", cfg.Addr)
	require.Equal(t, 4, cfg.Workers)
}

func TestLoadYAMLEnvOverlaysFile(t *testing.T) {
	path := writeYAML(t, "addr: \":9000\"\nworkers: 4\n")
	t.Setenv("APP_HTTP_ADDR", ":7777") // env wins over the file…

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(path, "APP_", &cfg))
	require.Equal(t, ":7777", cfg.Addr)
	require.Equal(t, 4, cfg.Workers, "…but a field without an env override keeps its YAML value")
}

func TestLoadYAMLMissingFileIsEnvOnly(t *testing.T) {
	t.Setenv("APP_HTTP_ADDR", ":8080")

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(filepath.Join(t.TempDir(), "absent.yaml"), "APP_", &cfg))
	require.Equal(t, ":8080", cfg.Addr, "a missing file is not an error; env-only config still loads")
	require.Equal(t, 0, cfg.Workers)
}
