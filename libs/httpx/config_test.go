package httpx_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// The tests below read the environment through a prefix no deployment uses,
// so a developer's own PING_* / TASKS_* variables cannot leak in.
const envPrefix = "HTTPXTEST_"

func TestLoadConfig_Defaults(t *testing.T) {
	t.Setenv(envPrefix+"ADMIN_TOKEN", "s3cret") // the default admin address is routable
	cfg, err := httpx.LoadConfig(envPrefix)
	require.NoError(t, err)
	assert.Equal(t, ":8080", cfg.Addr)
	assert.Equal(t, ":9080", cfg.AdminAddr)
	assert.False(t, cfg.AdminInsecure)
	assert.Equal(t, 5*time.Second, cfg.ReadHeaderTimeout)
	assert.Equal(t, 30*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 30*time.Second, cfg.WriteTimeout)
	assert.Equal(t, 120*time.Second, cfg.IdleTimeout)
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout)
	assert.Equal(t, 1<<20, cfg.MaxHeaderBytes)
	assert.Equal(t, int64(1<<20), cfg.MaxBodyBytes)
	assert.Equal(t, 2*time.Second, cfg.ShutdownDelay)
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, time.Second, cfg.SlowRequest)
	assert.Empty(t, cfg.TrustedProxies)
	assert.Equal(t, 10*time.Second, cfg.HealthInterval)
	assert.Equal(t, 3*time.Second, cfg.HealthTimeout)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 24*time.Hour, cfg.LogLevelMaxTTL)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.Equal(t, 100, cfg.LogSampleInitial)
	assert.Equal(t, 100, cfg.LogSampleThereafter)
	assert.NotEmpty(t, cfg.Service, "defaults to the executable name")
}

func TestLoadConfig_EmptyTokenOnRoutableAdminIsRejected(t *testing.T) {
	// The defaults put the admin listener on ":9080" (every interface) and
	// leave the token empty: running like that must be an explicit choice.
	_, err := httpx.LoadConfig(envPrefix)
	require.ErrorContains(t, err, "ADMIN_TOKEN is empty")

	t.Setenv(envPrefix+"ADMIN_INSECURE", "true")
	cfg, err := httpx.LoadConfig(envPrefix)
	require.NoError(t, err)
	assert.True(t, cfg.AdminInsecure)

	t.Setenv(envPrefix+"ADMIN_INSECURE", "false")
	t.Setenv(envPrefix+"ADMIN_ADDR", "127.0.0.1:9080")
	_, err = httpx.LoadConfig(envPrefix)
	require.NoError(t, err, "loopback needs no token")
}

func TestLoadConfig_PrefixAndOverride(t *testing.T) {
	t.Setenv(envPrefix+"ADMIN_ADDR", "127.0.0.1:9080")
	t.Setenv(envPrefix+"HTTP_ADDR", ":9999")
	t.Setenv(envPrefix+"HTTP_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv(envPrefix+"LOG_LEVEL", "debug")
	t.Setenv(envPrefix+"HTTP_TRUSTED_PROXIES", "10.0.0.0/8, 192.168.1.1")
	t.Setenv(envPrefix+"HTTP_MAX_BODY_BYTES", "2048")

	cfg, err := httpx.LoadConfig(envPrefix)
	require.NoError(t, err)
	assert.Equal(t, ":9999", cfg.Addr)
	assert.Equal(t, 3*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, []string{"10.0.0.0/8", " 192.168.1.1"}, cfg.TrustedProxies)
	assert.Equal(t, int64(2048), cfg.MaxBodyBytes)
}

func TestLoadConfig_InvalidIsAnError(t *testing.T) {
	t.Setenv(envPrefix+"ADMIN_ADDR", "127.0.0.1:9080")
	t.Setenv(envPrefix+"LOG_FORMAT", "xml")
	_, err := httpx.LoadConfig(envPrefix)
	require.ErrorContains(t, err, "LOG_FORMAT")

	t.Setenv(envPrefix+"LOG_FORMAT", "json")
	t.Setenv(envPrefix+"HTTP_READ_TIMEOUT", "soon")
	_, err = httpx.LoadConfig(envPrefix)
	require.ErrorContains(t, err, "parse config")
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	valid := func() httpx.Config { return httpx.Config{Addr: ":0", AdminAddr: "127.0.0.1:0"} }
	for _, tc := range []struct {
		name    string
		mutate  func(*httpx.Config)
		wantErr string // "" = valid
	}{
		{"baseline", func(*httpx.Config) {}, ""},
		{"unknown log level", func(c *httpx.Config) { c.LogLevel = "verbose" }, "LOG_LEVEL"},
		{"log level any case", func(c *httpx.Config) { c.LogLevel = "WARN" }, ""},
		{"unknown log format", func(c *httpx.Config) { c.LogFormat = "xml" }, "LOG_FORMAT"},
		{"no listener at all", func(c *httpx.Config) { c.Addr, c.AdminAddr = "", "" }, "neither HTTP_ADDR nor ADMIN_ADDR"},
		{"worker: admin only", func(c *httpx.Config) { c.Addr = "" }, ""},
		{"api only", func(c *httpx.Config) { c.AdminAddr = "" }, ""},
		{"same fixed address", func(c *httpx.Config) { c.Addr, c.AdminAddr, c.AdminToken = ":8080", ":8080", "t" }, "are both"},
		{"both ephemeral", func(c *httpx.Config) { c.Addr, c.AdminAddr = "127.0.0.1:0", "127.0.0.1:0" }, ""},
		{"negative shutdown timeout", func(c *httpx.Config) { c.ShutdownTimeout = -time.Second }, "HTTP_SHUTDOWN_TIMEOUT"},
		{"negative shutdown delay", func(c *httpx.Config) { c.ShutdownDelay = -time.Second }, "HTTP_SHUTDOWN_DELAY"},
		{"negative body limit", func(c *httpx.Config) { c.MaxBodyBytes = -1 }, "HTTP_MAX_BODY_BYTES"},
		{"negative header limit", func(c *httpx.Config) { c.MaxHeaderBytes = -1 }, "HTTP_MAX_HEADER_BYTES"},
		{"check timeout ≥ interval", func(c *httpx.Config) { c.HealthInterval, c.HealthTimeout = time.Second, time.Second }, "HEALTH_CHECK_TIMEOUT"},
		{"negative level TTL", func(c *httpx.Config) { c.LogLevelMaxTTL = -time.Minute }, "LOG_LEVEL_MAX_TTL"},
		{"negative sampling", func(c *httpx.Config) { c.LogSampleInitial = -1 }, "LOG_SAMPLE_INITIAL"},
		{"bad proxy CIDR", func(c *httpx.Config) { c.TrustedProxies = []string{"10.0.0.0/8", "not-an-ip"} }, "HTTP_TRUSTED_PROXIES"},
		{"proxy as bare IP", func(c *httpx.Config) { c.TrustedProxies = []string{"10.1.2.3", "::1", ""} }, ""},
		{"open admin on every interface", func(c *httpx.Config) { c.AdminAddr = ":9080" }, "ADMIN_TOKEN is empty"},
		{"open admin on 0.0.0.0", func(c *httpx.Config) { c.AdminAddr = "0.0.0.0:9080" }, "ADMIN_TOKEN is empty"},
		{"open admin on a pod IP", func(c *httpx.Config) { c.AdminAddr = "10.0.0.7:9080" }, "ADMIN_TOKEN is empty"},
		{"open admin on localhost", func(c *httpx.Config) { c.AdminAddr = "localhost:9080" }, ""},
		{"open admin on ::1", func(c *httpx.Config) { c.AdminAddr = "[::1]:9080" }, ""},
		{"open admin, insecure acknowledged", func(c *httpx.Config) { c.AdminAddr, c.AdminInsecure = ":9080", true }, ""},
		{"guarded admin anywhere", func(c *httpx.Config) { c.AdminAddr, c.AdminToken = ":9080", "s3cret" }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestConfigValidate_ReportsEveryProblemAndAppliesDefaults(t *testing.T) {
	cfg := httpx.Config{Addr: ":0", LogLevel: "loud", LogFormat: "xml", MaxBodyBytes: -1}
	err := cfg.Validate()
	require.Error(t, err)
	for _, want := range []string{"LOG_LEVEL", "LOG_FORMAT", "HTTP_MAX_BODY_BYTES"} {
		require.ErrorContains(t, err, want, "all problems at once, not the first one")
	}

	cfg = httpx.Config{Addr: ":0"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 30*time.Second, cfg.ReadTimeout, "a hand-built config gets the same defaults as LoadConfig")
	assert.Equal(t, 30*time.Second, cfg.WriteTimeout)
	assert.Equal(t, 120*time.Second, cfg.IdleTimeout)
	assert.Equal(t, 5*time.Second, cfg.ReadHeaderTimeout)
	assert.Equal(t, int64(1<<20), cfg.MaxBodyBytes)
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.NotEmpty(t, cfg.Service)
}

func TestConfig_LogConfigProjection(t *testing.T) {
	cfg := httpx.Config{Service: "svc", LogLevel: "warn", LogFormat: "text", LogSampleInitial: 5, LogSampleThereafter: 7}
	assert.Equal(t, httpx.LogConfig{Service: "svc", Level: "warn", Format: "text", SampleInitial: 5, SampleThereafter: 7}, cfg.LogConfig())
}

// sampleConfig is the minimal LoadYAML target: yaml + env tags, no defaults.
type sampleConfig struct {
	Addr    string `yaml:"addr" env:"HTTP_ADDR"`
	Workers int    `yaml:"workers" env:"WORKERS"`
}

// serviceConfig is the shape LoadYAML documents: the lib configs embedded,
// with their envDefault tags, instead of copied field by field.
type serviceConfig struct {
	httpx.Config `yaml:",inline"`

	Workers int      `yaml:"workers" env:"WORKERS" envDefault:"4"`
	DB      dbConfig `yaml:"db" envPrefix:"DB_"`
}

type dbConfig struct {
	DSN      string `yaml:"dsn" env:"DSN" envDefault:"postgres://localhost/app"`
	MaxConns int    `yaml:"max_conns" env:"MAX_CONNS" envDefault:"10"`
}

func writeYAML(t testing.TB, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestLoadYAML_FileOnly(t *testing.T) {
	path := writeYAML(t, "addr: \":9000\"\nworkers: 4\n")

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(path, envPrefix, &cfg))
	assert.Equal(t, ":9000", cfg.Addr)
	assert.Equal(t, 4, cfg.Workers)
}

func TestLoadYAML_EnvOverlaysFile(t *testing.T) {
	path := writeYAML(t, "addr: \":9000\"\nworkers: 4\n")
	t.Setenv(envPrefix+"HTTP_ADDR", ":7777") // env wins over the file…

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(path, envPrefix, &cfg))
	assert.Equal(t, ":7777", cfg.Addr)
	assert.Equal(t, 4, cfg.Workers, "…but a field without an env override keeps its YAML value")
}

func TestLoadYAML_MissingFileIsEnvOnly(t *testing.T) {
	t.Setenv(envPrefix+"HTTP_ADDR", ":8080")

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(filepath.Join(t.TempDir(), "absent.yaml"), envPrefix, &cfg))
	assert.Equal(t, ":8080", cfg.Addr, "a missing file is not an error; env-only config still loads")
	assert.Equal(t, 0, cfg.Workers)

	require.NoError(t, httpx.LoadYAML("", envPrefix, &cfg), "no path at all is env-only too")
}

func TestLoadYAML_PrecedenceEnvThenYAMLThenDefault(t *testing.T) {
	path := writeYAML(t, `
http_addr: ":9000"
log_level: warn
workers: 8
db:
  max_conns: 20
`)
	t.Setenv(envPrefix+"HTTP_ADDR", ":7777")                // env over yaml, embedded struct
	t.Setenv(envPrefix+"DB_DSN", "postgres://env-host/app") // env over default, nested envPrefix

	var cfg serviceConfig
	require.NoError(t, httpx.LoadYAML(path, envPrefix, &cfg))

	assert.Equal(t, ":7777", cfg.Addr, "env > yaml")
	assert.Equal(t, "warn", cfg.LogLevel, "yaml > envDefault (embedded)")
	assert.Equal(t, 8, cfg.Workers, "yaml > envDefault")
	assert.Equal(t, 20, cfg.DB.MaxConns, "yaml > envDefault (nested)")
	assert.Equal(t, "postgres://env-host/app", cfg.DB.DSN, "env > envDefault (nested, prefixed)")

	// Untouched fields are filled from the envDefault tags.
	assert.Equal(t, ":9080", cfg.AdminAddr)
	assert.Equal(t, 30*time.Second, cfg.ReadTimeout)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.Equal(t, 100, cfg.LogSampleInitial)

	// The embedded Config is usable as is.
	cfg.AdminAddr = "127.0.0.1:0"
	require.NoError(t, cfg.Validate())
}

func TestLoadYAML_EnvDefaultsWithoutFile(t *testing.T) {
	var cfg serviceConfig
	require.NoError(t, httpx.LoadYAML("", envPrefix, &cfg))
	assert.Equal(t, ":8080", cfg.Addr)
	assert.Equal(t, 4, cfg.Workers)
	assert.Equal(t, "postgres://localhost/app", cfg.DB.DSN)
	assert.Equal(t, 10, cfg.DB.MaxConns)
}

func TestLoadYAML_Errors(t *testing.T) {
	t.Run("unknown key", func(t *testing.T) {
		var cfg serviceConfig
		err := httpx.LoadYAML(writeYAML(t, "http_adr: \":9000\"\n"), envPrefix, &cfg)
		require.ErrorContains(t, err, "parse yaml")
		assert.ErrorContains(t, err, "http_adr", "a typo is named, not silently defaulted")
	})
	t.Run("unknown nested key", func(t *testing.T) {
		var cfg serviceConfig
		err := httpx.LoadYAML(writeYAML(t, "db:\n  max_con: 3\n"), envPrefix, &cfg)
		require.ErrorContains(t, err, "max_con")
	})
	t.Run("wrong type", func(t *testing.T) {
		var cfg sampleConfig
		err := httpx.LoadYAML(writeYAML(t, "workers: many\n"), envPrefix, &cfg)
		require.ErrorContains(t, err, "parse yaml")
	})
	t.Run("empty file is an empty config", func(t *testing.T) {
		var cfg sampleConfig
		require.NoError(t, httpx.LoadYAML(writeYAML(t, ""), envPrefix, &cfg))
	})
	t.Run("unreadable path", func(t *testing.T) {
		var cfg sampleConfig
		err := httpx.LoadYAML(t.TempDir(), envPrefix, &cfg) // a directory
		require.ErrorContains(t, err, "read yaml")
	})
	t.Run("bad env value", func(t *testing.T) {
		t.Setenv(envPrefix+"WORKERS", "many")
		var cfg sampleConfig
		err := httpx.LoadYAML("", envPrefix, &cfg)
		require.ErrorContains(t, err, "env overlay")
	})
}
