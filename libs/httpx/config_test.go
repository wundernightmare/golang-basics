package httpx_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/secret"
)

// The tests below read the environment through a prefix no deployment uses,
// so a developer's own PING_* / TASKS_* variables cannot leak in.
const envPrefix = "HTTPXTEST_"

func TestLoadConfig_Defaults(t *testing.T) {
	t.Parallel()

	environ := map[string]string{envPrefix + "ADMIN_TOKEN": "s3cret"} // the default admin address is routable
	cfg, err := httpx.LoadConfigFrom(envPrefix, environ)
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
	t.Parallel()

	// The defaults put the admin listener on ":9080" (every interface) and
	// leave the token empty: running like that must be an explicit choice.
	environ := map[string]string{} // nothing set; each step below adds to it
	_, err := httpx.LoadConfigFrom(envPrefix, environ)
	require.ErrorContains(t, err, "ADMIN_TOKEN is empty")

	environ[envPrefix+"ADMIN_INSECURE"] = "true"
	cfg, err := httpx.LoadConfigFrom(envPrefix, environ)
	require.NoError(t, err)
	assert.True(t, cfg.AdminInsecure)

	environ[envPrefix+"ADMIN_INSECURE"] = "false"
	environ[envPrefix+"ADMIN_ADDR"] = "127.0.0.1:9080"
	_, err = httpx.LoadConfigFrom(envPrefix, environ)
	require.NoError(t, err, "loopback needs no token")
}

// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
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
	t.Parallel()

	environ := map[string]string{
		envPrefix + "ADMIN_ADDR": "127.0.0.1:9080",
		envPrefix + "LOG_FORMAT": "xml",
	}
	_, err := httpx.LoadConfigFrom(envPrefix, environ)
	require.ErrorContains(t, err, "LOG_FORMAT")

	environ[envPrefix+"LOG_FORMAT"] = "json"
	environ[envPrefix+"HTTP_READ_TIMEOUT"] = "soon"
	_, err = httpx.LoadConfigFrom(envPrefix, environ)
	require.ErrorContains(t, err, "parse config")
	require.ErrorIs(t, err, httpx.ErrConfig, "a value that does not parse is a configuration error like a value that is refused")
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
		{"same fixed address", func(c *httpx.Config) { c.Addr, c.AdminAddr, c.AdminToken = ":8080", ":8080", secret.New("t") }, "are both"},
		{"both ephemeral", func(c *httpx.Config) { c.Addr, c.AdminAddr = "127.0.0.1:0", "127.0.0.1:0" }, ""},
		{"negative shutdown timeout", func(c *httpx.Config) { c.ShutdownTimeout = -time.Second }, "HTTP_SHUTDOWN_TIMEOUT"},
		{"negative shutdown delay", func(c *httpx.Config) { c.ShutdownDelay = -time.Second }, "HTTP_SHUTDOWN_DELAY"},
		{"negative body limit", func(c *httpx.Config) { c.MaxBodyBytes = -1 }, "HTTP_MAX_BODY_BYTES"},
		{"negative header limit", func(c *httpx.Config) { c.MaxHeaderBytes = -1 }, "HTTP_MAX_HEADER_BYTES"},
		{"check timeout ≥ interval", func(c *httpx.Config) { c.HealthInterval, c.HealthTimeout = time.Second, time.Second }, "HEALTH_CHECK_TIMEOUT"},
		{"negative read timeout", func(c *httpx.Config) { c.ReadTimeout = -time.Second }, "HTTP_READ_TIMEOUT"},
		{"negative read-header timeout", func(c *httpx.Config) { c.ReadHeaderTimeout = -time.Second }, "HTTP_READ_HEADER_TIMEOUT"},
		{"negative write timeout", func(c *httpx.Config) { c.WriteTimeout = -time.Second }, "HTTP_WRITE_TIMEOUT"},
		{"negative idle timeout", func(c *httpx.Config) { c.IdleTimeout = -time.Second }, "HTTP_IDLE_TIMEOUT"},
		{"negative check interval and timeout", func(c *httpx.Config) { c.HealthInterval, c.HealthTimeout = -time.Second, -2*time.Second }, "HEALTH_CHECK_"},
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
		{"guarded admin anywhere", func(c *httpx.Config) { c.AdminAddr, c.AdminToken = ":9080", secret.New("s3cret") }, ""},
		// The edge of each rule: the last value it accepts.
		{"a floor without a request timeout", func(c *httpx.Config) { c.RequestTimeoutMin = time.Second }, ""},
		{"floor equal to the request timeout", func(c *httpx.Config) { c.RequestTimeout, c.RequestTimeoutMin = time.Second, time.Second }, ""},
		{"floor above the request timeout", func(c *httpx.Config) { c.RequestTimeout, c.RequestTimeoutMin = time.Second, 2*time.Second }, "HTTP_REQUEST_TIMEOUT_MIN"},
		{"memory limit ratio of one", func(c *httpx.Config) { c.RuntimeMemoryLimitRatio = 1 }, ""},
		{"memory limit ratio above one", func(c *httpx.Config) { c.RuntimeMemoryLimitRatio = 1.01 }, "RUNTIME_MEMORY_LIMIT_RATIO"},
		{"memory threshold of one", func(c *httpx.Config) { c.DiagMemoryThreshold = 1 }, ""},
		{"memory threshold above one", func(c *httpx.Config) { c.DiagMemoryThreshold = 1.01 }, "DIAG_MEMORY_THRESHOLD"},
		{"one spool file, one crash byte", func(c *httpx.Config) { c.DiagSpoolMaxFiles, c.DiagCrashMaxBytes = 1, 1 }, ""},
		{"negative spool size", func(c *httpx.Config) { c.DiagSpoolMaxFiles = -1 }, "DIAG_SPOOL_MAX_FILES"},
		{"negative crash size", func(c *httpx.Config) { c.DiagCrashMaxBytes = -1 }, "DIAG_SPOOL_MAX_FILES"},
		{"flight recorder with no slow-request trigger and no cooldown", func(c *httpx.Config) { c.DiagFlightRecorder = true }, ""},
		{"flight recorder, negative trigger", func(c *httpx.Config) { c.DiagFlightRecorder, c.DiagFlightSlowRequest = true, -1 }, "DIAG_FLIGHT_"},
		{"flight recorder off: its keys are not read", func(c *httpx.Config) { c.DiagFlightCooldown = -1 }, ""},
		{"zero rates and bursts", func(c *httpx.Config) {
			c.RateLimit, c.RateLimitPerClient, c.RateBurst, c.RateBurstPerClient = 0, 0, 0, 0
		}, ""},
		{"negative client table", func(c *httpx.Config) { c.RateClientsMax = -1 }, "HTTP_RATE_"},
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
			require.ErrorIs(t, err, httpx.ErrConfig)
		})
	}
}

func TestConfigValidate_ReportsEveryProblemAndAppliesDefaults(t *testing.T) {
	t.Parallel()

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
	assert.Equal(t, 30*time.Second, cfg.RuntimeMemoryLimitRefresh)
	assert.Equal(t, 10*time.Second, cfg.DiagFlightMinAge)
	assert.Equal(t, int64(8<<20), cfg.DiagFlightMaxBytes)

	// A default fills a zero field only: what the service set is kept.
	cfg = httpx.Config{
		Addr: ":0", RuntimeMemoryLimitRefresh: 7 * time.Second, DiagMemoryCooldown: 7 * time.Second,
		DiagFlightMinAge: 7 * time.Second, DiagFlightMaxBytes: 7,
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 7*time.Second, cfg.RuntimeMemoryLimitRefresh)
	assert.Equal(t, 7*time.Second, cfg.DiagMemoryCooldown)
	assert.Equal(t, 7*time.Second, cfg.DiagFlightMinAge)
	assert.Equal(t, int64(7), cfg.DiagFlightMaxBytes)
}

// An explicit zero is kept where zero is a setting of its own; for any other
// key it is "not set" and takes the default, whichever way it was written.
func TestLoadConfig_ExplicitZero(t *testing.T) {
	t.Parallel()

	cfg, err := httpx.LoadConfigFrom("", map[string]string{
		"ADMIN_INSECURE":        "true",
		"HTTP_SHUTDOWN_DELAY":   "0s",
		"HTTP_SLOW_REQUEST":     "0s",
		"DIAG_MEMORY_COOLDOWN":  "0s",
		"HTTP_SHUTDOWN_TIMEOUT": "0s",
	})
	require.NoError(t, err)

	assert.Zero(t, cfg.ShutdownDelay, "no delay")
	assert.Zero(t, cfg.SlowRequest, "no slow-request warning")
	assert.Zero(t, cfg.DiagMemoryCooldown, "no cooldown")
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout, "a drain without a budget is not a setting")
}

// A hand-built config and a loaded one agree on every default: there is one
// place the values are written. The exceptions are the fields whose zero is
// a setting of its own, which a hand-built config keeps.
func TestConfigValidate_AHandBuiltConfigGetsTheLoadedDefaults(t *testing.T) {
	t.Parallel()

	loaded, err := httpx.LoadConfigFrom("", map[string]string{"ADMIN_INSECURE": "true"})
	require.NoError(t, err)

	built := httpx.Config{Addr: loaded.Addr, AdminAddr: loaded.AdminAddr, AdminInsecure: true}
	require.NoError(t, built.Validate())

	assert.Zero(t, built.RequestTimeout, "no deadline")
	assert.Zero(t, built.RequestTimeoutMin)
	assert.Zero(t, built.ShutdownDelay, "no delay")
	assert.Zero(t, built.SlowRequest)
	assert.Zero(t, built.LogSampleInitial, "no sampling")
	assert.Zero(t, built.LogSampleThereafter)
	assert.Zero(t, built.DiagFlightCooldown)
	assert.Zero(t, built.DiagMemoryCooldown, "no cooldown")

	worker := httpx.Config{AdminAddr: "127.0.0.1:0"}
	require.NoError(t, worker.Validate())
	assert.Empty(t, worker.Addr, "no API listener: a worker")

	built.RequestTimeout, built.RequestTimeoutMin = loaded.RequestTimeout, loaded.RequestTimeoutMin
	built.ShutdownDelay, built.SlowRequest = loaded.ShutdownDelay, loaded.SlowRequest
	built.LogSampleInitial, built.LogSampleThereafter = loaded.LogSampleInitial, loaded.LogSampleThereafter
	built.DiagFlightCooldown, built.DiagMemoryCooldown = loaded.DiagFlightCooldown, loaded.DiagMemoryCooldown

	assert.Equal(t, loaded, built)
}

func TestConfig_LogConfigProjection(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	path := writeYAML(t, "addr: \":9000\"\nworkers: 4\n")

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAMLFrom(path, envPrefix, map[string]string{}, &cfg))
	assert.Equal(t, ":9000", cfg.Addr)
	assert.Equal(t, 4, cfg.Workers)
}

// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
func TestLoadYAML_EnvOverlaysFile(t *testing.T) {
	path := writeYAML(t, "addr: \":9000\"\nworkers: 4\n")
	t.Setenv(envPrefix+"HTTP_ADDR", ":7777") // env wins over the file…

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAML(path, envPrefix, &cfg))
	assert.Equal(t, ":7777", cfg.Addr)
	assert.Equal(t, 4, cfg.Workers, "…but a field without an env override keeps its YAML value")
}

func TestLoadYAML_NoPathIsEnvOnly(t *testing.T) {
	t.Parallel()

	environ := map[string]string{envPrefix + "HTTP_ADDR": ":8080"}

	var cfg sampleConfig
	require.NoError(t, httpx.LoadYAMLFrom("", envPrefix, environ, &cfg), "no path: env-only")
	assert.Equal(t, ":8080", cfg.Addr)
	assert.Equal(t, 0, cfg.Workers)
}

// A path that is set but names no file is a mistake (a typo, a ConfigMap that
// was not mounted) — never a silent fall-back to the defaults.
func TestLoadYAML_MissingFileIsAnError(t *testing.T) {
	t.Parallel()

	var cfg sampleConfig

	err := httpx.LoadYAMLFrom(filepath.Join(t.TempDir(), "absent.yaml"), envPrefix, map[string]string{}, &cfg)
	require.ErrorIs(t, err, fs.ErrNotExist)
	assert.ErrorContains(t, err, "absent.yaml")
}

func TestLoadYAML_PrecedenceEnvThenYAMLThenDefault(t *testing.T) {
	t.Parallel()

	path := writeYAML(t, `
http_addr: ":9000"
log_level: warn
workers: 8
db:
  max_conns: 20
`)
	environ := map[string]string{
		envPrefix + "HTTP_ADDR": ":7777",                   // env over yaml, embedded struct
		envPrefix + "DB_DSN":    "postgres://env-host/app", // env over default, nested envPrefix
	}

	var cfg serviceConfig
	require.NoError(t, httpx.LoadYAMLFrom(path, envPrefix, environ, &cfg))

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
	t.Parallel()

	var cfg serviceConfig

	cfg.AdminAddr, cfg.DB.MaxConns = ":9082", 3 // the service's own defaults for two lib fields

	require.NoError(t, httpx.LoadYAMLFrom("", envPrefix, map[string]string{}, &cfg))
	assert.Equal(t, ":8080", cfg.Addr)
	assert.Equal(t, 4, cfg.Workers)
	assert.Equal(t, "postgres://localhost/app", cfg.DB.DSN)
	assert.Equal(t, ":9082", cfg.AdminAddr, "what the service preset is its default: the tag does not replace it")
	assert.Equal(t, 3, cfg.DB.MaxConns)
}

// agedSection is a library's config section that used to read SECTION_OLD.
type agedSection struct {
	New string `env:"SECTION_NEW" yaml:"new"`
}

func (agedSection) RetiredKeys() map[string]string {
	return map[string]string{"SECTION_OLD": "set SECTION_NEW instead"}
}

// The loader reads the keys the struct has, so a key a library dropped would
// be ignored without a word; the section names it and the load fails.
//
// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
func TestLoadYAML_RefusesAKeyASectionRetired(t *testing.T) {
	type config struct {
		Section agedSection `yaml:"section"`
		Second  agedSection `envPrefix:"SECOND_" yaml:"second"`
	}

	for _, key := range []string{envPrefix + "SECTION_OLD", envPrefix + "SECOND_SECTION_OLD"} {
		var cfg config

		err := httpx.LoadYAMLFrom("", envPrefix, map[string]string{key: ""}, &cfg)
		require.ErrorContains(t, err, key+" is no longer read: set SECTION_NEW instead", "whatever its value")
		require.ErrorIs(t, err, httpx.ErrConfig)
	}

	var cfg config
	require.NoError(t, httpx.LoadYAMLFrom("", envPrefix, map[string]string{"OTHER_SECTION_OLD": "1", envPrefix + "SECTION_NEW": "1"}, &cfg),
		"another prefix is another service's key")

	t.Setenv(envPrefix+"SECTION_OLD", "1")
	require.ErrorIs(t, httpx.LoadYAML("", envPrefix, &cfg), httpx.ErrConfig, "the process environment is searched the same way")
}

func TestLoadYAML_Errors(t *testing.T) {
	t.Parallel()

	t.Run("unknown key", func(t *testing.T) {
		t.Parallel()

		var cfg serviceConfig

		err := httpx.LoadYAMLFrom(writeYAML(t, "http_adr: \":9000\"\n"), envPrefix, map[string]string{}, &cfg)
		require.ErrorContains(t, err, "parse yaml")
		require.ErrorIs(t, err, httpx.ErrConfig)
		assert.ErrorContains(t, err, "http_adr", "a typo is named, not silently defaulted")
	})
	t.Run("unknown nested key", func(t *testing.T) {
		t.Parallel()

		var cfg serviceConfig

		err := httpx.LoadYAMLFrom(writeYAML(t, "db:\n  max_con: 3\n"), envPrefix, map[string]string{}, &cfg)
		require.ErrorContains(t, err, "max_con")
		require.ErrorIs(t, err, httpx.ErrConfig)
	})
	t.Run("a second document", func(t *testing.T) {
		t.Parallel()

		var cfg sampleConfig

		err := httpx.LoadYAMLFrom(writeYAML(t, "workers: 1\n---\nworkers: 2\n"), envPrefix, map[string]string{}, &cfg)
		require.ErrorContains(t, err, "one YAML document", "what it says would be dropped without a word")
		require.ErrorIs(t, err, httpx.ErrConfig)
	})
	t.Run("wrong type", func(t *testing.T) {
		t.Parallel()

		var cfg sampleConfig

		err := httpx.LoadYAMLFrom(writeYAML(t, "workers: many\n"), envPrefix, map[string]string{}, &cfg)
		require.ErrorContains(t, err, "parse yaml")
		require.ErrorIs(t, err, httpx.ErrConfig)
	})
	t.Run("empty file is an empty config", func(t *testing.T) {
		t.Parallel()

		var cfg sampleConfig
		require.NoError(t, httpx.LoadYAMLFrom(writeYAML(t, ""), envPrefix, map[string]string{}, &cfg))
	})
	t.Run("unreadable path", func(t *testing.T) {
		t.Parallel()

		var cfg sampleConfig

		err := httpx.LoadYAMLFrom(t.TempDir(), envPrefix, map[string]string{}, &cfg) // a directory
		require.ErrorContains(t, err, "read yaml")
		require.ErrorIs(t, err, httpx.ErrConfig)
	})
	t.Run("bad env value", func(t *testing.T) {
		t.Parallel()

		environ := map[string]string{envPrefix + "WORKERS": "many"}

		var cfg sampleConfig

		err := httpx.LoadYAMLFrom("", envPrefix, environ, &cfg)
		require.ErrorContains(t, err, "env overlay")
		require.ErrorIs(t, err, httpx.ErrConfig)
	})
}
