package httpx_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

type bootSection struct {
	Brokers []string      `env:"BROKERS" envSeparator:"," yaml:"brokers"`
	Timeout time.Duration `env:"TIMEOUT" envDefault:"5s" yaml:"timeout"`
}

type bootConfig struct {
	httpx.Config `yaml:",inline"`
	Kafka        bootSection `yaml:"kafka" envPrefix:"KAFKA_"`
	Size         int         `env:"SIZE" envDefault:"10" yaml:"size"`
	Internal     string      // no env tag: not a key
}

func testBoot(load func() (bootConfig, error)) httpx.Boot[bootConfig] {
	return httpx.Boot[bootConfig]{
		Name: "svc", Prefix: "SVC_", File: "SVC_CONFIG",
		Load: load,
		Defaults: func() (bootConfig, error) {
			cfg := bootConfig{}
			cfg.Service = "svc"
			err := httpx.Defaults("SVC_", &cfg)
			return cfg, err
		},
	}
}

func runBoot(b httpx.Boot[bootConfig], args ...string) (cfg bootConfig, code int, ok bool, stdout, stderr string) {
	var out, errOut bytes.Buffer
	cfg, code, ok = b.Config(args, &out, &errOut)
	return cfg, code, ok, out.String(), errOut.String()
}

func TestBoot_LoadsAndGoesOn(t *testing.T) {
	b := testBoot(func() (bootConfig, error) { return bootConfig{Size: 3}, nil })
	cfg, code, ok, out, errOut := runBoot(b)
	assert.True(t, ok)
	assert.Zero(t, code)
	assert.Equal(t, 3, cfg.Size)
	assert.Empty(t, out+errOut)
}

func TestBoot_ConfigErrorExits78(t *testing.T) {
	b := testBoot(func() (bootConfig, error) { return bootConfig{}, errors.New("SVC_SIZE must be positive") })
	for _, args := range [][]string{nil, {"--check-config"}} {
		_, code, ok, out, errOut := runBoot(b, args...)
		assert.False(t, ok)
		assert.Equal(t, httpx.ExitConfig, code)
		assert.Empty(t, out)
		assert.Equal(t, "svc: SVC_SIZE must be positive\n", errOut)
	}
}

func TestBoot_CheckConfigPasses(t *testing.T) {
	b := testBoot(func() (bootConfig, error) { return bootConfig{}, nil })
	_, code, ok, out, _ := runBoot(b, "--check-config")
	assert.False(t, ok, "a check never starts the service")
	assert.Zero(t, code)
	assert.Equal(t, "config ok\n", out)
}

func TestBoot_Help(t *testing.T) {
	loaded := false
	b := testBoot(func() (bootConfig, error) { loaded = true; return bootConfig{}, nil })
	for _, flag := range []string{"--help", "-h"} {
		_, code, ok, out, errOut := runBoot(b, flag)
		assert.False(t, ok)
		assert.Zero(t, code)
		assert.Empty(t, errOut)
		assert.Contains(t, out, "Usage: svc")
		assert.Contains(t, out, "SVC_*, over the optional YAML file named by SVC_CONFIG")
		assert.Contains(t, out, "-check-config")
	}
	assert.False(t, loaded, "--help needs no valid config")
}

func TestBoot_BadCommandLineExits64(t *testing.T) {
	b := testBoot(func() (bootConfig, error) { return bootConfig{}, nil })
	for _, args := range [][]string{{"--nope"}, {"serve"}} {
		_, code, ok, out, errOut := runBoot(b, args...)
		assert.False(t, ok)
		assert.Equal(t, httpx.ExitUsage, code, "%v", args)
		assert.Empty(t, out)
		assert.Contains(t, errOut, "--help")
	}
}

func TestBoot_ConfigReference(t *testing.T) {
	b := testBoot(func() (bootConfig, error) { t.Fatal("the reference needs no valid config"); return bootConfig{}, nil })
	_, code, ok, out, _ := runBoot(b, "--config-reference")
	assert.False(t, ok)
	assert.Zero(t, code)

	rows := map[string][]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		f := strings.Fields(line)
		require.Len(t, f, 3, line)
		rows[f[0]] = f[1:]
	}
	assert.Equal(t, []string{"service_name", "svc"}, rows["SVC_SERVICE_NAME"], "inline struct, service preset")
	assert.Equal(t, []string{"http_addr", ":8080"}, rows["SVC_HTTP_ADDR"], "tag default")
	assert.Equal(t, []string{"admin_token", "-"}, rows["SVC_ADMIN_TOKEN"], "unset")
	assert.Equal(t, []string{"kafka.brokers", "-"}, rows["SVC_KAFKA_BROKERS"], "nested section with envPrefix")
	assert.Equal(t, []string{"kafka.timeout", "5s"}, rows["SVC_KAFKA_TIMEOUT"])
	assert.Equal(t, []string{"size", "10"}, rows["SVC_SIZE"])
	assert.Equal(t, []string{"admin_insecure", "false"}, rows["SVC_ADMIN_INSECURE"], "a zero bool is a value")
	assert.NotContains(t, out, "Internal")
}

func TestConfigReference_HidesSecrets(t *testing.T) {
	var out bytes.Buffer
	httpx.ConfigReference(&out, "SVC_", httpx.Config{AdminToken: "s3cret", DebugToken: ""})
	assert.NotContains(t, out.String(), "s3cret")
	assert.Regexp(t, `SVC_ADMIN_TOKEN\s+admin_token\s+\(secret\)`, out.String())
	assert.Regexp(t, `SVC_DEBUG_TOKEN\s+debug_token\s+-`, out.String())
}
