package compat_test

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/go-viper/mapstructure/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/tracehubmmp/golang-basics/libs/secret"
)

const hunter = "hunter2-correct-horse"

// redacted is how hunter renders: the marker and the first four bytes of its
// SHA-256.
var redacted = "[redacted:" + secret.New(hunter).Fingerprint() + "]"

// config is what a service config looks like: tagged for every loader this
// suite runs it through.
type config struct {
	Host     string        `env:"HOST"     mapstructure:"host"     yaml:"host"`
	Password secret.String `env:"PASSWORD" mapstructure:"password" yaml:"password"`
	Token    secret.String `env:"TOKEN"    mapstructure:"token"    yaml:"token,omitempty"`
	Default  secret.String `env:"DEFAULT"  envDefault:"from-default" mapstructure:"-"      yaml:"-"`
}

func TestString_YAML(t *testing.T) {
	t.Parallel()

	var cfg config

	require.NoError(t, yaml.Unmarshal([]byte("host: db\npassword: "+hunter+"\n"), &cfg))
	assert.Equal(t, hunter, cfg.Password.Expose())

	out, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	assert.YAMLEq(t, "host: db\npassword: '"+redacted+"'\n", string(out), "set → redacted, unset + omitempty → absent")
}

func TestString_Env(t *testing.T) {
	t.Parallel()

	var cfg config

	require.NoError(t, env.ParseWithOptions(&cfg, env.Options{
		Prefix:      "APP_",
		Environment: map[string]string{"APP_HOST": "db", "APP_PASSWORD": hunter},
	}))
	assert.Equal(t, hunter, cfg.Password.Expose())
	assert.Equal(t, "from-default", cfg.Default.Expose(), "envDefault goes through UnmarshalText too")
	assert.True(t, cfg.Token.IsZero())
}

// DecodeHook against the real mapstructure — the decoder behind
// viper.Unmarshal — passed as a bare function, the way a caller would.
func TestDecodeHook_Mapstructure(t *testing.T) {
	t.Parallel()

	decode := func(t *testing.T, input map[string]any) (config, error) {
		t.Helper()

		var cfg config

		dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(secret.DecodeHook, mapstructure.StringToTimeDurationHookFunc()),
			Result:     &cfg,
		})
		require.NoError(t, err)

		return cfg, dec.Decode(input)
	}

	cfg, err := decode(t, map[string]any{"host": "db", "password": hunter, "token": []byte("from-bytes")})
	require.NoError(t, err)
	assert.Equal(t, "db", cfg.Host)
	assert.Equal(t, hunter, cfg.Password.Expose())
	assert.Equal(t, "from-bytes", cfg.Token.Expose())

	cfg, err = decode(t, map[string]any{"password": secret.New(hunter)})
	require.NoError(t, err)
	assert.Equal(t, hunter, cfg.Password.Expose())

	_, err = decode(t, map[string]any{"password": 31337})
	require.ErrorIs(t, err, secret.ErrDecode)
	assert.NotContains(t, err.Error(), "31337", "a rejected value is not echoed")
}
