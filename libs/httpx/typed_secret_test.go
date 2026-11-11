package httpx_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// opaque has the shape of libs/secret's String — the value behind a pointer,
// Expose / IsZero, text in and redacted text out — without importing it:
// httpx recognises a typed secret by that shape.
type opaque struct{ value *string }

func (o opaque) Expose() string {
	if o.value == nil {
		return ""
	}

	return *o.value
}

func (o opaque) IsZero() bool { return o.value == nil }

func (o opaque) Fingerprint() string {
	if o.value == nil || *o.value == "not-hex" {
		return "not hex!"
	}

	return "1a2b3c4d"
}

func (o opaque) MarshalText() ([]byte, error) { return []byte("[opaque]"), nil }

func (o *opaque) UnmarshalText(text []byte) error {
	o.value = nil
	if len(text) > 0 {
		o.value = new(string(text))
	}

	return nil
}

type typedSecretConfig struct {
	Host      string            `env:"HOST"      envDefault:"db"      yaml:"host"`
	Password  opaque            `env:"PASSWORD"  yaml:"password"`
	Signing   opaque            `env:"SIGNING"   envDefault:"dev-key" yaml:"signing"`
	Unset     opaque            `env:"UNSET"     yaml:"unset"`
	OptedOut  opaque            `env:"OPTED_OUT" secret:"false"       yaml:"opted_out"`
	Upstreams map[string]opaque `yaml:"upstreams"`
}

// A typed secret loads from YAML, the environment and envDefault like any
// scalar, and the loaded config is safe to serve and to print.
func TestTypedSecret_LoadRedactAndReference(t *testing.T) {
	t.Parallel()

	path := writeYAML(t, "password: from-file\nopted_out: still-hidden\nupstreams:\n  billing: upstream-key\n")

	var cfg typedSecretConfig
	require.NoError(t, httpx.LoadYAMLFrom(path, envPrefix, map[string]string{}, &cfg))
	assert.Equal(t, "from-file", cfg.Password.Expose())
	assert.Equal(t, "dev-key", cfg.Signing.Expose(), "envDefault fills a typed secret")
	assert.True(t, cfg.Unset.IsZero())
	assert.Equal(t, "upstream-key", cfg.Upstreams["billing"].Expose())

	require.NoError(t, httpx.LoadYAMLFrom(path, envPrefix, map[string]string{envPrefix + "PASSWORD": "from-env"}, &cfg))
	assert.Equal(t, "from-env", cfg.Password.Expose(), "the environment wins over the file")

	redacted, raw := redactJSON(t, cfg)
	for _, value := range []string{"from-env", "from-file", "dev-key", "still-hidden", "upstream-key"} {
		assert.NotContains(t, raw, value)
	}

	assert.Equal(t, "db", redacted["host"])
	assert.Equal(t, "[redacted:1a2b3c4d]", redacted["password"], "the fingerprint shows whether a rotation arrived")
	assert.Equal(t, "[redacted:1a2b3c4d]", redacted["signing"])
	assert.Empty(t, redacted["unset"], "unset stays visibly unset")
	assert.Equal(t, "[redacted:1a2b3c4d]", redacted["opted_out"], `secret:"false" cannot open a typed secret`)
	assert.Equal(t, map[string]any{"billing": "[redacted:1a2b3c4d]"}, redacted["upstreams"])

	redacted, _ = redactJSON(t, map[string]opaque{"odd": {value: new("not-hex")}})
	assert.Equal(t, "[redacted]", redacted["odd"], "only a hex fingerprint is passed on")

	var out bytes.Buffer
	httpx.ConfigReference(&out, "SVC_", cfg)
	ref := out.String()
	assert.Regexp(t, `SVC_PASSWORD\s+password\s+\(secret\)`, ref)
	assert.Regexp(t, `SVC_SIGNING\s+signing\s+\(secret\)`, ref)
	assert.Regexp(t, `SVC_UNSET\s+unset\s+-`, ref)
	assert.NotContains(t, ref, "from-env")
	assert.NotContains(t, ref, "dev-key")
}
