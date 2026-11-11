package secret_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/go-viper/mapstructure/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/tracehubmmp/golang-basics/libs/secret"
)

const hunter = "hunter2-correct-horse"

// redacted is how hunter renders; the fingerprint is computed here, apart
// from the package, and pinned for a well-known value in TestString_Fingerprint.
var redacted = "[redacted:" + fingerprint(hunter) + "]"

func fingerprint(value string) string {
	digest := sha256.Sum256([]byte(value))

	return hex.EncodeToString(digest[:4])
}

// config is what a service config looks like: tagged for every loader the
// package supports, with the secret in exported and unexported positions.
type config struct {
	Host     string        `env:"HOST"     json:"host"               mapstructure:"host"     yaml:"host"`
	Password secret.String `env:"PASSWORD" json:"password"           mapstructure:"password" yaml:"password"`
	Token    secret.String `env:"TOKEN"    json:"token,omitzero"     mapstructure:"token"    yaml:"token,omitempty"`
	Default  secret.String `env:"DEFAULT"  envDefault:"from-default" json:"-"                mapstructure:"-"       yaml:"-"`
}

type holder struct {
	name   string
	hidden secret.String
	ptr    *secret.String
	list   []secret.String
	byKey  map[string]secret.String
}

func TestString_ExposeAndZero(t *testing.T) {
	t.Parallel()

	set := secret.New(hunter)
	assert.Equal(t, hunter, set.Expose())
	assert.False(t, set.IsZero())

	var unset secret.String

	assert.Empty(t, unset.Expose())
	assert.True(t, unset.IsZero())
	assert.Equal(t, unset, secret.New(""), "an empty value is the zero String")
	assert.Equal(t, set, secret.New(hunter), "equal content is DeepEqual, so configs compare in tests")
}

func TestString_Equal(t *testing.T) {
	t.Parallel()

	set := secret.New(hunter)
	assert.True(t, set.Equal(hunter))
	assert.False(t, set.Equal(hunter+"x"))
	assert.False(t, set.Equal(""))

	var unset secret.String

	assert.False(t, unset.Equal(""), "an unset secret must not accept an empty credential")
	assert.False(t, unset.Equal(hunter))
}

func TestString_Fingerprint(t *testing.T) {
	t.Parallel()

	// printf %s hunter2 | sha256sum | cut -c1-8
	assert.Equal(t, "f52fbd32", secret.New("hunter2").Fingerprint())
	assert.Equal(t, "[redacted:f52fbd32]", secret.New("hunter2").String())
	assert.NotEqual(t, secret.New("hunter2").Fingerprint(), secret.New("hunter3").Fingerprint(), "a rotated secret shows")
	assert.Empty(t, secret.String{}.Fingerprint())
}

// Every fmt verb, on the value, a pointer to it, and a struct that keeps it
// in fields fmt cannot call methods on.
func TestString_FmtNeverPrintsTheValue(t *testing.T) {
	t.Parallel()

	set := secret.New(hunter)
	nested := holder{
		name:   "n",
		hidden: set,
		ptr:    &set,
		list:   []secret.String{set},
		byKey:  map[string]secret.String{"k": set},
	}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%t", "%p", "%T", "%10s", "%-10v", "%U", "%c", "%e"} {
		for name, value := range map[string]any{"value": set, "pointer": &set, "nested": nested, "nested pointer": &nested} {
			assert.NotContains(t, fmt.Sprintf(verb, value), hunter, "%s of %s", verb, name)
		}
	}

	assert.Equal(t, redacted, fmt.Sprint(set))
	assert.Equal(t, redacted, set.String())
	assert.Equal(t, `"`+redacted+`"`, fmt.Sprintf("%q", set))
	assert.Equal(t, redacted, fmt.Sprintf("%#v", set))
	assert.Equal(t, redacted, fmt.Sprintf("%d", set))
	assert.Equal(t, "{h "+redacted+"}", fmt.Sprint(struct {
		Host     string
		Password secret.String
	}{"h", set}))

	var unset secret.String

	assert.Empty(t, fmt.Sprint(unset), "unset stays visibly unset")
	assert.Equal(t, `""`, fmt.Sprintf("%q", unset))
	assert.Equal(t, "an error: "+redacted, fmt.Errorf("an error: %w", fmt.Errorf("%v", set)).Error())
}

func TestString_Slog(t *testing.T) {
	t.Parallel()

	set := secret.New(hunter)
	cfg := config{Host: "db", Password: set}

	handlers := map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	}

	for name, newHandler := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			log := slog.New(newHandler(&buf)).With("bound", set)
			log.Info("boot",
				"password", set,
				"pointer", &set,
				"config", cfg,
				"config_pointer", &cfg,
				"holder", holder{hidden: set, ptr: &set},
				"group", slog.GroupValue(slog.Any("password", set)),
				"formatted", fmt.Sprintf("%v", cfg),
			)

			out := buf.String()
			assert.NotContains(t, out, hunter)
			assert.Contains(t, out, redacted)
			assert.Contains(t, out, "db", "the rest of the config is still logged")
		})
	}
}

func TestString_JSON(t *testing.T) {
	t.Parallel()

	var cfg config

	require.NoError(t, json.Unmarshal([]byte(`{"host":"db","password":"`+hunter+`"}`), &cfg))
	assert.Equal(t, hunter, cfg.Password.Expose())
	assert.True(t, cfg.Token.IsZero())

	out, err := json.Marshal(cfg)
	require.NoError(t, err)
	assert.JSONEq(t, `{"host":"db","password":"`+redacted+`"}`, string(out), "set → redacted, unset + omitzero → absent")

	out, err = json.Marshal(map[string]any{"direct": cfg.Password, "pointer": &cfg.Password, "unset": secret.String{}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"direct":"`+redacted+`","pointer":"`+redacted+`","unset":""}`, string(out))

	require.Error(t, json.Unmarshal([]byte(`{"password":{"value":"x"}}`), &cfg), "only a JSON string is a secret")
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

func TestString_Flag(t *testing.T) {
	t.Parallel()

	var token secret.String

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.TextVar(&token, "token", secret.String{}, "API token")
	require.NoError(t, fs.Parse([]string{"-token", hunter}))
	assert.Equal(t, hunter, token.Expose())

	var usage bytes.Buffer

	fs.SetOutput(&usage)
	fs.PrintDefaults()
	assert.NotContains(t, usage.String(), hunter)
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

func TestDecodeHook_Direct(t *testing.T) {
	t.Parallel()

	target := reflect.TypeFor[secret.String]()

	out, err := secret.DecodeHook(reflect.TypeFor[string](), reflect.TypeFor[string](), hunter)
	require.NoError(t, err)
	assert.Equal(t, hunter, out, "other target types pass through")

	out, err = secret.DecodeHook(reflect.TypeFor[string](), target, "")
	require.NoError(t, err)
	assert.Equal(t, secret.String{}, out)

	_, err = secret.DecodeHook(reflect.TypeFor[map[string]any](), target, map[string]any{"value": hunter})
	require.ErrorIs(t, err, secret.ErrDecode)
	assert.NotContains(t, err.Error(), hunter)
}

func TestString_TextRoundTripIsRedacted(t *testing.T) {
	t.Parallel()

	text, err := secret.New(hunter).MarshalText()
	require.NoError(t, err)
	assert.Equal(t, redacted, string(text))

	var back secret.String

	require.NoError(t, back.UnmarshalText([]byte("  spaced \n")))
	assert.Equal(t, "  spaced \n", back.Expose(), "the text is taken as is")
	require.NoError(t, back.UnmarshalText(nil))
	assert.True(t, back.IsZero())
}

// FuzzString: whatever the value, Expose returns it and no rendering does.
func FuzzString(f *testing.F) {
	for _, seed := range []string{hunter, "", "%s%v%d", "[redacted:f52fbd32]", "\x00\xff", `"quoted"`, "redacted"} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, value string) {
		s := secret.New(value)
		if s.Expose() != value {
			t.Fatalf("Expose() = %q, want %q", s.Expose(), value)
		}

		if !s.Equal(value) && value != "" {
			t.Fatalf("Equal(%q) = false", value)
		}

		want := "[redacted:" + fingerprint(value) + "]"
		if value == "" {
			want = ""
		}

		jsonOut, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}

		yamlOut, err := yaml.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}

		renderings := map[string]string{
			"%v":   fmt.Sprintf("%v", s),
			"%+v":  fmt.Sprintf("%+v", s),
			"%#v":  fmt.Sprintf("%#v", s),
			"%s":   fmt.Sprintf("%s", s),
			"%x":   fmt.Sprintf("%x", s),
			"%d":   fmt.Sprintf("%d", s),
			"slog": s.LogValue().String(),
			"json": strings.Trim(string(jsonOut), `"`),
			"yaml": strings.Trim(strings.TrimSpace(string(yamlOut)), `'"`),
		}
		for name, got := range renderings {
			if got != want {
				t.Fatalf("%s rendered %q for value %q, want %q", name, got, value, want)
			}
		}
	})
}
