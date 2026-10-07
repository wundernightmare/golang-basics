package httpx_test

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func redactJSON(t testing.TB, v any) (map[string]any, string) {
	t.Helper()
	b, err := json.Marshal(httpx.Redact(v))
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m, string(b)
}

func TestRedact_HidesSecretsByTagNameAndURL(t *testing.T) {
	type Nested struct {
		Password string `json:"password"`
		Host     string `json:"host"`
	}
	type cfg struct {
		Plain       string            `yaml:"plain"`
		Tagged      string            `yaml:"tagged" secret:"true"`
		EmptyTagged string            `yaml:"empty_tagged" secret:"true"`
		APIKey      string            // by name
		MaxTokens   int               `yaml:"max_tokens"` // by name too — over-redaction is the safe side
		NotSecret   string            `yaml:"public_token" secret:"false"`
		DatabaseURL string            `yaml:"database_url"`
		Brokers     []string          `yaml:"brokers"`
		Timeout     time.Duration     `yaml:"timeout"`
		Ptr         *Nested           `yaml:"ptr"`
		Nil         *Nested           `yaml:"nil"`
		Extra       map[string]string `yaml:"extra"`
		Blob        []byte            `yaml:"blob"`
		Skipped     string            `yaml:"-"`
		unexported  string
		Started     time.Time `yaml:"started"`
		Nested      `yaml:",inline"`
	}
	in := cfg{
		Plain: "v", Tagged: "s", APIKey: "k", MaxTokens: 3, NotSecret: "shown",
		DatabaseURL: "postgres://app:hunter2@db:5432/app?sslmode=disable",
		Brokers:     []string{"kafka://u:p@b1:9092", "b2:9092"},
		Timeout:     10 * time.Second,
		Ptr:         &Nested{Password: "p", Host: "h"},
		Extra:       map[string]string{"client_secret": "x", "region": "eu"},
		Blob:        []byte("raw"),
		Skipped:     "no", unexported: "no",
		Started: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Nested:  Nested{Password: "inline", Host: "ih"},
	}
	m, raw := redactJSON(t, in)

	assert.Equal(t, "v", m["plain"])
	assert.Equal(t, "[redacted]", m["tagged"])
	assert.Empty(t, m["empty_tagged"], "unset stays visibly unset")
	assert.Equal(t, "[redacted]", m["APIKey"])
	assert.Equal(t, "[redacted]", m["max_tokens"])
	assert.Equal(t, "shown", m["public_token"], `secret:"false" opts out of the name rule`)
	assert.Equal(t, "postgres://app:xxxxx@db:5432/app?sslmode=disable", m["database_url"])
	assert.Equal(t, []any{"kafka://u:xxxxx@b1:9092", "b2:9092"}, m["brokers"])
	assert.Equal(t, "10s", m["timeout"])
	assert.Equal(t, map[string]any{"password": "[redacted]", "host": "h"}, m["ptr"])
	assert.Nil(t, m["nil"])
	assert.Equal(t, map[string]any{"client_secret": "[redacted]", "region": "eu"}, m["extra"])
	assert.Equal(t, "cmF3", m["blob"], "a non-secret []byte is a blob, left to encoding/json")
	assert.NotContains(t, m, "Skipped")
	assert.NotContains(t, m, "unexported")
	assert.Equal(t, "2026-01-02T03:04:05Z", m["started"])
	assert.Equal(t, "[redacted]", m["password"], "embedded struct fields are inlined")
	assert.Equal(t, "ih", m["host"])
	assert.NotContains(t, raw, "hunter2")
}

// Field names that carry a whole connection string or credential blob are
// secrets as a whole, not just their URL password.
func TestRedact_SecretFieldNames(t *testing.T) {
	type cfg struct {
		DSN        string `yaml:"dsn"`
		Auth       string `yaml:"auth"`
		BasicAuth  string `yaml:"basic_auth"`
		ClientCert string `yaml:"client_cert"`
		DBPwd      string `yaml:"db_pwd"`
		Secret     []byte `yaml:"secret"`
		Credential struct {
			User string `yaml:"user"`
		} `yaml:"credential"`
		Host string `yaml:"host"`
	}
	in := cfg{DSN: "host=db password=x", Auth: "Basic abc", BasicAuth: "u:p", ClientCert: "/etc/tls.crt", DBPwd: "p", Secret: []byte("k"), Host: "db"}
	in.Credential.User = "app"
	m, _ := redactJSON(t, in)
	for _, k := range []string{"dsn", "auth", "basic_auth", "client_cert", "db_pwd", "secret"} {
		assert.Equal(t, "[redacted]", m[k], k)
	}
	assert.Equal(t, map[string]any{"user": "[redacted]"}, m["credential"], "a secret struct redacts every non-empty leaf")
	assert.Equal(t, "db", m["host"])
}

func TestRedact_KeywordDSN(t *testing.T) {
	for in, want := range map[string]string{
		"host=db user=app password=s3cret dbname=app":       "host=db user=app password=[redacted] dbname=app",
		"password='with space' host=db":                     "password=[redacted] host=db",
		`host=db password='it\'s' sslmode=require`:          "host=db password=[redacted] sslmode=require",
		"host=db PASSWORD=Upper":                            "host=db PASSWORD=[redacted]",
		"host=db sslpassword=k user=app":                    "host=db sslpassword=[redacted] user=app",
		"host=db user=app":                                  "host=db user=app",
		"a=b":                                               "a=b",
		"mypassword=x is not a keyword DSN pair on its own": "mypassword=x is not a keyword DSN pair on its own",
	} {
		assert.Equal(t, want, httpx.Redact(in), in)
	}
}

func TestRedact_SecretQueryParameters(t *testing.T) {
	out, ok := httpx.Redact("postgres://app@db:5432/app?sslmode=require&sslpassword=k&token=t&api_key=a&x-auth=z").(string)
	require.True(t, ok)
	u, err := url.Parse(out)
	require.NoError(t, err)
	q := u.Query()
	assert.Equal(t, "require", q.Get("sslmode"), "ordinary parameters are kept")
	for _, k := range []string{"sslpassword", "token", "api_key", "x-auth"} {
		assert.Equal(t, "[redacted]", q.Get(k), k)
	}
	assert.Equal(t, "app", u.User.Username())

	// Both at once: userinfo password and a secret parameter.
	out, _ = httpx.Redact("https://u:p@h/x?secret=s&page=2").(string)
	u, err = url.Parse(out)
	require.NoError(t, err)
	pw, _ := u.User.Password()
	assert.Equal(t, "xxxxx", pw)
	assert.Equal(t, "[redacted]", u.Query().Get("secret"))
	assert.Equal(t, "2", u.Query().Get("page"))

	// Nothing secret: the string is returned byte for byte.
	assert.Equal(t, "https://h/x?b=2&a=1", httpx.Redact("https://h/x?b=2&a=1"))
}

func TestRedact_PassesScalarsAndNilThrough(t *testing.T) {
	assert.Nil(t, httpx.Redact(nil))
	assert.Equal(t, 42, httpx.Redact(42))
	assert.Equal(t, "plain", httpx.Redact("plain"))
	assert.Equal(t, "http://u:xxxxx@h/", httpx.Redact("http://u:p@h/"))
	assert.Equal(t, "http://u@h/", httpx.Redact("http://u@h/"), "a user without a password is left alone")
	assert.Equal(t, "not a url with @", httpx.Redact("not a url with @"))

	// Strings net/url rejects still lose their password.
	for in, want := range map[string]string{
		"http://u:p%zz@h/":                 "http://u:xxxxx@h/",
		"kafka://u:pa ss@b:9092,b2:9092":   "kafka://u:xxxxx@b:9092,b2:9092",
		"redis://:p@ss w@cache:6379/0":     "redis://:xxxxx@cache:6379/0",
		"http://host:port-is-not-a-number": "http://host:port-is-not-a-number",
	} {
		assert.Equal(t, want, httpx.Redact(in), in)
	}
	assert.Equal(t, "1m30s", httpx.Redact(90*time.Second))
}
