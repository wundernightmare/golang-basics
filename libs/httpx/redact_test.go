package httpx_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/testx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func TestRedact_HidesSecretsByTagNameAndURL(t *testing.T) {
	testx.Run(t, func(t testx.T) {
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
			DSN         string            `yaml:"dsn"`
			Brokers     []string          `yaml:"brokers"`
			Timeout     time.Duration     `yaml:"timeout"`
			Ptr         *Nested           `yaml:"ptr"`
			Nil         *Nested           `yaml:"nil"`
			Extra       map[string]string `yaml:"extra"`
			Skipped     string            `yaml:"-"`
			unexported  string
			Started     time.Time `yaml:"started"`
			Nested      `yaml:",inline"`
		}
		in := cfg{
			Plain: "v", Tagged: "s", APIKey: "k", MaxTokens: 3, NotSecret: "shown",
			DSN:     "postgres://app:hunter2@db:5432/app?sslmode=disable",
			Brokers: []string{"kafka://u:p@b1:9092", "b2:9092"},
			Timeout: 10 * time.Second,
			Ptr:     &Nested{Password: "p", Host: "h"},
			Extra:   map[string]string{"client_secret": "x", "region": "eu"},
			Skipped: "no", unexported: "no",
			Started: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			Nested:  Nested{Password: "inline", Host: "ih"},
		}
		b, err := json.Marshal(httpx.Redact(in))
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, json.Unmarshal(b, &m))

		assert.Equal(t, "v", m["plain"])
		assert.Equal(t, "[redacted]", m["tagged"])
		assert.Empty(t, m["empty_tagged"], "unset stays visibly unset")
		assert.Equal(t, "[redacted]", m["APIKey"])
		assert.Equal(t, "[redacted]", m["max_tokens"])
		assert.Equal(t, "shown", m["public_token"], `secret:"false" opts out of the name rule`)
		assert.Equal(t, "postgres://app:xxxxx@db:5432/app?sslmode=disable", m["dsn"])
		assert.Equal(t, []any{"kafka://u:xxxxx@b1:9092", "b2:9092"}, m["brokers"])
		assert.Equal(t, "10s", m["timeout"])
		assert.Equal(t, map[string]any{"password": "[redacted]", "host": "h"}, m["ptr"])
		assert.Nil(t, m["nil"])
		assert.Equal(t, map[string]any{"client_secret": "[redacted]", "region": "eu"}, m["extra"])
		assert.NotContains(t, m, "Skipped")
		assert.NotContains(t, m, "unexported")
		assert.Equal(t, "2026-01-02T03:04:05Z", m["started"])
		assert.Equal(t, "[redacted]", m["password"], "embedded struct fields are inlined")
		assert.Equal(t, "ih", m["host"])
		assert.NotContains(t, string(b), "hunter2")
	}, "httpx", "unit")
}

func TestRedact_PassesScalarsAndNilThrough(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		assert.Nil(t, httpx.Redact(nil))
		assert.Equal(t, 42, httpx.Redact(42))
		assert.Equal(t, "plain", httpx.Redact("plain"))
		assert.Equal(t, "http://u:xxxxx@h/", httpx.Redact("http://u:p@h/"))
		assert.Equal(t, "not a url with @", httpx.Redact("not a url with @"))
	}, "httpx", "unit")
}
