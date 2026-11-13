package httpx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// logged builds the real logger over a buffer and returns it with a reader of
// the one line it wrote: parsed, and raw.
func logged(t *testing.T, cfg httpx.LogConfig) (*slog.Logger, func() (map[string]any, string)) {
	t.Helper()

	var buf bytes.Buffer

	cfg.Writer = &buf

	return httpx.NewLogger(cfg), func() (map[string]any, string) {
		t.Helper()

		raw := strings.TrimSpace(buf.String())
		require.NotContains(t, raw, "\n", "one record, one line")

		var line map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &line), raw)

		return line, raw
	}
}

type account struct {
	Login    string `json:"login"`
	Password string `json:"password"`
	Endpoint string `json:"endpoint"`
}

// Every way a secret can ride into a log record as an attribute: under a key
// that names it, inside a URL or a DSN under a key that does not, in an
// error's text, in a header, in a map, in a struct, in a group. None of the
// values may reach the line; what is not secret must.
func TestLogger_MasksPrivateDataInAttributes(t *testing.T) {
	t.Parallel()

	log, line := logged(t, httpx.LogConfig{RedactKeys: []string{"email", "Phone"}})

	secrets := []string{
		"hunter2", "tok-9f8e7d", "dsn-pass", "url-pass", "q-secret", "err-pass", "bearer-abc", "sid=42", "map-pass",
		"struct-pass", "grp-pass", "typed-pass", "a@b.example", "+15550100", "hdr-key",
	}

	log.Info("user signed in with password hunter (the message is the caller's business)",
		"password", "hunter2",
		"api_key", "tok-9f8e7d",
		"email", "a@b.example",
		"phone", "+15550100",
		"database", "host=db user=app password=dsn-pass sslmode=disable",
		"upstream", "https://svc:url-pass@billing.internal/v1?token=q-secret&page=2",
		"err", fmt.Errorf("dial postgres://app:err-pass@db:5432/app: %w", errors.New("refused")),
		"request_headers", http.Header{"Authorization": {"Bearer bearer-abc"}, "Cookie": {"sid=42"}, "X-Api-Key": {"hdr-key"}, "Accept": {"application/json"}},
		"labels", map[string]string{"region": "eu", "client_secret": "map-pass"},
		"account", account{Login: "app", Password: "struct-pass", Endpoint: "https://api.example/v1"},
		slog.Group("db", slog.String("password", "grp-pass"), slog.String("host", "db-1")),
		"credential", secret.New("typed-pass"),
		// Not secrets, whatever their names suggest:
		"debug_token", true,
		"max_tokens", 4096,
		"token_ttl", 90*time.Second,
		"peer", netip.MustParseAddr("10.0.0.7"),
		"callback", &url.URL{Scheme: "https", Host: "example.org", Path: "/hook"},
		"empty_password", "",
	)

	got, raw := line()
	for _, s := range secrets {
		assert.NotContains(t, raw, s)
	}

	assert.Equal(t, "[redacted]", got["password"])
	assert.Equal(t, "[redacted]", got["api_key"])
	assert.Equal(t, "[redacted]", got["email"], "a key from LOG_REDACT_KEYS")
	assert.Equal(t, "[redacted]", got["phone"], "matched without regard to case")
	assert.Equal(t, "host=db user=app password=[redacted] sslmode=disable", got["database"])
	assert.Equal(t, "https://svc:xxxxx@billing.internal/v1?page=2&token=%5Bredacted%5D", got["upstream"])
	assert.Equal(t, "dial postgres://app:xxxxx@db:5432/app: refused", got["err"])
	assert.Equal(t, map[string]any{
		"Authorization": []any{"[redacted]"}, "Cookie": []any{"[redacted]"}, "X-Api-Key": []any{"[redacted]"},
		"Accept": []any{"application/json"},
	}, got["request_headers"])
	assert.Equal(t, map[string]any{"region": "eu", "client_secret": "[redacted]"}, got["labels"])
	assert.Equal(t, map[string]any{"login": "app", "password": "[redacted]", "endpoint": "https://api.example/v1"}, got["account"])
	assert.Equal(t, map[string]any{"password": "[redacted]", "host": "db-1"}, got["db"])
	assert.Equal(t, "[redacted]", got["credential"])

	assert.Equal(t, true, got["debug_token"], "a boolean carries no secret")
	assert.InDelta(t, 4096, got["max_tokens"], 0)
	assert.InDelta(t, float64(90*time.Second), got["token_ttl"], 0)
	assert.Equal(t, "10.0.0.7", got["peer"], "a type that renders itself is left to")
	assert.Equal(t, "https://example.org/hook", got["callback"])
	assert.Empty(t, got["empty_password"], "unset stays visibly unset")
	assert.Contains(t, got["msg"], "password hunter", "the message is not rewritten")
}

// LOG_FORMAT=ecs: the same record under Elastic Common Schema names, so an
// Elastic pipeline needs no rename processor; nothing is left under the
// default names.
func TestLogger_ECSFormat(t *testing.T) {
	t.Parallel()

	spans := testx.Recorder(t)
	log, line := logged(t, httpx.LogConfig{Format: "ecs", Service: "billing"})

	ctx, span := spans.Tracer().Start(context.Background(), "op")
	log.ErrorContext(ctx, "charge failed",
		"err", errors.New("card declined"),
		"method", "POST", "path", "/charges", "status", 502,
		"stack", "goroutine 1 [running]",
		"attempt", 3,
		slog.Group("card", slog.String("status", "expired")),
	)
	span.End()

	got, raw := line()

	at, err := time.Parse(time.RFC3339Nano, got["@timestamp"].(string))
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), at, time.Minute)
	assert.Equal(t, "error", got["log.level"], "lowercase, as ECS has it")
	assert.Equal(t, "charge failed", got["message"])
	assert.Equal(t, "8.11.0", got["ecs.version"])
	assert.Equal(t, "billing", got["service.name"])
	assert.Equal(t, span.SpanContext().TraceID().String(), got["trace.id"])
	assert.Equal(t, span.SpanContext().SpanID().String(), got["span.id"])
	assert.Equal(t, "card declined", got["error.message"])
	assert.Equal(t, "goroutine 1 [running]", got["error.stack_trace"])
	assert.Equal(t, "POST", got["http.request.method"])
	assert.Equal(t, "/charges", got["url.path"])
	assert.InDelta(t, 502, got["http.response.status_code"], 0)
	assert.InDelta(t, 3, got["attempt"], 0, "an attribute ECS has no name for keeps its own")
	assert.Equal(t, map[string]any{"status": "expired"}, got["card"], "inside a group nothing is renamed")

	for _, legacy := range []string{`"time"`, `"level"`, `"msg"`, `"trace_id"`, `"span_id"`, `"service"`, `"err"`} {
		assert.NotContains(t, raw, legacy+":")
	}
}

// The default JSON format keeps its field names: masking is the only thing
// the rewriter does to it.
func TestLogger_JSONFormatKeepsItsNames(t *testing.T) {
	t.Parallel()

	log, line := logged(t, httpx.LogConfig{Service: "billing"})
	log.Info("ok", "status", 200)

	got, _ := line()
	assert.Equal(t, "INFO", got["level"])
	assert.Equal(t, "ok", got["msg"])
	assert.Equal(t, "billing", got["service"])
	assert.Contains(t, got, "time")
	assert.NotContains(t, got, "ecs.version")
}

func TestConfig_LogFormatAndRedactKeys(t *testing.T) {
	t.Parallel()

	var cfg httpx.Config
	require.NoError(t, httpx.LoadYAMLFrom("", "SVC_", map[string]string{
		"SVC_LOG_FORMAT": "ecs", "SVC_LOG_REDACT_KEYS": "email,phone",
	}, &cfg))

	if err := cfg.Validate(); err != nil {
		require.NotContains(t, err.Error(), "LOG_FORMAT", "ecs is a known format")
	}

	assert.Equal(t, []string{"email", "phone"}, cfg.LogConfig().RedactKeys)
	assert.Equal(t, "ecs", cfg.LogConfig().Format)

	cfg.LogFormat = "logfmt"
	require.ErrorContains(t, cfg.Validate(), "LOG_FORMAT")
}

func BenchmarkLogger_SixAttributes(b *testing.B) {
	log := httpx.NewLogger(httpx.LogConfig{Writer: bytes.NewBuffer(make([]byte, 0, 1<<20)), Service: "bench"})
	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		log.InfoContext(ctx, "request", "method", "GET", "path", "/tasks/42", "status", 200,
			"duration_ms", 12, "client", "10.0.0.7", "route", "/tasks/{id}")
	}
}
