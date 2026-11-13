package httpx

import (
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// ecsVersion is the Elastic Common Schema version the "ecs" log format
// follows.
const ecsVersion = "8.11.0"

// ecsKeys maps the attribute names this workspace logs with to their ECS
// field. Only top-level attributes are renamed: a field inside a group keeps
// the name its call site gave it.
var ecsKeys = map[string]string{ //nolint:gochecknoglobals // read-only lookup table (Go has no const maps)
	slog.TimeKey:    "@timestamp",
	slog.LevelKey:   "log.level",
	slog.MessageKey: "message",
	"service":       "service.name",
	"trace_id":      "trace.id",
	"span_id":       "span.id",
	"request_id":    "http.request.id",
	"method":        "http.request.method",
	"status":        "http.response.status_code",
	"path":          "url.path",
	"client":        "client.address",
	"error":         "error.message",
	"err":           "error.message",
	"stack":         "error.stack_trace",
	"panic":         "error.type",
}

// maxCachedKeys bounds the per-key sensitivity cache: attribute keys are a
// small fixed set in a sane program, and a program that derives keys from
// input must not grow the cache with them.
const maxCachedKeys = 4096

// attrRewriter is the logger's slog ReplaceAttr: it masks private data in
// every attribute and, in the ECS format, renames the well-known ones.
//
// Masking is by key and by shape, the same rules as [Redact]:
//
//   - an attribute whose key names a secret (password, token, secret, api
//     key, credential, authorization, …) or is listed in LogConfig.RedactKeys
//     is logged as "[redacted]" — strings, errors and composite values;
//     numbers, booleans, durations and times pass, they carry no secret;
//   - a string (or an error's text) that is a URL with credentials, or a
//     keyword DSN with a password, has them masked wherever it appears;
//   - headers and string maps get the same treatment entry by entry, structs
//     go through [Redact].
//
// The message itself is not rewritten: do not format secrets into it.
type attrRewriter struct {
	ecs   bool
	extra map[string]struct{}

	sensitive sync.Map // key → bool
	cached    atomic.Int64
}

func newAttrRewriter(ecs bool, extraKeys []string) *attrRewriter {
	rw := &attrRewriter{ecs: ecs, extra: make(map[string]struct{}, len(extraKeys))}
	for _, k := range extraKeys {
		if k = strings.ToLower(strings.TrimSpace(k)); k != "" {
			rw.extra[k] = struct{}{}
		}
	}

	return rw
}

func (rw *attrRewriter) isSensitive(key string) bool {
	if v, ok := rw.sensitive.Load(key); ok {
		return v.(bool) //nolint:forcetypeassert // only bools are stored
	}

	_, listed := rw.extra[strings.ToLower(key)]
	sensitive := listed || secretFieldName.MatchString(key)

	if rw.cached.Load() < maxCachedKeys {
		if _, loaded := rw.sensitive.LoadOrStore(key, sensitive); !loaded {
			rw.cached.Add(1)
		}
	}

	return sensitive
}

func (rw *attrRewriter) replace(groups []string, attr slog.Attr) slog.Attr {
	builtin := len(groups) == 0 && (attr.Key == slog.TimeKey || attr.Key == slog.LevelKey || attr.Key == slog.MessageKey)

	if !builtin {
		attr.Value = rw.mask(attr.Key, attr.Value)
	}

	if rw.ecs && len(groups) == 0 {
		if attr.Key == slog.LevelKey {
			attr.Value = slog.StringValue(strings.ToLower(attr.Value.String()))
		}

		if renamed, ok := ecsKeys[attr.Key]; ok {
			attr.Key = renamed
		}
	}

	return attr
}

// mask returns v with private data removed, or v itself when there is none.
func (rw *attrRewriter) mask(key string, v slog.Value) slog.Value {
	switch v.Kind() {
	case slog.KindString:
		s := v.String()
		if s == "" {
			return v
		}

		if rw.isSensitive(key) {
			return slog.StringValue(redactedValue)
		}

		if masked := redactURL(s); masked != s {
			return slog.StringValue(masked)
		}

		return v
	case slog.KindAny:
		return rw.maskAny(key, v)
	default: // numbers, booleans, durations, times; groups arrive attribute by attribute
		return v
	}
}

func (rw *attrRewriter) maskAny(key string, v slog.Value) slog.Value {
	anyValue := v.Any()
	if anyValue == nil {
		return v
	}

	if rw.isSensitive(key) {
		return slog.StringValue(redactedValue)
	}

	switch typed := anyValue.(type) {
	case error:
		text := typed.Error()
		if masked := redactURL(text); masked != text {
			return slog.StringValue(masked)
		}

		return v
	case *url.URL:
		return slog.StringValue(redactURL(typed.String()))
	case url.URL:
		return slog.StringValue(redactURL(typed.String()))
	case http.Header:
		out := make(map[string][]string, len(typed))
		for name, values := range typed {
			if rw.isSensitive(name) || strings.EqualFold(name, "Cookie") || strings.EqualFold(name, "Set-Cookie") {
				out[name] = []string{redactedValue}

				continue
			}

			out[name] = values
		}

		return slog.AnyValue(out)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for name, value := range typed {
			out[name] = rw.mask(name, slog.StringValue(value)).String()
		}

		return slog.AnyValue(out)
	case fmt.Stringer, json.Marshaler, encoding.TextMarshaler:
		return v // the type renders itself; taking it apart field by field would change what is logged
	}

	if kind := reflect.Indirect(reflect.ValueOf(anyValue)).Kind(); kind == reflect.Struct || kind == reflect.Map {
		return slog.AnyValue(Redact(anyValue))
	}

	return v
}
