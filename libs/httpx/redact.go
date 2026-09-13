package httpx

import (
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// redactedValue replaces a secret in [Redact] output.
const redactedValue = "[redacted]"

// secretFieldName matches field / map-key names that are secrets by
// convention, so a config struct is safe to expose even where its author
// forgot the tag. Explicit `secret:"true"` / `secret:"false"` tags override.
var secretFieldName = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|private[_-]?key|credential)`)

// Redact returns a copy of v suitable for showing to an operator — what
// GET /admin/config serves. It walks structs, pointers, slices and maps and:
//
//   - replaces the value of any struct field or map entry whose name matches
//     password / secret / token / api key / private key / credential, or whose
//     field carries `secret:"true"`, with "[redacted]" (an empty value stays
//     empty, so "unset" remains visible); `secret:"false"` opts a field out;
//   - rewrites the password in any string that parses as a URL with
//     userinfo (postgres://app:s3cret@host → postgres://app:xxxxx@host);
//   - renders time.Duration as its string ("10s"), not nanoseconds;
//   - names struct fields by their yaml tag, then json tag, then Go name, so
//     the output reads like the config file the operator wrote.
//
// Unexported fields are skipped. Anything else is returned as is for
// encoding/json to render.
func Redact(v any) any {
	if v == nil {
		return nil
	}
	return redactValue(reflect.ValueOf(v), false)
}

func redactValue(rv reflect.Value, secret bool) any {
	if !rv.IsValid() {
		return nil
	}
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return redactValue(rv.Elem(), secret)
	case reflect.Struct:
		if t, ok := rv.Interface().(time.Time); ok {
			return t
		}
		return redactStruct(rv)
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return nil
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 { // []byte: a blob, never an operator-readable value
			if secret && rv.Len() > 0 {
				return redactedValue
			}
			return rv.Interface()
		}
		out := make([]any, rv.Len())
		for i := range rv.Len() {
			out[i] = redactValue(rv.Index(i), secret)
		}
		return out
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := keyString(iter.Key())
			out[key] = redactValue(iter.Value(), secret || secretFieldName.MatchString(key))
		}
		return out
	case reflect.String:
		s := rv.String()
		if secret {
			if s == "" {
				return ""
			}
			return redactedValue
		}
		return redactURL(s)
	case reflect.Int64:
		if rv.Type() == reflect.TypeFor[time.Duration]() {
			return time.Duration(rv.Int()).String()
		}
	}
	if secret && !rv.IsZero() {
		return redactedValue
	}
	return rv.Interface()
}

func redactStruct(rv reflect.Value) map[string]any {
	rt := rv.Type()
	out := make(map[string]any, rt.NumField())
	for i := range rt.NumField() {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		name := fieldName(f)
		if name == "-" {
			continue
		}
		if f.Anonymous && f.Type.Kind() == reflect.Struct && !hasNameTag(f) {
			// Embedded struct without its own name: inline its fields, as
			// encoding/json does.
			for k, v := range redactStruct(rv.Field(i)) {
				out[k] = v
			}
			continue
		}
		secret := secretFieldName.MatchString(f.Name)
		switch f.Tag.Get("secret") {
		case "true":
			secret = true
		case "false":
			secret = false
		}
		out[name] = redactValue(rv.Field(i), secret)
	}
	return out
}

func hasNameTag(f reflect.StructField) bool {
	for _, tag := range []string{"yaml", "json"} {
		if n, _, _ := strings.Cut(f.Tag.Get(tag), ","); n != "" && n != "-" {
			return true
		}
	}
	return false
}

func fieldName(f reflect.StructField) string {
	for _, tag := range []string{"yaml", "json"} {
		if n, _, _ := strings.Cut(f.Tag.Get(tag), ","); n != "" {
			return n
		}
	}
	return f.Name
}

func keyString(k reflect.Value) string {
	if k.Kind() == reflect.String {
		return k.String()
	}
	if s, ok := k.Interface().(interface{ String() string }); ok {
		return s.String()
	}
	return fmt.Sprint(k.Interface())
}

// redactURL masks the password of a URL with userinfo and returns any other
// string unchanged.
func redactURL(s string) string {
	if !strings.Contains(s, "://") || !strings.Contains(s, "@") {
		return s
	}
	u, err := url.Parse(s)
	if err != nil || u.User == nil {
		return s
	}
	if _, has := u.User.Password(); !has {
		return s
	}
	return u.Redacted()
}
