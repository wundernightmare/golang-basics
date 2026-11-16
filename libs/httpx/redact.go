package httpx

import (
	"encoding"
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const redactedValue = "[redacted]"

// secretFieldName matches field / map-key names that are secrets by
// convention, so a config struct is safe to expose even where its author
// forgot the tag. Explicit `secret:"true"` / `secret:"false"` tags override.
// It covers pass-through client properties too, where a key holds inline key
// material rather than a path: librdkafka's ssl.key.pem, ssl.keystore.*,
// sasl.oauthbearer.assertion.private.key.{pem,passphrase}.
var secretFieldName = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|token|api[_-]?key|private[_.-]?key|credential|dsn|auth|cert|key[_.-]?pem|keystore|passphrase)`)

// secretParam matches URL query parameters and keyword-DSN keys whose value
// is a secret (?sslpassword=…, ?token=…, "host=db password=x"): a name that
// ends in one of the long words (X-Amz-Signature), or is one of the short
// ones of a presigned URL or an OAuth redirect — whole, since "code" and
// "key" end too many ordinary names (status_code).
var secretParam = regexp.MustCompile(`(?i)^(.*(password|passwd|pwd|secret|token|api[_-]?key|private[_-]?key|credential|sslkey|auth|signature)|sig|key|code)$`)

// dsnPair matches one "key=value" of a libpq keyword/value DSN.
var dsnPair = regexp.MustCompile(`(?i)\b(password|passwd|pwd|sslpassword)=('(?:[^'\\]|\\.)*'|\S+)`)

// typedSecret is a value that is a secret by its type rather than by a tag or
// a name: libs/secret's String. It is matched by shape, so this package (and
// every module that imports it) does not depend on that one. Expose is never
// called here — it only identifies the type.
type typedSecret interface {
	Expose() string
	IsZero() bool
}

// secretPlaceholder is "" for an unset typed secret and "[redacted]" for a
// set one — "[redacted:1a2b3c4d]" when the type offers a fingerprint, so an
// operator sees on /admin/config whether a rotation reached this instance.
func secretPlaceholder(s typedSecret) string {
	if s.IsZero() {
		return ""
	}

	if f, ok := s.(interface{ Fingerprint() string }); ok {
		if fp := f.Fingerprint(); isHex(fp) {
			return "[redacted:" + fp + "]"
		}
	}

	return redactedValue
}

// isHex guards the one thing taken from a typed secret: a short hex digest
// prefix is shown, anything else is not.
func isHex(s string) bool {
	const maxLen = 16
	if s == "" || len(s) > maxLen {
		return false
	}

	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}

	return true
}

// Redact returns a copy of v suitable for showing to an operator — what
// GET /admin/config serves. It walks structs, pointers, slices and maps and:
//
//   - replaces the value of any struct field or map entry whose name looks
//     like a secret — it contains password, secret, token, api key, private
//     key, credential, dsn, auth, cert, keystore or passphrase; the pattern
//     is secretFieldName, and a field is matched by its Go name and by the
//     name it is shown under — or whose field carries `secret:"true"`, with
//     "[redacted]" (an empty value stays empty, so "unset" remains visible);
//     `secret:"false"` opts a field out. Such a value is replaced whole: a
//     field named DSN shows "[redacted]", not a DSN with its password masked;
//   - does the same for a value that is a secret by type (libs/secret's
//     String), wherever it sits and whatever its tags say;
//   - rewrites, in any other string, the userinfo password of every URL in
//     it (postgres://app:s3cret@host → postgres://app:xxxxx@host), the
//     secret-looking query and fragment parameters of those URLs
//     (?sslpassword=, ?token=, ?X-Amz-Signature=, ?sig=, ?key=, ?code=), and the password pair of a libpq keyword DSN
//     ("host=db password=x");
//   - renders time.Duration as its string ("10s"), not nanoseconds, and a
//     struct with a text form of its own (url.URL, an encoding.TextMarshaler
//     such as netip.Addr) as that text, under the rules for strings;
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
		if t, ok := reflect.TypeAssert[time.Time](rv); ok {
			return t
		}

		if s, ok := reflect.TypeAssert[typedSecret](rv); ok {
			return secretPlaceholder(s)
		}

		if text, ok := ownText(rv); ok {
			return redactString(text, secret)
		}

		return redactStruct(rv, secret)
	case reflect.Slice, reflect.Array:
		return redactList(rv, secret)
	case reflect.Map:
		return redactMap(rv, secret)
	case reflect.String:
		return redactString(rv.String(), secret)
	case reflect.Int64:
		if rv.Type() == reflect.TypeFor[time.Duration]() {
			return time.Duration(rv.Int()).String()
		}
	default: // other scalars (and an Int64 that is not a Duration): below
	}

	if secret && !rv.IsZero() {
		return redactedValue
	}

	return rv.Interface()
}

// ownText returns the text form of a struct that has one — an address, a
// prefix, a URL: taken apart field by field it shows nothing (netip.Addr has
// no exported fields) or too much (url.URL keeps its query in RawQuery).
func ownText(rv reflect.Value) (string, bool) {
	if u, ok := reflect.TypeAssert[url.URL](rv); ok {
		return u.String(), true
	}

	if m, ok := reflect.TypeAssert[encoding.TextMarshaler](rv); ok {
		if text, err := m.MarshalText(); err == nil {
			return string(text), true
		}
	}

	return "", false
}

// redactList renders a slice or array; a []byte is a blob, never an
// operator-readable value.
func redactList(rv reflect.Value, secret bool) any {
	if rv.Kind() == reflect.Slice && rv.IsNil() {
		return nil
	}

	if rv.Type().Elem().Kind() == reflect.Uint8 {
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
}

// redactMap renders a map with string keys; a key that names a secret
// redacts its value.
func redactMap(rv reflect.Value, secret bool) any {
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
}

// redactString hides a secret (an empty one stays empty: "unset" is not a
// secret) and strips credentials from anything URL-shaped.
func redactString(s string, secret bool) any {
	if secret {
		if s == "" {
			return ""
		}

		return redactedValue
	}

	return redactURL(s)
}

// redactStruct renders a struct as a map. inherited is true when the struct
// itself sits under a secret name or tag: every field is then secret, as the
// elements of a secret slice or map are.
func redactStruct(rv reflect.Value, inherited bool) map[string]any {
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
			maps.Copy(out, redactStruct(rv.Field(i), inherited))

			continue
		}

		secret := inherited || secretFieldName.MatchString(f.Name) || secretFieldName.MatchString(name)
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

	if s, ok := reflect.TypeAssert[interface{ String() string }](k); ok {
		return s.String()
	}

	return fmt.Sprint(k.Interface())
}

// urlStart matches where a URL begins inside a text: a scheme and "//", or
// a bare "//" (a reference without a scheme has an authority too).
var urlStart = regexp.MustCompile(`(?:[A-Za-z][A-Za-z0-9+.\-]*:)?//`)

// urlEnd is what ends a URL inside a text: whitespace, a quote, a bracket.
const urlEnd = " \t\r\n\"'`<>"

// passwordMask replaces a userinfo password. Not redactedValue: a reader
// that parses the result (net/url) rejects brackets in userinfo.
const passwordMask = "xxxxx"

// redactURL masks credentials wherever they stand in s — a config value, an
// error's text: the userinfo password and the secret-looking query and
// fragment parameters of every URL in it, and the password pair of a keyword
// DSN outside them. Everything else comes back byte for byte, and a string
// with nothing to mask is returned as it is.
func redactURL(s string) string {
	if !strings.Contains(s, "://") {
		return redactDSN(s)
	}

	var out strings.Builder

	// A string that is one URL from its first byte is a configured value,
	// where a password may hold what would end a URL inside a text.
	whole := strings.Count(s, "://") == 1

	rest := s
	for {
		at := urlStart.FindStringIndex(rest)
		if at == nil {
			break
		}

		masked, end := redactURLAt(rest, at[0], at[1], whole && at[0] == 0 && out.Len() == 0)

		out.WriteString(redactDSN(rest[:at[0]]))
		out.WriteString(masked)

		rest = rest[end:]
	}

	out.WriteString(redactDSN(rest))

	return out.String()
}

func redactDSN(s string) string {
	if strings.Contains(s, "=") && dsnPair.MatchString(s) {
		return dsnPair.ReplaceAllString(s, "$1="+redactedValue)
	}

	return s
}

// redactURLAt masks the URL of s that starts at start and whose authority
// starts at authority, and returns it with the index in s where it ends.
// The URL runs to the first byte of urlEnd; with whole set, its authority
// does not stop there (a password with a space in it).
func redactURLAt(s string, start, authority int, whole bool) (masked string, end int) {
	stops := "/?#" + urlEnd
	if whole {
		stops = "/?#"
	}

	authorityEnd := len(s)
	if i := strings.IndexAny(s[authority:], stops); i >= 0 {
		authorityEnd = authority + i
	}

	end = len(s)
	if i := strings.IndexAny(s[authorityEnd:], urlEnd); i >= 0 {
		end = authorityEnd + i
	}

	host, tail := s[authority:authorityEnd], s[authorityEnd:end]

	if at := strings.LastIndexByte(host, '@'); at >= 0 {
		if user, _, has := strings.Cut(host[:at], ":"); has {
			host = user + ":" + passwordMask + host[at:]
		}
	} else if at := strings.LastIndexByte(tail, '@'); at >= 0 && strings.Contains(host, ":") {
		// "scheme://user:pa/ss@host": a password with a raw '/', '?' or '#'
		// ends the authority early. net/url tells that from a URL with an
		// '@' in its path — "https://host:443/pkg@1" parses, this does not.
		if _, err := url.Parse(s[start:end]); err != nil {
			user, _, _ := strings.Cut(host, ":")

			return s[start:authority] + user + ":" + passwordMask + redactURLTail(tail[at:]), end
		}
	}

	return s[start:authority] + host + redactURLTail(tail), end
}

// redactURLTail masks the query and the fragment of what follows an
// authority; a fragment is treated as a query because tokens travel there in
// that form (#access_token=…).
func redactURLTail(tail string) string {
	rest, fragment, hasFragment := strings.Cut(tail, "#")
	path, query, hasQuery := strings.Cut(rest, "?")

	if hasQuery {
		path += "?" + redactRawQuery(query)
	}

	if hasFragment {
		path += "#" + redactRawQuery(fragment)
	}

	return path
}

// redactRawQuery replaces the value of every secret-looking parameter of an
// encoded query ("a=1&token=x") and leaves the rest of it as it was written.
func redactRawQuery(query string) string {
	if !strings.Contains(query, "=") {
		return query
	}

	pairs := strings.Split(query, "&")
	changed := false

	for i, pair := range pairs {
		key, value, _ := strings.Cut(pair, "=")
		if value == "" {
			continue
		}

		name, err := url.QueryUnescape(key)
		if err != nil {
			name = key
		}

		if secretParam.MatchString(name) {
			pairs[i] = key + "=" + redactedValue
			changed = true
		}
	}

	if !changed {
		return query
	}

	return strings.Join(pairs, "&")
}
