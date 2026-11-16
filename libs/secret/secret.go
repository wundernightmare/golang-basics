package secret

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strconv"
)

// fingerprintBytes is how much of the SHA-256 digest a rendering shows.
const fingerprintBytes = 4

// ErrDecode is returned by [DecodeHook] for a value that is not text, and by
// [String.UnmarshalYAML] for a node that is not a scalar. It never carries
// the value itself.
var ErrDecode = errors.New("secret: cannot decode into secret.String")

// String is a secret text value: a password, a token, a key. The zero value
// is "not set".
//
// It renders as "[redacted:1a2b3c4d]" — the marker and its
// [String.Fingerprint] — and an unset one as "", so "unset" stays visible,
// through fmt, log/slog, encoding/json, YAML and any other
// [encoding.TextMarshaler] consumer; [String.Expose] returns the value.
//
// The value sits behind a pointer on purpose: fmt cannot call methods on an
// unexported struct field, so a String kept in one would otherwise be printed
// raw by %v of the enclosing struct. With the pointer fmt prints an address.
// The price is that == compares identity, not content: compare with
// [String.Equal], or reflect.DeepEqual in tests.
//
// A String is not a vault: the bytes stay in process memory, a debugger or a
// reflection walk that follows pointers (go-spew, a test diff) reads them.
// It guards against the accident, not against the process.
type String struct {
	value *string
}

// New wraps value. An empty value is the zero String.
func New(value string) String {
	if value == "" {
		return String{value: nil}
	}

	return String{value: &value}
}

// Expose returns the secret. This is the only way to it: call it where the
// value is handed to what needs it (a driver, a header), not earlier.
func (s String) Expose() string {
	if s.value == nil {
		return ""
	}

	return *s.value
}

// IsZero reports whether the secret is unset. encoding/json consults it for
// `omitzero` and go.yaml.in/yaml for `omitempty` — not the other way round:
// `json:",omitempty"` keeps an unset secret as "", and go.yaml.in/yaml v3.0.6
// panics on a tag with `omitzero`.
func (s String) IsZero() bool { return s.value == nil }

// Equal reports whether the secret is candidate. It compares the SHA-256
// digests of the two with [subtle.ConstantTimeCompare], so the answer takes
// as long whether they differ in the first byte, the last, or in length. Hashing itself
// takes time proportional to each value's own length (per 64-byte block),
// which is the one thing timing can still tell. An unset secret equals
// nothing, the empty string included: no credential must not mean "any
// credential".
func (s String) Equal(candidate string) bool {
	if s.value == nil {
		return false
	}

	want, got := sha256.Sum256([]byte(*s.value)), sha256.Sum256([]byte(candidate))

	return subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

// Fingerprint returns the first four bytes of the SHA-256 of the secret, in
// hex ("" when unset) — what `printf %s "$SECRET" | sha256sum | cut -c1-8`
// prints. It tells whether a secret changed across a rotation, or whether two
// instances hold the same one, without showing it.
//
// It is unsalted on purpose: salted, it could not be compared between
// processes. The cost is that 32 bits of a digest confirm an offline guess,
// so a fingerprint in a log makes a weak, guessable password checkable
// against a dictionary. For generated tokens and keys that is no information
// at all; a human-chosen password should not be a service secret anyway.
func (s String) Fingerprint() string {
	if s.value == nil {
		return ""
	}

	digest := sha256.Sum256([]byte(*s.value))

	return hex.EncodeToString(digest[:fingerprintBytes])
}

// String implements [fmt.Stringer]: "[redacted:<fingerprint>]", or "" when
// unset.
func (s String) String() string {
	if s.value == nil {
		return ""
	}

	return "[redacted:" + s.Fingerprint() + "]"
}

// Format implements [fmt.Formatter], so that no verb reaches the value: %d
// or %x of a plain Stringer would print the struct underneath.
func (s String) Format(state fmt.State, verb rune) {
	text := s.String()
	if verb == 'q' {
		text = strconv.Quote(text)
	}

	_, _ = io.WriteString(state, text)
}

// LogValue implements [slog.LogValuer].
func (s String) LogValue() slog.Value { return slog.StringValue(s.String()) }

// MarshalText implements [encoding.TextMarshaler] — what encoding/json and
// YAML render a String with. It is redacted too: a config that is marshalled
// is a config that is about to be shown. The output therefore does not
// round-trip; to write a secret out, write [String.Expose].
func (s String) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText implements [encoding.TextUnmarshaler] — what encoding/json,
// caarlos0/env (envDefault included), flag.TextVar and a YAML library
// without [String.UnmarshalYAML] read a String through. The text is taken as
// is, surrounding whitespace included — and so is "[redacted:1a2b3c4d]": a
// marshalled config read back holds the marker as its secret.
func (s *String) UnmarshalText(text []byte) error {
	*s = New(string(text))

	return nil
}

// UnmarshalYAML is what a YAML library calls instead of UnmarshalText when it
// knows this method (go.yaml.in/yaml v2, v3 and v4 do). It asks the library
// for the node as a string, so a scalar is taken as its text, and a mapping
// or a sequence is [ErrDecode]. Through UnmarshalText alone a mapping is no
// error in v2 and v3 unless the decoder is strict: it is matched against a
// struct with no exported fields, and the secret stays unset.
//
// The library's own error is dropped, not wrapped: goccy/go-yaml v1.19.2
// quotes the offending lines of the document in it, and a mapping written
// where a secret belongs is likely to hold the secret.
func (s *String) UnmarshalYAML(unmarshal func(any) error) error {
	var text string

	if err := unmarshal(&text); err != nil {
		return fmt.Errorf("%w: not a YAML scalar", ErrDecode)
	}

	*s = New(text)

	return nil
}

// DecodeHook is a mapstructure decode hook that turns a string (or []byte)
// into a [String]. It has the shape of mapstructure.DecodeHookFuncType
// without importing it; with viper:
//
//	err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
//		secret.DecodeHook,
//		mapstructure.StringToTimeDurationHookFunc(),
//	)))
//
// Values bound for any other type pass through untouched.
func DecodeHook(from, to reflect.Type, data any) (any, error) {
	if to != reflect.TypeFor[String]() {
		return data, nil
	}

	switch value := data.(type) {
	case String:
		return value, nil
	case string:
		return New(value), nil
	case []byte:
		return New(string(value)), nil
	default:
		return nil, fmt.Errorf("%w: got %s", ErrDecode, from)
	}
}
