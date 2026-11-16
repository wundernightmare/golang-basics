# secret

`secret.String` marks a configuration value as a secret **in its type**, so the
config that holds it can be logged, printed, marshalled and served by
`/admin/config` as it is. Standard library only.

```go
type Config struct {
    Host     string        `env:"DB_HOST"     yaml:"host"`
    Password secret.String `env:"DB_PASSWORD" yaml:"password"`
}

log.Info("boot", "config", cfg)        // password=[redacted:f52fbd32]
fmt.Printf("%+v", cfg)                 // {Host:db Password:[redacted:f52fbd32]}
json.Marshal(cfg)                      // {"host":"db","password":"[redacted:f52fbd32]"}

connect(cfg.Host, cfg.Password.Expose()) // the one way to the value
```

`Expose()` is the only accessor: every place a secret leaves its wrapper is a
call that `grep -rn 'Expose()'` finds and a reviewer sees.

## What renders it, and how

| Consumer | Through | Set | Unset |
| --- | --- | --- | --- |
| `fmt` — every verb (`%v %+v %#v %s %q %x %d` …) | `fmt.Formatter` | `[redacted:f52fbd32]` | empty |
| `log/slog` — attribute, group, inside a logged struct | `slog.LogValuer`, `MarshalText` | `[redacted:f52fbd32]` | empty |
| `encoding/json`, YAML (`go.yaml.in/yaml/v3`) | `encoding.TextMarshaler` | `"[redacted:f52fbd32]"` | `""`, or absent with `json:",omitzero"` / `yaml:",omitempty"` |
| `httpx.Redact` (`/admin/config`), `--config-reference` | the `Expose` + `IsZero` shape | `[redacted:f52fbd32]` / `(secret)` | `""` / `-` |

An unset secret renders empty on purpose: "the password is missing" is the
thing an operator looks for in a dumped config.

The two tags are not interchangeable: `json:",omitempty"` leaves an unset
secret in the output as `""`, and `go.yaml.in/yaml` (v3.0.6) panics on a tag
that says `omitzero`.

Two shapes render differently from what the table says:

- a nil `*secret.String` passed to `slog` as an attribute of its own is logged
  as `LogValue panicked` with a stack — no value is shown, there is none;
  `fmt` prints `<nil>` and JSON `null` for it;
- a struct that **embeds** `secret.String` takes over its methods, so the
  whole struct prints and marshals as `[redacted:…]` and its other fields
  disappear. Give the secret a field name.

Marshalling is redacted too, so the output does **not** round-trip. A config
that is being marshalled is a config about to be shown; to write a secret out
(a generated file, a request body) write `Expose()` explicitly. Reading the
output back is not refused: the secret is then the text `[redacted:f52fbd32]`
itself.

## What reads it

| Source | Through |
| --- | --- |
| `encoding/json`, `caarlos0/env` (`env` and `envDefault`), `flag.TextVar` | `encoding.TextUnmarshaler` |
| YAML (`go.yaml.in/yaml/v3`) — and with it `httpx.LoadYAML` | `UnmarshalYAML` |
| mapstructure, viper | `secret.DecodeHook` |

```go
err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
    secret.DecodeHook, // no mapstructure import in this package: it is a plain func
    mapstructure.StringToTimeDurationHookFunc(),
    mapstructure.StringToSliceHookFunc(","),
)))
```

Passing `viper.DecodeHook` replaces viper's two default hooks, hence the last
two lines. The hook is tested against `go-viper/mapstructure/v2`, the decoder
`viper.Unmarshal` runs; viper itself is not a dependency of the tests. Viper's
own store is outside the type: `v.AllSettings()` and `v.Debug()` print what was
loaded, raw.

The text is taken as is — no trimming.

In YAML a secret is a scalar, read as its text: `password: 012345` is
`"012345"`. A mapping or a sequence in its place is an error
(`secret.ErrDecode`) with or without `KnownFields(true)`; the error does not
say which line, because the library's own message may quote the document.
That is `go.yaml.in/yaml` (v2, v3, v4). Other YAML libraries resolve an
unquoted scalar before handing it over — `goccy/go-yaml` (v1.19.2) turns
`012345` into `"5349"` — so with them quote the secret.

## Mounted files

What a pod mounts instead of putting it in the environment is read through
the package too, so every client means the same thing by the same kind of
file:

- `secret.ReadFile(path)` — a password or token file, as a `String`: the
  whole file without the line break an editor or `echo` leaves at the end;
  a file that is missing or holds nothing is an error (`ErrEmptyFile`), not
  an empty secret. `DB_PASSWORD_FILE` and `KAFKA_SASL_PASSWORD_FILE` are
  read with it.
- `secret.CertPool(path)` — a PEM bundle of certificate authorities, as the
  pool to verify a dependency with. The pool is the file and nothing else:
  a CA file names whom to trust, the system roots are what a client uses
  without one. `KAFKA_SSL_CA_LOCATION` (franz-go), `S3_CA_FILE` and
  `OTEL_EXPORTER_OTLP_CERTIFICATE` are read with it. The file is read whole
  or not at all: one damaged block — cut short, not base64, a `CERTIFICATE`
  that does not parse — is `ErrMalformedBundle` even when the other
  certificates are sound, so a bundle with the old and the new authority
  does not load with only one of them. Text between the blocks and blocks of
  another type are passed over; no certificate at all is `ErrNoCertificate`.

## Fingerprint

A set secret renders as `[redacted:f52fbd32]`: the hex is `Fingerprint()`, the
first four bytes of its SHA-256 — the same thing
`printf %s "$SECRET" | sha256sum | cut -c1-8` prints. It answers "did the
rotation reach this pod?" and "do these two instances hold the same key?" from
a log line or `/admin/config`, without showing the secret.

It is unsalted on purpose — salted per process it could not be compared
between instances. The cost: 32 bits of a digest are enough to confirm an
offline guess, so a logged fingerprint makes a *weak, guessable* password
checkable against a dictionary. For generated tokens and keys it reveals
nothing; do not use human-chosen passwords as service secrets.

## Comparing

```go
if !cfg.AdminToken.Equal(presented) { /* 401 */ }
```

`Equal` compares SHA-256 digests with `subtle.ConstantTimeCompare`: it takes
as long whether the values differ in the first byte, the last, or in length
(hashing a value takes time proportional to its own length, nothing more is
told). No test holds this: timing is not something a functional test sees,
and a plain `==` in its place passes the suite — it is kept by review. An unset
secret equals nothing, `""` included. `==` on two `String`s
compares identity, not content — use `Equal`, or `reflect.DeepEqual` /
`assert.Equal` in tests.

## What it does not do

- **It is not a vault.** The bytes are ordinary Go memory: a core dump, a
  debugger, `unsafe`, or a reflection walk that follows pointers (`go-spew`,
  the diff testify prints for a failed `assert.Equal`) reads them. Nothing is
  zeroed on drop — Go strings cannot be.
- **It ends at `Expose()`.** `fmt.Sprintf("postgres://app:%s@db", p.Expose())`
  is a plain string again; `httpx.Redact` still masks URL passwords, a log
  line does not.
- **Strings only.** A secret map is `map[string]secret.String`; a binary key is
  its text encoding.

## Why a pointer inside

`fmt` cannot call methods on unexported struct fields, so a wrapper that keeps
the value inline (`struct{ v string }`) is printed raw by `%v` of any struct
holding it in an unexported field. `String` keeps the value behind a pointer:
there `fmt` prints an address. One small allocation per secret, at config load.

## Tests

`just mod libs/secret test` — every fmt verb on a value, a pointer and a struct
with the secret in unexported fields; both slog handlers; JSON, `flag`, the
decode hook; `FuzzString` checks that for any value `Expose` returns it and no
rendering does (`go test -fuzz FuzzString`).

[`compat/`](compat) is a module of its own with the tests against YAML,
`caarlos0/env` and mapstructure: those libraries are that suite's
dependencies, and this module's `go.mod` stays at the standard library (plus
testify).

`just mutate libs/secret` (from the repo root, nightly in CI) mutates the
package and expects every mutant gremlins makes to be noticed: a surviving
one would be a way for a value to leak that no test sees. Its mutators do
not include swapping the constant-time comparison for `==` (see Comparing).
