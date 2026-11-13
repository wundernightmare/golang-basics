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
| `encoding/json`, YAML (`go.yaml.in/yaml/v3`) | `encoding.TextMarshaler` | `"[redacted:f52fbd32]"` | `""`, or absent with `omitzero` / `omitempty` |
| `httpx.Redact` (`/admin/config`), `--config-reference` | the `Expose` + `IsZero` shape | `[redacted:f52fbd32]` / `(secret)` | `""` / `-` |

An unset secret renders empty on purpose: "the password is missing" is the
thing an operator looks for in a dumped config.

Marshalling is redacted too, so the output does **not** round-trip. A config
that is being marshalled is a config about to be shown; to write a secret out
(a generated file, a request body) write `Expose()` explicitly.

## What reads it

| Source | Through |
| --- | --- |
| `encoding/json`, YAML, `caarlos0/env` (`env` and `envDefault`), `flag.TextVar` — and with them `httpx.LoadYAML` | `encoding.TextUnmarshaler` |
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

The text is taken as is — no trimming. A secret read from a file with a
trailing newline keeps it; trim at the place that reads the file.

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

`Equal` is constant-time and independent of both lengths (it compares SHA-256
digests). An unset secret equals nothing, `""` included. `==` on two `String`s
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
with the secret in unexported fields; both slog handlers; JSON, YAML, env,
`flag`, mapstructure; `FuzzString` checks that for any value `Expose` returns
it and no rendering does (`go test -fuzz FuzzString`).
