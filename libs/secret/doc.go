// Package secret marks a configuration value as a secret in the type system.
//
// A [String] holds a password, token or key. Everything that renders a value
// — fmt with any verb, log/slog, encoding/json, YAML, a text marshaller —
// shows "[redacted:1a2b3c4d]" instead of it (the hex is a short fingerprint
// that changes when the secret does), so a config struct can be logged, dumped
// or served by an admin endpoint as it is. The one way to the value is
// [String.Expose], which makes every use of a secret a line that can be
// grepped for and reviewed.
//
// Reading goes the other way through the same standard interface: a String
// is an [encoding.TextUnmarshaler], which is what encoding/json, YAML,
// caarlos0/env and flag.TextVar decode through; [DecodeHook] does the same
// for mapstructure, and with it for viper. Text that is itself a rendering
// ("[redacted:1a2b3c4d]") is refused on all of them with [ErrDecode].
//
// "YAML" here is go.yaml.in/yaml/v3, the library the compat suite runs.
// Rendering goes through MarshalText in any library that honours it; reading
// an unquoted scalar is the library's business: goccy/go-yaml v1.19.2 hands
// over `012345` as "5349" and `1.50` as "1.5". With a library other than
// go.yaml.in/yaml, quote the secret in the document.
//
// [ReadFile] and [CertPool] read what a pod mounts instead: a secret in a
// file, and the CA bundle that usually sits next to it.
//
// The package imports the standard library only.
package secret
