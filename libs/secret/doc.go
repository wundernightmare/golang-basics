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
// for mapstructure, and with it for viper.
//
// The package imports the standard library only.
package secret
