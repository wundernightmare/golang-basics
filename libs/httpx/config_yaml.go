package httpx

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/caarlos0/env/v11"
	"gopkg.in/yaml.v3"
)

// LoadYAML populates dst from an optional YAML file and then overlays
// environment variables on top, giving services a config-file story alongside
// the pure-env [LoadConfig]. Precedence, highest first:
//
//	environment variable  >  YAML file value  >  struct's zero value / defaults
//
// A missing file at path is not an error (the service falls back to env-only),
// so the same binary runs from a mounted config.yaml in production and from
// bare env vars in a test or container.
//
// Make this precedence work by giving dst's fields `yaml:"…"` tags and `env:"…"`
// tags WITHOUT an `envDefault` — apply code defaults after loading (e.g. a
// withDefaults() method) so an absent env var leaves the YAML value intact
// rather than resetting it to a default. See services/tasks/internal/config for
// a worked example.
func LoadYAML[T any](path, prefix string, dst *T) error {
	if path != "" {
		data, err := os.ReadFile(path) //nolint:gosec // path is operator-supplied config, not user input
		switch {
		case err == nil:
			if err := yaml.Unmarshal(data, dst); err != nil {
				return fmt.Errorf("httpx: parse yaml %q: %w", path, err)
			}
		case errors.Is(err, fs.ErrNotExist):
			// No file — env-only configuration, which is valid.
		default:
			return fmt.Errorf("httpx: read yaml %q: %w", path, err)
		}
	}

	if err := env.ParseWithOptions(dst, env.Options{Prefix: prefix}); err != nil {
		return fmt.Errorf("httpx: env overlay (prefix %q): %w", prefix, err)
	}
	return nil
}
