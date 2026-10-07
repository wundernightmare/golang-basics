package httpx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"reflect"
	"time"

	"github.com/caarlos0/env/v11"
	"go.yaml.in/yaml/v3"
)

// LoadYAML populates dst from an optional YAML file, overlays environment
// variables on top, and finally fills whatever is still unset from the
// struct's envDefault tags. Precedence, highest first:
//
//	environment variable  >  YAML file value  >  envDefault tag  >  zero value
//
// A missing file at path is not an error (the service falls back to env-only),
// so the same binary runs from a mounted config.yaml in production and from
// bare env vars in a test or container. Unknown YAML keys are an error: a
// typo must not silently become a default.
//
// Defaults come from the envDefault tags, so a service config can embed the
// libs' Config structs directly instead of copying fields by hand:
//
//	type Config struct {
//		httpx.Config `yaml:",inline"`
//		Postgres pgx.Config `yaml:"postgres" envPrefix:"DB_"`
//	}
//
// An explicit zero in the file or the environment is kept ("shutdown_delay:
// 0s" is zero, not the default); only a key that is absent from both takes
// the default. A value dst already holds before the call (a service's own
// default for a lib field, e.g. its admin port) counts as the default for
// that field: the tag default does not replace it, the file and the
// environment do.
func LoadYAML[T any](path, prefix string, dst *T) error {
	// 1. Defaults: the envDefault tags parsed against an empty environment,
	//    copied into every field dst still has at zero. Done first so that an
	//    explicit zero in the file or the environment ("shutdown_delay: 0s",
	//    SLOW_REQUEST=0) is a value, not "unset"; only into zero fields so a
	//    service can pre-set its own defaults (":9082", "tasks") before the
	//    call and have the operator's values layered on top of them.
	var defaults T
	if err := env.ParseWithOptions(&defaults, env.Options{Prefix: prefix, Environment: map[string]string{}}); err != nil {
		return fmt.Errorf("httpx: env defaults (prefix %q): %w", prefix, err)
	}
	fillZero(reflect.ValueOf(dst).Elem(), reflect.ValueOf(&defaults).Elem())

	// 2. The file, when there is one: yaml only touches the keys it finds.
	if path != "" {
		data, err := os.ReadFile(path) //nolint:gosec // path is operator-supplied config, not user input
		switch {
		case err == nil:
			dec := yaml.NewDecoder(bytes.NewReader(data))
			dec.KnownFields(true)
			if err := dec.Decode(dst); err != nil && !errors.Is(err, io.EOF) { // an empty file is an empty config
				return fmt.Errorf("httpx: parse yaml %q: %w", path, err)
			}
		case errors.Is(err, fs.ErrNotExist):
			// No file — env-only configuration, which is valid.
		default:
			return fmt.Errorf("httpx: read yaml %q: %w", path, err)
		}
	}

	// 3. The environment, only the variables that are actually set (defaults
	//    were applied in step 1, so an absent variable leaves the file's value).
	if err := env.ParseWithOptions(dst, env.Options{Prefix: prefix, DefaultValueTagName: "envDefaultDisabled"}); err != nil {
		return fmt.Errorf("httpx: env overlay (prefix %q): %w", prefix, err)
	}
	return nil
}

// fillZero copies every field of def into dst that dst has at its zero value,
// descending into nested and embedded structs.
func fillZero(dst, def reflect.Value) {
	for i := range dst.NumField() {
		f := dst.Field(i)
		if !f.CanSet() {
			continue
		}
		d := def.Field(i)
		if f.Kind() == reflect.Struct && f.Type() != reflect.TypeFor[time.Time]() {
			fillZero(f, d)
			continue
		}
		if f.IsZero() && !d.IsZero() {
			f.Set(d)
		}
	}
}
