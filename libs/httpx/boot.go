package httpx

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
	"time"
)

// Exit statuses of a service binary (sysexits.h), so a supervisor can tell
// "fix the deployment" from "the process crashed": a crash-looping pod that
// exits 78 needs its configuration changed, not a restart.
const (
	ExitUsage  = 64 // EX_USAGE: an unknown flag or a stray argument
	ExitConfig = 78 // EX_CONFIG: the configuration does not load or validate
)

// Boot is the configuration half of a service's main: it handles the
// command line every binary shares, loads the configuration, and says how
// to exit when the process should not go on.
//
//	--help              what configures the binary and these flags
//	--check-config      load and validate, print "config ok" or the problems,
//	                    exit 0 or 78 — a deploy pipeline's pre-flight
//	--config-reference  every key: env name, YAML path, default (secrets
//	                    marked), from the struct tags — never out of date
//
// Typical use:
//
//	func main() {
//		cfg, code, ok := boot.Config(os.Args[1:], os.Stdout, os.Stderr)
//		if !ok {
//			os.Exit(code)
//		}
//		if err := run(cfg); err != nil {
//			os.Exit(1)
//		}
//	}
type Boot[T any] struct {
	Name     string            // binary name, for --help and messages
	Prefix   string            // environment prefix of every key, e.g. "TASKS_"
	File     string            // variable naming the optional YAML file; "" = environment only
	Load     func() (T, error) // loads and validates the configuration
	Defaults func() (T, error) // the configuration with nothing set
}

// Config parses args (without the program name) and loads the
// configuration. ok is false when the process is done: exit with code —
// 0 after --help, --config-reference or a passing --check-config,
// [ExitUsage] for a bad command line, [ExitConfig] for a configuration that
// does not load. Problems go to stderr; there is no logger yet, its settings
// are part of what failed.
func (b Boot[T]) Config(args []string, stdout, stderr io.Writer) (cfg T, code int, ok bool) {
	fs := flag.NewFlagSet(b.Name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	check := fs.Bool("check-config", false, "load and validate the configuration, then exit (0 valid, 78 not)")
	reference := fs.Bool("config-reference", false, "print every configuration key with its YAML path and default, then exit")
	fs.Usage = func() {} // printed below: to stdout for --help, not after a mistake
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			b.usage(stdout, fs)
			return cfg, 0, false
		}
		_, _ = fmt.Fprintf(stderr, "%s: see --help\n", b.Name) // the flag package has said what was wrong
		return cfg, ExitUsage, false
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "%s: unexpected argument %q (see --help)\n", b.Name, fs.Arg(0))
		return cfg, ExitUsage, false
	}

	if *reference {
		def, err := b.Defaults()
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", b.Name, err)
			return cfg, ExitConfig, false
		}
		ConfigReference(stdout, b.Prefix, def)
		return cfg, 0, false
	}

	cfg, err := b.Load()
	if err != nil {
		msg := err.Error()
		if !strings.HasPrefix(msg, b.Name+":") {
			msg = b.Name + ": " + msg
		}
		_, _ = fmt.Fprintln(stderr, msg)
		return cfg, ExitConfig, false
	}
	if *check {
		_, _ = fmt.Fprintln(stdout, "config ok")
		return cfg, 0, false
	}
	return cfg, 0, true
}

func (b Boot[T]) usage(w io.Writer, fs *flag.FlagSet) {
	_, _ = fmt.Fprintf(w, "Usage: %s [--check-config | --config-reference | --help]\n\n", b.Name)
	source := "environment variables " + b.Prefix + "*"
	if b.File != "" {
		source += ", over the optional YAML file named by " + b.File
	}
	_, _ = fmt.Fprintf(w, "Configured by %s (--config-reference lists every key).\n\nFlags:\n", source)
	fs.SetOutput(w)
	fs.PrintDefaults()
}

// ConfigReference writes one line per configuration key of cfg — the
// environment name (with prefix), the YAML path and the value cfg holds
// (pass the defaults) — derived from the env/yaml/envPrefix struct tags, the
// same ones [LoadYAML] reads. A field tagged secret:"true" shows "(secret)"
// instead of a value.
func ConfigReference(w io.Writer, prefix string, cfg any) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ENV\tYAML\tDEFAULT")
	walkConfig(reflect.ValueOf(cfg), prefix, "", func(env, yaml, def string) {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", env, yaml, def)
	})
	_ = tw.Flush()
}

func walkConfig(v reflect.Value, envPrefix, yamlPrefix string, emit func(env, yaml, def string)) {
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		yamlName, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		inline := strings.Contains(f.Tag.Get("yaml"), "inline")
		if yamlName == "" {
			yamlName = strings.ToLower(f.Name)
		}
		envName, _, _ := strings.Cut(f.Tag.Get("env"), ",")
		if envName == "" && f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeFor[time.Time]() {
			yp := yamlPrefix + yamlName + "."
			if inline {
				yp = yamlPrefix
			}
			walkConfig(v.Field(i), envPrefix+f.Tag.Get("envPrefix"), yp, emit)
			continue
		}
		if envName == "" {
			continue
		}
		yamlPath := yamlPrefix + yamlName
		if yamlName == "-" {
			yamlPath = "-"
		}
		emit(envPrefix+envName, yamlPath, formatDefault(v.Field(i), f.Tag.Get("secret") == "true"))
	}
}

// formatDefault renders a value for the reference: "-" for an empty string
// or list (unset), "(secret)" for a set secret.
func formatDefault(v reflect.Value, secret bool) string {
	switch {
	case v.IsZero() && (v.Kind() == reflect.String || v.Kind() == reflect.Slice):
		return "-"
	case secret && !v.IsZero():
		return "(secret)"
	}
	switch x := v.Interface().(type) {
	case time.Duration:
		return x.String()
	case []string:
		return strings.Join(x, ",")
	default:
		return fmt.Sprint(x)
	}
}
