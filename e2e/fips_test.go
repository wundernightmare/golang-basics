//go:build e2e

package e2e

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The FIPS 140-3 build mode (docs/RUNTIME.md "FIPS 140-3") is a property of
// the build line, so it is proven here on binaries this file builds itself
// through scripts/build-service.sh — the binaries in E2E_BIN_DIR are whatever
// the caller built. What the process then reports is the Go Cryptographic
// Module's own answer (crypto/fips140.Enabled), not a flag the build stamps.

// buildService runs scripts/build-service.sh for svc into out and returns its
// exit code and output. The build knobs under test are taken only from env:
// whatever the caller's environment (or a CI matrix) set for them is dropped.
func buildService(t *testing.T, svc, out string, env ...string) (int, string) {
	t.Helper()

	script, err := filepath.Abs(filepath.Join("..", "scripts", "build-service.sh"))
	require.NoError(t, err)

	cmd := exec.Command(script, svc, out) // the repo's own build script

	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "FIPS", "GOFIPS140_VERSION", "GOFIPS140", "KAFKA_PROVIDER":
		default:
			cmd.Env = append(cmd.Env, kv)
		}
	}

	cmd.Env = append(cmd.Env, env...)

	output, err := cmd.CombinedOutput()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return exit.ExitCode(), string(output)
	}

	require.NoError(t, err, string(output))

	return 0, string(output)
}

// TestFIPSBuild: a FIPS=1 build of ping runs with the Go Cryptographic Module
// in FIPS mode and says so on /version and in its start-up line; the same
// build line without the knob does not.
func TestFIPSBuild(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		env  []string
		want bool
	}{
		{name: "FIPS=1", env: []string{"FIPS=1"}, want: true},
		{name: "FIPS unset", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bin := filepath.Join(t.TempDir(), "ping")
			code, out := buildService(t, "ping", bin, tc.env...)
			require.Equal(t, 0, code, "build-service.sh ping: %s", out)

			sp := pingSpec()
			sp.bin = bin
			s := start(t, sp)

			bi := s.version(t)
			require.NotNil(t, bi.FIPS140, "/version has no fips140 field")
			assert.Equal(t, tc.want, *bi.FIPS140, "/version fips140")

			var starting map[string]any

			for _, l := range s.logLines() {
				if l["msg"] == "ping starting" {
					starting = l
				}
			}

			require.NotNil(t, starting, "no \"ping starting\" log line")
			assert.Equal(t, tc.want, starting["fips140"], "start-up line fips140")
		})
	}
}

// TestFIPSBuildRefusesRdkafka: librdkafka brings OpenSSL through cgo, which
// Go's FIPS module does not cover, so the build script refuses FIPS=1 for a
// service that would link it — before compiling anything — instead of
// producing a binary that reports FIPS mode over non-FIPS crypto.
func TestFIPSBuildRefusesRdkafka(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		env  []string
	}{
		{name: "KAFKA_PROVIDER=rdkafka", env: []string{"FIPS=1", "KAFKA_PROVIDER=rdkafka"}},
		{name: "the default provider", env: []string{"FIPS=1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bin := filepath.Join(t.TempDir(), "tasks")
			code, out := buildService(t, "tasks", bin, tc.env...)
			assert.Equal(t, 2, code, out)
			assert.Contains(t, out, "FIPS=1 needs KAFKA_PROVIDER=franz")
			assert.NoFileExists(t, bin, "a refused build leaves no binary")
		})
	}
}
