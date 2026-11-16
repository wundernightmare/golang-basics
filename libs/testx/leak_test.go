package testx_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/testx"
)

func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }

// leakEnv turns TestLeaksAGoroutine, in a child run of this binary, into a
// test that passes and leaves a goroutine behind.
const leakEnv = "TESTX_LEAK_A_GOROUTINE"

func TestLeaksAGoroutine(t *testing.T) {
	t.Parallel()

	if os.Getenv(leakEnv) == "" {
		t.Skip("the child half of TestMainFailsTheBinaryOnALeakedGoroutine")
	}

	go func() { select {} }()
}

// The binary's own TestMain is testx.Main: a child run whose only test leaks
// must exit 1 although that test passed.
func TestMainFailsTheBinaryOnALeakedGoroutine(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLeaksAGoroutine$")

	cmd.Env = append(os.Environ(), leakEnv+"=1")

	out, err := cmd.CombinedOutput()

	var exit *exec.ExitError

	require.ErrorAs(t, err, &exit, "output:\n%s", out)
	assert.Equal(t, 1, exit.ExitCode())
	assert.Contains(t, string(out), "goroutines leaked by the tests")
	assert.NotContains(t, string(out), "--- FAIL", "the test itself passed")
}
