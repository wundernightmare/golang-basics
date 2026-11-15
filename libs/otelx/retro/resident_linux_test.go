package retro

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// residentChildEnv makes the test binary measure itself instead of asserting:
// the measurement needs a process whose heap is as small as a service's is
// when it calls otelx.Init, and a test binary that ran other tests is not one
// (memory the runtime reuses is zeroed, and zeroing makes it resident).
const residentChildEnv = "RETRO_RESIDENT_CHILD"

// residentReport is the line the child prints, and the parent reads.
const residentReport = "retro-resident reserved=%d filled=%d\n"

// residentBytes is the resident set size of this process.
func residentBytes(t *testing.T) int {
	t.Helper()

	raw, err := os.ReadFile("/proc/self/statm")
	require.NoError(t, err)

	fields := strings.Fields(string(raw))
	require.Greater(t, len(fields), 1)

	pages, err := strconv.Atoi(fields[1])
	require.NoError(t, err)

	return pages * os.Getpagesize()
}

// The buffer is allocated at start whatever the sampling ratio, and a service
// that samples everything never writes to it. That costs address space and
// not memory: the pages of a fresh allocation are not resident until written.
// The second number is what makes the first one mean something — the same
// measurement does see the ring once spans fill it.
func TestProcessor_AnEmptyBufferIsNotResident(t *testing.T) { //nolint:paralleltest // the child measures its own process: nothing else may run in it
	if raceEnabled {
		t.Skip("the race detector shadows the memory it watches")
	}

	if os.Getenv(residentChildEnv) != "" {
		before := residentBytes(t)
		proc := New(&sink{}, Config{})
		reserved := residentBytes(t) - before

		// Several budgets of spans, over every shard.
		for i := range 400_000 {
			proc.OnEnd(spanOf(i, byte(i), "GET /tasks/{id}", false))
		}

		require.Positive(t, proc.arena.usage().evicted, "the ring went round")
		fmt.Printf(residentReport, reserved, residentBytes(t)-before) //nolint:forbidigo // the report to the parent process

		return
	}

	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")

	child.Env = append(os.Environ(), residentChildEnv+"=1")

	out, err := child.CombinedOutput()
	require.NoError(t, err, string(out))

	var reserved, filled int

	found := false

	for line := range strings.Lines(string(out)) {
		if n, _ := fmt.Sscanf(line, residentReport, &reserved, &filled); n == 2 {
			found = true
		}
	}

	require.True(t, found, string(out))

	budget := DefaultBufferMiB * mib

	assert.Less(t, reserved, budget/2, "an unused buffer is address space, not memory")
	assert.Greater(t, filled, budget*3/4, "a full buffer is resident: the measurement sees the ring")
}
