package containers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// halfStarted is a container that only counts how often it is terminated.
type halfStarted struct {
	testcontainers.Container

	terminated int
}

func (c *halfStarted) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminated++

	return nil
}

// stopsHere is a testing.TB whose Skipf and Fatalf end the call, as the real
// ones do, without ending the test that looks at the outcome.
type stopsHere struct {
	testing.TB

	skipped, failed bool
}

type stopped struct{}

func (s *stopsHere) Skipf(string, ...any) { s.skipped = true; panic(stopped{}) }

func (s *stopsHere) Fatalf(string, ...any) { s.failed = true; panic(stopped{}) }

// A starter that fails hands the container back with the error, and use
// removes it after every attempt: without Ryuk nothing else would. No Docker
// is involved — the container is a fake.
func TestUseRemovesAHalfStartedContainer(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("use skips every caller under -short")
	}

	var (
		holder shared[string]
		ctr    halfStarted
	)

	fake := &stopsHere{TB: t}
	attempts := 0

	func() {
		defer func() {
			_, ok := recover().(stopped)
			require.True(t, ok, "use ends through Skipf or Fatalf when the start fails")
		}()

		use(fake, &holder, "fake", func(context.Context) (string, testcontainers.Container, error) {
			attempts++

			return "", &ctr, errors.New("did not come up")
		})
	}()

	assert.Equal(t, 3, attempts)
	assert.Equal(t, attempts, ctr.terminated, "each half-started container was removed")
	assert.Nil(t, holder.container, "and none is kept for Main or the chaos helpers")
	assert.Nil(t, holder.terminate)
	assert.True(t, fake.skipped || fake.failed)
}
