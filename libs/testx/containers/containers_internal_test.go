package containers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
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

// stopsHere is a testing.TB whose Skipf, Fatalf and FailNow end the call, as
// the real ones do, without ending the test that looks at the outcome.
type stopsHere struct {
	testing.TB

	skipped, failed bool
	logged          []string
}

type stopped struct{}

func (s *stopsHere) Skipf(string, ...any) { s.skipped = true; panic(stopped{}) }

func (s *stopsHere) Fatalf(string, ...any) { s.failed = true; panic(stopped{}) }

func (s *stopsHere) Errorf(string, ...any) { s.failed = true }

func (s *stopsHere) Logf(format string, args ...any) {
	s.logged = append(s.logged, fmt.Sprintf(format, args...))
}

func (s *stopsHere) FailNow() { s.failed = true; panic(stopped{}) }

// run calls f and reports whether it ended through the fake's Skipf, Fatalf
// or FailNow.
func (s *stopsHere) run(f func()) (ended bool) {
	defer func() {
		if r := recover(); r != nil {
			_, ended = r.(stopped)
			require.True(s.TB, ended, "unexpected panic: %v", r)
		}
	}()

	f()

	return false
}

func healthy(context.Context) error { return nil }

func noRuntime(context.Context) error { return errors.New("cannot connect to the daemon") }

func neverStarts(context.Context) (string, testcontainers.Container, error) {
	return "", nil, errors.New("did not come up")
}

// A starter that fails hands the container back with the error, and use
// removes it after every attempt: without Ryuk nothing else would.
func TestUseRemovesAHalfStartedContainer(t *testing.T) {
	t.Parallel()

	var (
		holder shared[string]
		ctr    halfStarted
	)

	fake := &stopsHere{TB: t}
	attempts := 0

	require.True(t, fake.run(func() {
		useIn(fake, &holder, "fake", func(context.Context) (string, testcontainers.Container, error) {
			attempts++

			return "", &ctr, errors.New("did not come up")
		}, false, healthy)
	}))

	assert.Equal(t, 3, attempts)
	assert.Equal(t, attempts, ctr.terminated, "each half-started container was removed")
	assert.Nil(t, holder.container, "and none is kept for Main or the chaos helpers")
	assert.Nil(t, holder.terminate)
}

// A fixture that did not start is skipped in one case only: outside CI, on a
// machine whose container runtime does not answer.
func TestUseSkipsOnlyWithoutAContainerRuntime(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		inCI    bool
		runtime func(context.Context) error
		skipped bool
	}{
		"locally, no runtime":      {runtime: noRuntime, skipped: true},
		"locally, runtime healthy": {runtime: healthy},
		"in CI, no runtime":        {inCI: true, runtime: noRuntime},
		"in CI, runtime healthy":   {inCI: true, runtime: healthy},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var holder shared[string]

			fake := &stopsHere{TB: t}

			require.True(t, fake.run(func() { useIn(fake, &holder, "fake", neverStarts, tc.inCI, tc.runtime) }))
			assert.Equal(t, tc.skipped, fake.skipped)
			assert.Equal(t, !tc.skipped, fake.failed)
		})
	}
}

// A starter that fails the test instead of returning its error leaves through
// runtime.Goexit; the fixture's other callers must not take that for a start.
func TestUseFailsEveryCallerAfterAStarterThatDidNotReturn(t *testing.T) {
	t.Parallel()

	var holder shared[string]

	first := &stopsHere{TB: t}

	require.True(t, first.run(func() {
		useIn(first, &holder, "fake", func(context.Context) (string, testcontainers.Container, error) {
			first.FailNow()

			return "localhost:1", nil, nil
		}, false, healthy)
	}))

	second := &stopsHere{TB: t}

	require.True(t, second.run(func() { useIn(second, &holder, "fake", neverStarts, false, healthy) }),
		"the second caller got a value from a fixture that never started")
	assert.True(t, second.failed)
}

// Running out of spare ports fails the test that asked; the next ProxyTo in
// the binary must still get to the lock.
func TestSparePortsStayUsableAfterRunningOut(t *testing.T) {
	t.Parallel()

	var ports sparePorts

	for i := range sparePortCount {
		assert.Equal(t, sparePortFirst+i, ports.port(t, string(rune('a'+i)), "32768"))
	}

	assert.Equal(t, sparePortFirst, ports.port(t, "a", "32768"), "a name keeps its port")

	fake := &stopsHere{TB: t}

	require.True(t, fake.run(func() { ports.port(fake, "one too many", "32768") }))
	assert.True(t, fake.failed)
	require.True(t, ports.mu.TryLock(), "the lock was left held")
	ports.mu.Unlock()
}

func TestAProxyNameCannotMoveToAnotherUpstream(t *testing.T) {
	t.Parallel()

	var ports sparePorts

	ports.port(t, "edge", "32768")

	fake := &stopsHere{TB: t}

	require.True(t, fake.run(func() { ports.port(fake, "edge", "32769") }))
	assert.True(t, fake.failed)
}

// Reset is every proxy's cleanup: what it could not undo must reach the log
// of the test that left the dependency broken.
func TestProxyResetLogsWhatItCouldNotUndo(t *testing.T) {
	t.Parallel()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/proxies/edge" {
			_, _ = w.Write([]byte(`{"name":"edge","enabled":false,"toxics":[]}`))
			return
		}

		http.Error(w, `{"error":"toxiproxy is gone","status":500}`, http.StatusInternalServerError)
	}))
	t.Cleanup(api.Close)

	proxy, err := toxiclient.NewClient(api.URL).Proxy("edge")
	require.NoError(t, err)

	fake := &stopsHere{TB: t}
	(&Proxy{tb: fake, proxy: proxy}).Reset()

	assert.False(t, fake.failed, "a cleanup that cannot reset does not change the test's verdict")
	require.Len(t, fake.logged, 2, "listing the toxics and enabling the proxy both failed")
	assert.Contains(t, fake.logged[1], "reset proxy edge: enable")
}

func TestAnEmptyCIVariableIsNotCI(t *testing.T) {
	t.Setenv("CI", "")
	assert.False(t, inCI())

	t.Setenv("CI", "true")
	assert.True(t, inCI())
}
