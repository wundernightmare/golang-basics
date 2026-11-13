package integration_test

// SIGKILL: the one failure no code in the process gets to handle — an OOM
// kill, a node that disappears, `kubectl delete --force`. The claim under
// test is the service's whole reason to hold no state: whatever it answered
// 200 for is already in a sink, so killing it loses nothing it promised.
//
// This needs a real process (a goroutine cannot be killed), so the test
// builds the service's binary and runs it as a child.

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

var (
	buildOnce sync.Once
	builtBin  string
	errBuild  error
)

// binary builds services/ingest once per test run.
func binary(t *testing.T) string {
	t.Helper()

	buildOnce.Do(func() {
		// Shared by every test of the binary (t.TempDir would remove it with
		// the first one); TestMain removes it.
		dir, err := os.MkdirTemp("", "ingest-kill-test") //nolint:usetesting // outlives a single test, see above
		if err != nil {
			errBuild = err
			return
		}

		builtBin = filepath.Join(dir, "ingest")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// The package directory is internal/integration: the module root is
		// two levels up.
		cmd := exec.CommandContext(ctx, "go", "build", "-tags", buildTags, "-o", builtBin, ".")
		cmd.Dir = filepath.Join("..", "..")

		if out, err := cmd.CombinedOutput(); err != nil {
			errBuild = &buildError{err: err, output: string(out)}
		}
	})

	require.NoError(t, errBuild)

	return builtBin
}

type buildError struct {
	err    error
	output string
}

func (e *buildError) Error() string { return "go build: " + e.err.Error() + "\n" + e.output }

func freeAddr(t *testing.T) string {
	t.Helper()

	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	return addr
}

// child is the service as a separate process.
type child struct {
	*service // the request and read-back helpers; no in-process server

	cmd    *exec.Cmd
	output *strings.Builder
	exited chan struct{}
}

// spawn starts the binary against back with the suite's fast configuration
// and waits until it is ready. prefix and topic are the test's: a restarted
// child continues in the same archive.
func spawn(t *testing.T, back backends, topic, prefix string) *child {
	t.Helper()

	api, admin := freeAddr(t), freeAddr(t)
	env := environment(back, topic, prefix, map[string]string{
		"INGEST_HTTP_ADDR":  api,
		"INGEST_ADMIN_ADDR": admin,
	})

	proc := &child{
		service: &service{t: t, back: back, topic: topic, prefix: prefix, base: "http://" + api, admin: "http://" + admin},
		output:  &strings.Builder{},
		exited:  make(chan struct{}),
	}

	proc.cmd = exec.CommandContext(context.Background(), binary(t)) // the test's own build output
	proc.cmd.Env = os.Environ()

	for key, value := range env {
		proc.cmd.Env = append(proc.cmd.Env, key+"="+value)
	}

	var mu sync.Mutex

	proc.cmd.Stdout = lockedWriter{mu: &mu, dst: proc.output}
	proc.cmd.Stderr = proc.cmd.Stdout

	require.NoError(t, proc.cmd.Start())

	go func() {
		_ = proc.cmd.Wait()

		close(proc.exited)
	}()

	t.Cleanup(func() {
		_ = proc.cmd.Process.Kill()

		<-proc.exited

		if t.Failed() {
			mu.Lock()
			t.Logf("── child output ──\n%s", proc.output.String())
			mu.Unlock()
		}
	})

	require.Eventually(t, func() bool {
		res, err := http.Get(proc.admin + "/readyz")
		if err != nil {
			return false
		}

		defer func() { _ = res.Body.Close() }()

		_, _ = io.Copy(io.Discard, res.Body)

		return res.StatusCode == http.StatusOK
	}, 60*time.Second, 50*time.Millisecond, "the child becomes ready")

	return proc
}

type lockedWriter struct {
	mu  *sync.Mutex
	dst *strings.Builder
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.dst.Write(p)
}

// hammer posts batches from several clients until the listener goes away,
// and returns the ids of every event that was answered 200.
func (c *child) hammer(clients int, kill func()) []string {
	var (
		mu    sync.Mutex
		acked []string
		wg    sync.WaitGroup
	)

	for range clients {
		wg.Go(func() {
			for {
				batch := ids(5)

				got := c.post(batch...)
				if got.err != nil {
					return // connection refused or reset: the process is gone
				}

				if got.status == http.StatusOK {
					mu.Lock()

					acked = append(acked, batch...)
					mu.Unlock()
				}
			}
		})
	}

	kill()
	wg.Wait()

	return acked
}

func killAfter(c *child, d time.Duration) func() {
	return func() {
		time.Sleep(d)

		_ = c.cmd.Process.Kill() // SIGKILL: no handler, no drain, no flush

		<-c.exited
	}
}

// Archive only — the footprint the service is brought back on before Kafka.
// Every answer waited for a put, so every acknowledged event must be in the
// archive the instant the process dies.
func TestKill9_ArchiveOnly_EveryAcknowledgedEventIsStored(t *testing.T) {
	t.Parallel()

	back := direct(t)
	back.brokers = nil

	topic, prefix := "", testx.Unique("kill-archive")+"/"
	proc := spawn(t, back, topic, prefix)

	acked := proc.hammer(8, killAfter(proc, 1500*time.Millisecond))
	require.NotEmpty(t, acked)
	t.Logf("%d events acknowledged before SIGKILL", len(acked))

	archived := proc.archived()

	var lost []string

	for _, id := range acked {
		if archived[id] == 0 {
			lost = append(lost, id)
		}
	}

	assert.Empty(t, lost, "%d of %d acknowledged events are not in the archive", len(lost), len(acked))

	// A new process continues in the same archive under its own writer id:
	// nothing the dead one wrote is overwritten.
	before := len(archived)
	restarted := spawn(t, back, topic, prefix)
	more := ids(3)
	require.Equal(t, http.StatusOK, restarted.post(more...).status)

	after := restarted.archived()
	assert.Len(t, after, before+len(more), "the restart added its events and replaced none")

	for _, id := range acked {
		assert.Positive(t, after[id])
	}
}

// Both sinks: an answer came from whichever confirmed first, so each
// acknowledged event is in Kafka or in the archive — the second copy is what
// a kill may take, never the first.
func TestKill9_BothSinks_EveryAcknowledgedEventIsInOne(t *testing.T) {
	t.Parallel()

	back := direct(t)
	topic, prefix := testx.Unique("ingest.kill"), testx.Unique("kill-both")+"/"
	proc := spawn(t, back, topic, prefix)

	acked := proc.hammer(8, killAfter(proc, 1500*time.Millisecond))
	require.NotEmpty(t, acked)
	t.Logf("%d events acknowledged before SIGKILL", len(acked))

	archived := proc.archived()
	published := proc.published(acked, 60*time.Second)

	var lost, kafkaOnly, archiveOnly int

	for _, id := range acked {
		_, inKafka := published[id]
		inArchive := archived[id] > 0

		switch {
		case !inKafka && !inArchive:
			lost++
		case inKafka && !inArchive:
			kafkaOnly++
		case !inKafka:
			archiveOnly++
		}
	}

	t.Logf("second copies the kill took: %d events in Kafka only, %d in the archive only", kafkaOnly, archiveOnly)
	assert.Zero(t, lost, "acknowledged events in neither sink")
}
