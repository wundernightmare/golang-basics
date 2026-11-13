package httpx

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runBriefly runs a server until it listens, then stops it cleanly.
func runBriefly(t *testing.T, cfg Config) {
	t.Helper()

	srv, err := NewServer(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return srv.ListenAddr() != "" }, 10*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestConfig_RuntimeAndDiagnosticsValidation(t *testing.T) {
	t.Parallel()

	for key, value := range map[string]string{
		"DIAG_MEMORY_THRESHOLD": "1.5",
		"DIAG_CHECK_INTERVAL":   "-1s",
		"DIAG_SPOOL_MAX_FILES":  "-1",
		"DIAG_KEY_PREFIX":       "/",
	} {
		_, err := LoadConfigFrom("", map[string]string{"ADMIN_INSECURE": "true", key: value})
		require.ErrorContains(t, err, key)
	}
}

// An explicit GOMEMLIMIT, or a negative ratio, leaves the runtime alone.
func TestServer_MemoryLimitOptOuts(t *testing.T) {
	runBriefly(t, Config{Service: "svc", Addr: "127.0.0.1:0", RuntimeMemoryLimitRatio: -1})

	t.Setenv("GOMEMLIMIT", "1GiB")
	runBriefly(t, Config{Service: "svc", Addr: "127.0.0.1:0"})
}

func TestMemoryLimiter_RunFollowsUntilCancelled(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	limiter := &memoryLimiter{
		root: container("1000", "1", "0"), ratio: 0.5, log: slog.New(slog.DiscardHandler),
		set: func(int64) int64 { calls.Add(1); return 0 },
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() { defer close(done); limiter.run(ctx, time.Millisecond) }()

	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestCgroup_OddFilesystems(t *testing.T) {
	t.Parallel()

	const (
		controllers = "sys/fs/cgroup/cgroup.controllers"
		limit       = "sys/fs/cgroup/memory.max"
		current     = "sys/fs/cgroup/memory.current"
		stat        = "sys/fs/cgroup/memory.stat"
	)

	_, _, ok := cgroupMemoryUsage(fstest.MapFS{controllers: file(""), limit: file("1000\n")})
	assert.False(t, ok, "a limit without a usage figure measures nothing")

	usage, _, ok := cgroupMemoryUsage(fstest.MapFS{controllers: file(""), limit: file("1000\n"), current: file("700\n")})
	require.True(t, ok, "no memory.stat: usage as it is")
	assert.Equal(t, int64(700), usage)

	usage, _, _ = cgroupMemoryUsage(fstest.MapFS{
		controllers: file(""), limit: file("1000\n"), current: file("700\n"), stat: file("inactive_file many\n"),
	})
	assert.Equal(t, int64(700), usage, "a garbage stat value subtracts nothing")

	usage, _, _ = cgroupMemoryUsage(fstest.MapFS{
		controllers: file(""), limit: file("1000\n"), current: file("700\n"), stat: file("anon 5\n"),
	})
	assert.Equal(t, int64(700), usage, "no inactive_file line")

	got, limited := cgroupMemoryLimit(fstest.MapFS{
		controllers: file(""), "proc/self/cgroup": file("12:memory:/legacy\n"), limit: file("4096\n"),
	})
	require.True(t, limited, "no unified-hierarchy line: the root cgroup")
	assert.Equal(t, int64(4096), got)
}

// A dump directory that cannot be used turns crash capture off and nothing
// else: the server runs.
func TestDiagnostics_UnusableDumpDirectory(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}

	blocker := filepath.Join(t.TempDir(), "a-file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))

	underFile := testDiagnostics(t, filepath.Join(blocker, "dumps"), nil)
	underFile.armCrashCapture(t.Context())
	assert.Empty(t, underFile.ownPending)

	readOnly := t.TempDir()
	require.NoError(t, os.Chmod(readOnly, 0o500))

	noWrite := testDiagnostics(t, readOnly, nil)
	noWrite.armCrashCapture(t.Context())
	assert.Empty(t, noWrite.ownPending)

	// Neither uploaded nor written: counted as lost.
	sink := &memSink{}
	sink.fail(errors.New("store is down"))

	lost := testDiagnostics(t, readOnly, sink)
	lost.deliver(t.Context(), artifact{kind: dumpKindMemory, name: "20261111T000000.000Z-memory.json", at: time.Now()})
	assert.InDelta(t, 1, testutil.ToFloat64(lost.dumps.WithLabelValues(dumpKindMemory, dumpFailed)), 0)

	gone := testDiagnostics(t, filepath.Join(t.TempDir(), "never-created"), sink)
	gone.flushSpool(t.Context()) // nothing to read, nothing to do
	gone.trimSpool(t.Context())
}

// The dump directory may hold anything; only this package's files are read,
// shipped and trimmed.
func TestDiagnostics_SpoolTakesOnlyItsOwnFiles(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("root reads unreadable files")
	}

	dir := t.TempDir()
	sink := &memSink{}
	diag := testDiagnostics(t, dir, sink)

	write := func(name string, mode os.FileMode) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("{}"), mode))
	}

	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0o700))
	write("notes", 0o600)                                // no stamp at all
	write("draft-memory.json", 0o600)                    // not a stamp
	write("20261111T000000.000Z-core.bin", 0o600)        // a stamp, not our artifact
	write("20261111T000001.000Z-memory.json", 0o000)     // ours, unreadable
	write("20261111T000002.000Z-crash.json", 0o600)      // a crash report whose upload failed earlier
	write("20261111T000003.000Z-goroutine.pb.gz", 0o600) // a profile whose upload failed earlier

	diag.flushSpool(t.Context())

	_, _ = sink.find(t, "20261111T000002.000Z-crash.json")
	_, _ = sink.find(t, "20261111T000003.000Z-goroutine.pb.gz")
	assert.Len(t, sink.keys(), 2)
	assert.InDelta(t, 1, testutil.ToFloat64(diag.dumps.WithLabelValues(dumpKindCrash, dumpUploaded)), 0)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 5, "everything that was not shipped is still there")
}

// Crash files of earlier runs count toward the bound when there is no sink.
func TestDiagnostics_TrimCountsCrashFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	diag := testDiagnostics(t, dir, nil)
	diag.cfg.DiagSpoolMaxFiles = 1

	for i, name := range []string{"crash-20261111T000000.000Z-1.pending", "crash-20261111T000100.000Z-2.pending"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("panic: x\n"), 0o600))

		at := time.Date(2026, 11, 11, 0, i, 0, 0, time.UTC)
		require.NoError(t, os.Chtimes(path, at, at))
	}

	diag.flushSpool(t.Context())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "crash-20261111T000100.000Z-2.pending", entries[0].Name())
}

// The watcher as the server runs it: it ships what waits in the directory,
// notices memory pressure on a tick and retries the spool.
func TestDiagnostics_WatcherLoop(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sink := &memSink{}

	diag := testDiagnostics(t, "", sink)
	diag.cfg.DiagDumpDir = dir // a spool, without arming the process-wide crash output
	diag.cfg.DiagCheckInterval = time.Millisecond
	diag.cfg.DiagSpoolRetry = time.Millisecond
	diag.root = container("1000", "990", "0")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "20261111T000002.000Z-crash.json"), []byte("{}"), 0o600))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() { defer close(done); diag.run(ctx) }()

	require.Eventually(t, func() bool { return len(sink.keys()) == 4 }, 10*time.Second, 2*time.Millisecond,
		"the spooled crash report and the three memory artifacts")
	cancel()
	<-done
}
