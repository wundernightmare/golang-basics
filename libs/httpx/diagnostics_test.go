package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memSink is a DumpSink that remembers what it was given, or refuses.
type memSink struct {
	mu      sync.Mutex
	objects map[string][]byte
	err     error
}

func (s *memSink) Put(_ context.Context, key string, body []byte, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}

	if s.objects == nil {
		s.objects = map[string][]byte{}
	}

	s.objects[key] = body

	return nil
}

func (s *memSink) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.err = err
}

func (s *memSink) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.objects))
	for key := range s.objects {
		out = append(out, key)
	}

	return out
}

// find returns the one object whose key ends in suffix.
func (s *memSink) find(t *testing.T, suffix string) (string, []byte) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, body := range s.objects {
		if strings.HasSuffix(key, suffix) {
			return key, body
		}
	}

	t.Fatalf("no object ending in %q", suffix)

	return "", nil
}

// container is a cgroup v2 filesystem with the given limit and usage.
func container(limit, usage, inactiveFile string) fstest.MapFS {
	return fstest.MapFS{
		"sys/fs/cgroup/cgroup.controllers": file("memory\n"),
		"proc/self/cgroup":                 file("0::/\n"),
		"sys/fs/cgroup/memory.max":         file(limit + "\n"),
		"sys/fs/cgroup/memory.current":     file(usage + "\n"),
		"sys/fs/cgroup/memory.stat":        file("anon 1\ninactive_file " + inactiveFile + "\nactive_file 2\n"),
	}
}

func testDiagnostics(t *testing.T, dir string, sink DumpSink) *diagnostics {
	t.Helper()

	cfg := Config{Service: "svc", Addr: "127.0.0.1:0", DiagDumpDir: dir}
	require.NoError(t, cfg.Validate())

	diag := newDiagnostics(cfg, BuildInfo{Service: "svc", Version: "v1", Revision: "abc"}, slog.New(slog.DiscardHandler))
	diag.instance = "pod-1"
	diag.started = time.Now()

	if sink != nil {
		diag.sink = sink
	}

	return diag
}

func TestCgroupMemoryUsage(t *testing.T) {
	t.Parallel()

	usage, limit, ok := cgroupMemoryUsage(container("1000", "900", "300"))
	require.True(t, ok)
	assert.Equal(t, int64(1000), limit)
	assert.Equal(t, int64(600), usage, "inactive file cache is not pressure")

	_, _, ok = cgroupMemoryUsage(container("max", "900", "0"))
	assert.False(t, ok, "no limit, nothing to measure against")

	usage, limit, ok = cgroupMemoryUsage(fstest.MapFS{
		"sys/fs/cgroup/memory/memory.limit_in_bytes": file("2000\n"),
		"sys/fs/cgroup/memory/memory.usage_in_bytes": file("1500\n"),
		"sys/fs/cgroup/memory/memory.stat":           file("inactive_file 1\ntotal_inactive_file 500\n"),
	})
	require.True(t, ok, "cgroup v1")
	assert.Equal(t, int64(2000), limit)
	assert.Equal(t, int64(1000), usage)
}

// Above the threshold the watcher ships a report and two profiles, once per
// cooldown; below it, nothing.
func TestDiagnostics_MemoryPressureShipsProfiles(t *testing.T) {
	t.Parallel()

	sink := &memSink{}
	diag := testDiagnostics(t, "", sink)
	now := time.Date(2026, 11, 11, 4, 38, 6, 123e6, time.UTC)
	diag.now = func() time.Time { return now }

	diag.root = container("1000", "850", "0")
	diag.checkMemory(t.Context())
	require.Empty(t, sink.keys(), "85% is below the 90% threshold")

	diag.root = container("1000", "990", "50") // working set 940
	diag.checkMemory(t.Context())
	require.Len(t, sink.keys(), 3)

	const prefix = "diagnostics/svc/dt=2026-11-11/pod-1/20261111T043806.123Z-"

	key, heap := sink.find(t, "-heap.pb.gz")
	assert.Equal(t, prefix+"heap.pb.gz", key)
	require.Greater(t, len(heap), 2)
	assert.Equal(t, []byte{0x1f, 0x8b}, heap[:2], "a gzipped pprof profile")

	_, goroutines := sink.find(t, "-goroutine.pb.gz")
	assert.Equal(t, []byte{0x1f, 0x8b}, goroutines[:2])

	_, raw := sink.find(t, "-memory.json")

	var report memoryReport
	require.NoError(t, json.Unmarshal(raw, &report))
	assert.Equal(t, int64(1000), report.ContainerLimitBytes)
	assert.Equal(t, int64(940), report.ContainerUsageBytes)
	assert.Equal(t, "pod-1", report.Instance)
	assert.Equal(t, "v1", report.Version)
	assert.Positive(t, report.GoHeapAllocBytes)
	assert.Positive(t, report.Goroutines)

	assert.InDelta(t, 3, testutil.ToFloat64(diag.dumps.WithLabelValues(dumpKindMemory, dumpUploaded)), 0)

	now = now.Add(diag.cfg.DiagMemoryCooldown - time.Second)
	diag.checkMemory(t.Context())
	require.Len(t, sink.keys(), 3, "still inside the cooldown")

	now = now.Add(2 * time.Second)

	diag.checkMemory(t.Context())
	require.Len(t, sink.keys(), 6, "the cooldown passed and the pod is still full")
}

func TestDiagnostics_MemoryThresholdOff(t *testing.T) {
	t.Parallel()

	sink := &memSink{}
	diag := testDiagnostics(t, "", sink)
	diag.cfg.DiagMemoryThreshold = -1
	diag.root = container("1000", "999", "0")

	diag.checkMemory(t.Context())
	assert.Empty(t, sink.keys())
}

// A sink that is down costs nothing: the artifacts wait in the dump
// directory and leave it when the sink is back.
func TestDiagnostics_SinkDownSpoolsThenShips(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sink := &memSink{}
	sink.fail(errors.New("store is down"))

	diag := testDiagnostics(t, dir, sink)
	diag.root = container("1000", "950", "0")

	diag.checkMemory(t.Context())
	require.Empty(t, sink.keys())

	spooled, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, spooled, 3)
	assert.InDelta(t, 3, testutil.ToFloat64(diag.dumps.WithLabelValues(dumpKindMemory, dumpSpooled)), 0)

	diag.flushSpool(t.Context())

	left, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, left, 3, "a file leaves the directory only once it is uploaded")

	sink.fail(nil)
	diag.flushSpool(t.Context())

	left, err = os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, left)
	require.Len(t, sink.keys(), 3)

	for _, entry := range spooled {
		key, _ := sink.find(t, entry.Name())
		assert.True(t, strings.HasPrefix(key, "diagnostics/svc/dt="), key)
	}
}

func TestDiagnostics_NoSinkNoDirectoryCountsTheLoss(t *testing.T) {
	t.Parallel()

	diag := testDiagnostics(t, "", nil)
	require.False(t, diag.enabled())

	diag.deliver(t.Context(), artifact{kind: dumpKindMemory, name: "x", at: time.Now()})
	assert.InDelta(t, 1, testutil.ToFloat64(diag.dumps.WithLabelValues(dumpKindMemory, dumpFailed)), 0)
}

// Without a sink the directory is the destination; it holds the newest
// DIAG_SPOOL_MAX_FILES of this package's files and nobody else's are touched.
func TestDiagnostics_SpoolIsBounded(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	diag := testDiagnostics(t, dir, nil)
	diag.cfg.DiagSpoolMaxFiles = 2

	require.NoError(t, os.WriteFile(filepath.Join(dir, "README"), []byte("not ours"), 0o600))

	base := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		at := base.Add(time.Duration(i) * time.Minute)
		name := at.Format(dumpStamp) + "-memory.json"
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("{}"), 0o600))
		require.NoError(t, os.Chtimes(path, at, at))
	}

	diag.flushSpool(t.Context())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	assert.ElementsMatch(t, []string{
		"README",
		base.Add(3*time.Minute).Format(dumpStamp) + "-memory.json",
		base.Add(4*time.Minute).Format(dumpStamp) + "-memory.json",
	}, names)
}

// A pod that was killed leaves an empty crash file: no crash, nothing to ship.
func TestDiagnostics_EmptyCrashFileIsNotACrash(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sink := &memSink{}
	diag := testDiagnostics(t, dir, sink)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "crash-20261111T000000.000Z-7.pending"), nil, 0o600))
	diag.flushSpool(t.Context())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.Empty(t, sink.keys())
}

const crashHelperEnv = "HTTPX_TEST_CRASH_DIR"

// TestCrashHelper is not a test: it is the process TestDiagnostics_CrashIsShippedByTheNextStart
// crashes. It runs a server with a dump directory and panics on a goroutine
// nothing recovers.
func TestCrashHelper(t *testing.T) { //nolint:paralleltest // dies by design
	dir := os.Getenv(crashHelperEnv)
	if dir == "" {
		t.Skip("helper process only")
	}

	srv, err := NewServer(Config{Service: "svc", Addr: "127.0.0.1:0", DiagDumpDir: dir}, nil)
	require.NoError(t, err)

	go func() { _ = srv.Run(context.Background()) }()

	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(dir)
		return len(entries) == 1 && srv.ListenAddr() != ""
	}, 10*time.Second, 5*time.Millisecond)

	go panic("boom: the helper crashes on purpose")

	select {}
}

// The whole path with a real crash: a process with a dump directory dies of
// an unrecovered panic; the next start finds the runtime's report and ships
// it as JSON.
func TestDiagnostics_CrashIsShippedByTheNextStart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCrashHelper$", "-test.v")

	cmd.Env = append(os.Environ(), crashHelperEnv+"="+dir)

	out, err := cmd.CombinedOutput()

	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "the helper must die: %s", out)
	require.Equal(t, 2, exit.ExitCode(), "a Go panic exits 2: %s", out)

	sink := &memSink{}
	diag := testDiagnostics(t, dir, sink)
	diag.flushSpool(t.Context())

	key, raw := sink.find(t, "-crash.json")
	assert.True(t, strings.HasPrefix(key, "diagnostics/svc/dt="), key)
	assert.Contains(t, key, "/pod-1/")

	var report crashReport
	require.NoError(t, json.Unmarshal(raw, &report))
	assert.Equal(t, "panic: boom: the helper crashes on purpose", report.Reason)
	assert.Contains(t, report.Output, "goroutine ")
	assert.Contains(t, report.Output, "TestCrashHelper", "RUNTIME_TRACEBACK=all: the other goroutines are in the report")
	assert.False(t, report.Truncated)
	assert.Equal(t, "svc", report.Service)
	assert.WithinDuration(t, time.Now(), report.CrashedAt, time.Minute)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "shipped, so gone from the directory")
	assert.InDelta(t, 1, testutil.ToFloat64(diag.dumps.WithLabelValues(dumpKindCrash, dumpUploaded)), 0)
}

// A long report is cut at DIAG_CRASH_MAX_BYTES and says so.
func TestDiagnostics_CrashReportIsCapped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sink := &memSink{}
	diag := testDiagnostics(t, dir, sink)
	diag.cfg.DiagCrashMaxBytes = 16

	require.NoError(t, os.WriteFile(filepath.Join(dir, "crash-20261111T000000.000Z-7.pending"),
		[]byte("panic: x\n"+strings.Repeat("goroutine\n", 100)), 0o600))
	diag.flushSpool(t.Context())

	_, raw := sink.find(t, "-crash.json")

	var report crashReport
	require.NoError(t, json.Unmarshal(raw, &report))
	assert.True(t, report.Truncated)
	assert.Len(t, report.Output, 16)
	assert.Equal(t, "panic: x", report.Reason)
}

// A server that stops cleanly leaves nothing behind for the next start.
func TestServer_CleanStopLeavesNoCrashFile(t *testing.T) { //nolint:paralleltest // SetCrashOutput is process-wide
	dir := t.TempDir()

	srv, err := NewServer(Config{Service: "svc", Addr: "127.0.0.1:0", DiagDumpDir: dir}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(dir)
		return len(entries) == 1
	}, 10*time.Second, 5*time.Millisecond, "the crash file is armed while the server runs")

	cancel()
	require.NoError(t, <-done)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}
