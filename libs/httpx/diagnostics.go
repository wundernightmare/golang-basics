package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Post-mortem diagnostics: what is left of a process after it is gone.
//
// On-demand profiles (/debug/pprof) need a live process and somebody
// watching. The two ways a pod usually dies give neither:
//
//   - a crash — an unrecovered panic or a runtime fatal error. The runtime's
//     report (every goroutine, see RUNTIME_TRACEBACK) is duplicated into a
//     file in DIAG_DUMP_DIR as it is written; the next start of the container
//     finds the file and ships it. An emptyDir outlives a container restart,
//     so no sidecar is involved.
//   - an OOM kill — SIGKILL from the kernel, no hook possible. So the evidence
//     is taken before it: when the container's memory use crosses
//     DIAG_MEMORY_THRESHOLD of its limit, a heap profile, a goroutine profile
//     and a summary of where the memory is are captured and shipped at once.
//
// "Shipped" means handed to the [DumpSink] the service installed with
// [Server.SetDumpSink] — libs/s3's client is one. Without a sink, or when the
// sink fails, artifacts stay in DIAG_DUMP_DIR and are retried. With neither a
// sink nor a directory nothing here runs.
//
// Profiles are statistics about allocation sites and stacks: no payloads. A
// crash report shows argument words as numbers, not the data they point to.
// A full heap dump (debug.WriteHeapDump) is deliberately not taken: it is the
// size of the heap, stops the process while it is written, and contains every
// secret and every customer event in memory.

// DumpSink is where diagnostics go. *s3.Client (libs/s3) satisfies it.
type DumpSink interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
}

// SetDumpSink installs the destination of crash reports and memory-pressure
// profiles. Call it before [Server.Run].
func (s *Server) SetDumpSink(sink DumpSink) { s.diag.sink = sink }

const (
	defaultDiagKeyPrefix       = "diagnostics"
	defaultDiagMemoryThreshold = 0.9
	defaultDiagMemoryCooldown  = 10 * time.Minute
	defaultDiagCheckInterval   = 5 * time.Second
	defaultDiagUploadTimeout   = 15 * time.Second
	defaultDiagSpoolRetry      = time.Minute
	defaultDiagCrashMaxBytes   = 8 << 20
	defaultDiagSpoolMaxFiles   = 20

	dumpKindCrash  = "crash"
	dumpKindMemory = "memory"

	dumpUploaded = "uploaded"
	dumpSpooled  = "spooled"
	dumpFailed   = "failed"

	// The current run's crash output; renamed by nothing, read by the next run.
	crashPendingPrefix = "crash-"
	crashPendingSuffix = ".pending"

	// Timestamps in names sort lexically and contain nothing a filesystem or
	// an object key dislikes.
	dumpStamp = "20060102T150405.000Z"

	contentJSON  = "application/json"
	contentPprof = "application/octet-stream"
)

// artifact is one object to ship: its name is "<stamp>-<what>", unique per
// instance, and both the spool file name and the tail of the object key.
type artifact struct {
	kind        string
	name        string
	at          time.Time
	body        []byte
	contentType string
}

type diagnostics struct {
	cfg      Config
	build    BuildInfo
	log      *slog.Logger
	sink     DumpSink
	instance string
	started  time.Time
	dumps    *prometheus.CounterVec

	// Seams for the tests.
	root fs.FS
	now  func() time.Time

	ownPending string    // base name of this run's crash file, "" when there is none
	crashFile  *os.File  // kept open for the runtime
	lastMemory time.Time // last memory-pressure capture
	nextSpool  time.Time
}

func newDiagnostics(cfg Config, build BuildInfo, log *slog.Logger) *diagnostics {
	instance, err := os.Hostname() // the pod name in Kubernetes
	if err != nil || instance == "" {
		instance = "unknown"
	}

	return &diagnostics{
		cfg: cfg, build: build, log: log, instance: instance,
		root: os.DirFS("/"), now: time.Now,
		dumps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "diag_dumps_total",
			Help: "Diagnostic artifacts produced, by kind (crash, memory) and result (uploaded, spooled, failed).",
		}, []string{"kind", "result"}),
	}
}

func (d *diagnostics) enabled() bool { return d.sink != nil || d.cfg.DiagDumpDir != "" }

// start arms crash capture and ships what earlier runs left behind, then
// watches memory until ctx is done. It returns at once; stop undoes the
// crash capture on a clean exit.
func (d *diagnostics) start(ctx context.Context) (stop func()) {
	if !d.enabled() {
		return func() {}
	}

	d.started = d.now()

	if d.cfg.DiagDumpDir != "" {
		d.armCrashCapture(ctx)
	}

	// The watcher owns every field from here on; stop waits for it before
	// touching the crash file.
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		d.run(ctx)
	}()

	return func() {
		cancel()
		<-done
		d.disarmCrashCapture()
	}
}

func (d *diagnostics) run(ctx context.Context) {
	d.flushSpool(ctx) // off the start-up path: a store that hangs must not delay the listeners

	ticker := time.NewTicker(d.cfg.DiagCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.checkMemory(ctx)

			if now := d.now(); !now.Before(d.nextSpool) {
				d.flushSpool(ctx)
			}
		}
	}
}

// --- crash capture -------------------------------------------------------------

// armCrashCapture points the runtime's crash output at a fresh file in the
// dump directory. A directory that cannot be written disables the capture
// and says so: the report still goes to stderr.
func (d *diagnostics) armCrashCapture(ctx context.Context) {
	dir := d.cfg.DiagDumpDir
	if err := os.MkdirAll(dir, 0o750); err != nil {
		d.log.WarnContext(ctx, "diagnostics: dump directory is not usable, crash capture is off", "dir", dir, "error", err.Error())
		return
	}

	name := fmt.Sprintf("%s%s-%d%s", crashPendingPrefix, d.started.UTC().Format(dumpStamp), os.Getpid(), crashPendingSuffix)

	file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600) //nolint:gosec // the directory is the operator's configuration
	if err != nil {
		d.log.WarnContext(ctx, "diagnostics: cannot create the crash file, crash capture is off", "dir", dir, "error", err.Error())
		return
	}

	if err := debug.SetCrashOutput(file, debug.CrashOptions{}); err != nil {
		d.log.WarnContext(ctx, "diagnostics: the runtime refused the crash file, crash capture is off", "error", err.Error())

		_ = file.Close()
		_ = os.Remove(file.Name())

		return
	}

	d.crashFile, d.ownPending = file, name
}

// disarmCrashCapture runs on a clean exit: nothing crashed, the file is empty
// and would only be noise for the next start.
func (d *diagnostics) disarmCrashCapture() {
	if d.crashFile == nil {
		return
	}

	_ = debug.SetCrashOutput(nil, debug.CrashOptions{})
	_ = d.crashFile.Close()
	_ = os.Remove(d.crashFile.Name())
	d.crashFile, d.ownPending = nil, ""
}

// crashReport is the JSON a crash is shipped as.
type crashReport struct {
	Service   string    `json:"service"`
	Version   string    `json:"version"`
	Revision  string    `json:"revision"`
	Instance  string    `json:"instance"`
	CrashedAt time.Time `json:"crashed_at"` // the crash file's last write
	FoundAt   time.Time `json:"found_at"`   // the start that picked it up
	Reason    string    `json:"reason"`     // the first line: "panic: …" / "fatal error: …"
	Truncated bool      `json:"truncated"`  // the output was longer than DIAG_CRASH_MAX_BYTES
	Output    string    `json:"output"`     // what the runtime printed
}

// wrapCrash turns a crash file an earlier run left into an artifact. The
// build fields describe the binary that found it; after a rollout that
// crashes, that is normally the one that crashed.
func (d *diagnostics) wrapCrash(path string, info fs.FileInfo) (artifact, error) {
	file, err := os.Open(path) //nolint:gosec // a file this package created in the configured directory
	if err != nil {
		return artifact{}, fmt.Errorf("open: %w", err)
	}

	defer func() { _ = file.Close() }()

	raw, err := io.ReadAll(io.LimitReader(file, d.cfg.DiagCrashMaxBytes))
	if err != nil {
		return artifact{}, fmt.Errorf("read: %w", err)
	}

	reason, _, _ := strings.Cut(strings.TrimSpace(string(raw)), "\n")
	crashedAt := info.ModTime().UTC()

	body, err := json.Marshal(crashReport{
		Service: d.build.Service, Version: d.build.Version, Revision: d.build.Revision,
		Instance: d.instance, CrashedAt: crashedAt, FoundAt: d.now().UTC(),
		Reason: reason, Truncated: info.Size() > int64(len(raw)), Output: strings.ToValidUTF8(string(raw), "�"),
	})
	if err != nil {
		return artifact{}, fmt.Errorf("encode: %w", err)
	}

	return artifact{
		kind: dumpKindCrash, at: crashedAt, body: body, contentType: contentJSON,
		name: crashedAt.Format(dumpStamp) + "-crash.json",
	}, nil
}

// --- memory pressure -----------------------------------------------------------

// memoryReport says where the memory is when the container is nearly full:
// the Go heap (then the heap profile explains it), goroutine stacks (the
// goroutine profile), or outside the runtime altogether — C allocations,
// page cache — when the container's figure is far above the runtime's.
type memoryReport struct {
	Service    string    `json:"service"`
	Version    string    `json:"version"`
	Revision   string    `json:"revision"`
	Instance   string    `json:"instance"`
	CapturedAt time.Time `json:"captured_at"`
	StartedAt  time.Time `json:"started_at"`

	ContainerLimitBytes int64   `json:"container_limit_bytes"`
	ContainerUsageBytes int64   `json:"container_usage_bytes"` // working set: usage minus inactive file cache
	Threshold           float64 `json:"threshold"`

	GoMemoryLimitBytes   int64  `json:"go_memory_limit_bytes"`
	GoSysBytes           uint64 `json:"go_sys_bytes"` // everything the runtime got from the OS
	GoHeapAllocBytes     uint64 `json:"go_heap_alloc_bytes"`
	GoHeapInuseBytes     uint64 `json:"go_heap_inuse_bytes"`
	GoHeapIdleBytes      uint64 `json:"go_heap_idle_bytes"`
	GoHeapReleasedBytes  uint64 `json:"go_heap_released_bytes"`
	GoStackInuseBytes    uint64 `json:"go_stack_inuse_bytes"`
	GoGCCycles           uint32 `json:"go_gc_cycles"`
	GoGCCPUFraction      string `json:"go_gc_cpu_fraction"`
	Goroutines           int    `json:"goroutines"`
	GoMaxProcs           int    `json:"gomaxprocs"`
	OutsideGoRuntimeHint int64  `json:"outside_go_runtime_bytes"` // container usage minus what the runtime holds; negative means none
}

// checkMemory captures once when the working set crosses the threshold, and
// not again for the cooldown: a pod that sits above the line would otherwise
// profile itself every tick.
func (d *diagnostics) checkMemory(ctx context.Context) {
	if d.cfg.DiagMemoryThreshold < 0 {
		return
	}

	usage, limit, ok := cgroupMemoryUsage(d.root)
	if !ok || float64(usage) < float64(limit)*d.cfg.DiagMemoryThreshold {
		return
	}

	now := d.now()
	if !d.lastMemory.IsZero() && now.Sub(d.lastMemory) < d.cfg.DiagMemoryCooldown {
		return
	}

	d.lastMemory = now

	d.log.WarnContext(ctx, "diagnostics: container memory is near its limit, capturing profiles",
		"container_usage_bytes", usage, "container_limit_bytes", limit, "threshold", d.cfg.DiagMemoryThreshold)

	for _, art := range d.memoryArtifacts(now.UTC(), usage, limit) {
		d.deliver(ctx, art)
	}
}

func (d *diagnostics) memoryArtifacts(at time.Time, usage, limit int64) []artifact {
	stamp := at.Format(dumpStamp)

	var stats runtime.MemStats

	runtime.ReadMemStats(&stats)

	held := int64(stats.Sys - stats.HeapReleased) //nolint:gosec // far below MaxInt64

	out := make([]artifact, 0, 3) //nolint:mnd // the report and two profiles

	report, err := json.Marshal(memoryReport{
		Service: d.build.Service, Version: d.build.Version, Revision: d.build.Revision,
		Instance: d.instance, CapturedAt: at, StartedAt: d.started.UTC(),
		ContainerLimitBytes: limit, ContainerUsageBytes: usage, Threshold: d.cfg.DiagMemoryThreshold,
		GoMemoryLimitBytes: debug.SetMemoryLimit(-1),
		GoSysBytes:         stats.Sys, GoHeapAllocBytes: stats.HeapAlloc, GoHeapInuseBytes: stats.HeapInuse,
		GoHeapIdleBytes: stats.HeapIdle, GoHeapReleasedBytes: stats.HeapReleased, GoStackInuseBytes: stats.StackInuse,
		GoGCCycles: stats.NumGC, GoGCCPUFraction: fmt.Sprintf("%.4f", stats.GCCPUFraction),
		Goroutines: runtime.NumGoroutine(), GoMaxProcs: runtime.GOMAXPROCS(0),
		OutsideGoRuntimeHint: usage - held,
	})
	if err == nil {
		out = append(out, artifact{kind: dumpKindMemory, at: at, name: stamp + "-memory.json", body: report, contentType: contentJSON})
	}

	// debug=0: the gzipped protobuf `go tool pprof` reads.
	for _, profile := range []string{"heap", "goroutine"} {
		var buf bytes.Buffer
		if err := pprof.Lookup(profile).WriteTo(&buf, 0); err != nil {
			continue
		}

		out = append(out, artifact{kind: dumpKindMemory, at: at, name: stamp + "-" + profile + ".pb.gz", body: buf.Bytes(), contentType: contentPprof})
	}

	return out
}

// --- delivery ------------------------------------------------------------------

// key is where an artifact lives in the sink:
//
//	<prefix>/<service>/dt=2026-11-11/<instance>/20261111T043806.123Z-heap.pb.gz
//
// One listing per service and day finds everything; the instance keeps pods
// apart; the stamp groups the artifacts of one event.
func (d *diagnostics) key(at time.Time, name string) string {
	return strings.Join([]string{
		strings.Trim(d.cfg.DiagKeyPrefix, "/"), d.build.Service, "dt=" + at.UTC().Format(time.DateOnly), d.instance, name,
	}, "/")
}

func (d *diagnostics) upload(ctx context.Context, art artifact) error {
	if d.sink == nil {
		return errors.New("no dump sink installed")
	}

	// Its own deadline: a store that stopped answering must not hang the
	// watcher.
	ctx, cancel := context.WithTimeout(ctx, d.cfg.DiagUploadTimeout)
	defer cancel()

	return d.sink.Put(ctx, d.key(art.at, art.name), art.body, art.contentType)
}

// deliver ships an artifact, or leaves it in the dump directory for a later
// try when there is no sink or the sink failed.
func (d *diagnostics) deliver(ctx context.Context, art artifact) {
	err := d.upload(ctx, art)
	if err == nil {
		d.dumps.WithLabelValues(art.kind, dumpUploaded).Inc()
		d.log.InfoContext(ctx, "diagnostics: artifact uploaded", "kind", art.kind, "key", d.key(art.at, art.name), "bytes", len(art.body))

		return
	}

	if d.cfg.DiagDumpDir == "" {
		d.dumps.WithLabelValues(art.kind, dumpFailed).Inc()
		d.log.ErrorContext(ctx, "diagnostics: artifact lost: upload failed and there is no dump directory", "kind", art.kind, "name", art.name, "error", err.Error())

		return
	}

	path := filepath.Join(d.cfg.DiagDumpDir, art.name)
	if writeErr := os.WriteFile(path, art.body, 0o600); writeErr != nil {
		d.dumps.WithLabelValues(art.kind, dumpFailed).Inc()
		d.log.ErrorContext(ctx, "diagnostics: artifact lost: neither uploaded nor written", "kind", art.kind, "name", art.name,
			"upload_error", err.Error(), "error", writeErr.Error())

		return
	}

	d.dumps.WithLabelValues(art.kind, dumpSpooled).Inc()
	d.log.WarnContext(ctx, "diagnostics: artifact kept in the dump directory", "kind", art.kind, "path", path, "reason", err.Error())
	d.trimSpool(ctx)
}

// flushSpool ships what is waiting in the dump directory: crash files of
// earlier runs (an empty one is a run that was killed, not one that crashed)
// and artifacts whose upload failed. A file leaves the directory only after
// its upload succeeded.
func (d *diagnostics) flushSpool(ctx context.Context) {
	d.nextSpool = d.now().Add(d.cfg.DiagSpoolRetry)

	dir := d.cfg.DiagDumpDir
	if dir == "" {
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == d.ownPending {
			continue
		}

		path := filepath.Join(dir, name)

		info, err := entry.Info()
		if err != nil {
			continue
		}

		art, ok := d.spooled(path, name, info)
		if !ok {
			continue
		}

		if d.sink == nil {
			continue // kept for whoever looks into the directory; trimSpool bounds it
		}

		if err := d.upload(ctx, art); err != nil {
			d.log.WarnContext(ctx, "diagnostics: spooled artifact not uploaded yet", "name", name, "error", err.Error())
			break // the sink is down: the rest would fail the same way
		}

		d.dumps.WithLabelValues(art.kind, dumpUploaded).Inc()
		d.log.InfoContext(ctx, "diagnostics: spooled artifact uploaded", "kind", art.kind, "key", d.key(art.at, art.name), "bytes", len(art.body))

		_ = os.Remove(path)
	}

	d.trimSpool(ctx)
}

// spooled reads one file of the dump directory as an artifact. Files this
// package did not write are left alone.
func (d *diagnostics) spooled(path, name string, info fs.FileInfo) (artifact, bool) {
	if strings.HasPrefix(name, crashPendingPrefix) && strings.HasSuffix(name, crashPendingSuffix) {
		if info.Size() == 0 {
			_ = os.Remove(path) // killed or restarted without crashing

			return artifact{}, false
		}

		art, err := d.wrapCrash(path, info)

		return art, err == nil
	}

	stamp, what, found := strings.Cut(name, "-")
	if !found {
		return artifact{}, false
	}

	at, err := time.Parse(dumpStamp, stamp)
	if err != nil {
		return artifact{}, false
	}

	art := artifact{kind: dumpKindMemory, at: at, name: name, contentType: contentPprof}

	switch what {
	case "crash.json":
		art.kind, art.contentType = dumpKindCrash, contentJSON
	case "memory.json":
		art.contentType = contentJSON
	case "heap.pb.gz", "goroutine.pb.gz":
	default:
		return artifact{}, false
	}

	body, err := os.ReadFile(path) //nolint:gosec // a file this package wrote in the configured directory
	if err != nil {
		return artifact{}, false
	}

	art.body = body

	return art, true
}

// ownDumpFile reports whether name is one this package writes: a crash file,
// or "<stamp>-<what>".
func ownDumpFile(name string) bool {
	if strings.HasPrefix(name, crashPendingPrefix) && strings.HasSuffix(name, crashPendingSuffix) {
		return true
	}

	stamp, _, found := strings.Cut(name, "-")
	if !found {
		return false
	}

	_, err := time.Parse(dumpStamp, stamp)

	return err == nil
}

// trimSpool keeps the newest DIAG_SPOOL_MAX_FILES files: the directory is an
// emptyDir with a size limit, and a crash loop without a sink must not fill it.
func (d *diagnostics) trimSpool(ctx context.Context) {
	dir := d.cfg.DiagDumpDir

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	type aged struct {
		name string
		at   time.Time
	}

	files := make([]aged, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == d.ownPending || !ownDumpFile(entry.Name()) {
			continue
		}

		if info, err := entry.Info(); err == nil {
			files = append(files, aged{name: entry.Name(), at: info.ModTime()})
		}
	}

	if len(files) <= d.cfg.DiagSpoolMaxFiles {
		return
	}

	slices.SortFunc(files, func(a, b aged) int { return b.at.Compare(a.at) }) // newest first

	for _, old := range files[d.cfg.DiagSpoolMaxFiles:] {
		_ = os.Remove(filepath.Join(dir, old.name))
	}

	d.log.WarnContext(ctx, "diagnostics: dump directory trimmed", "dir", dir, "removed", len(files)-d.cfg.DiagSpoolMaxFiles)
}
