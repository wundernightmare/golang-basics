package httpx

import (
	"bufio"
	"bytes"
	"context"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Runtime tuning. A service gets its Go runtime settings from the container
// it actually runs in, not from the manifest: an autoscaler (VPA) changes the
// container's resources, and a GOMEMLIMIT written next to them in a chart is
// then wrong in the dangerous direction. docs/RUNTIME.md has the reasoning.
//
//   - GOMAXPROCS is left to the runtime: since Go 1.25 it follows the cgroup
//     CPU limit by itself, and keeps following it.
//   - The memory limit has no such automation, so [Server.Run] sets it to
//     RUNTIME_MEMORY_LIMIT_RATIO of the cgroup memory limit and re-reads the
//     cgroup every RUNTIME_MEMORY_LIMIT_REFRESH (an in-place resize changes
//     the limit without a restart). A GOMEMLIMIT in the environment wins: the
//     operator said what they want.
//   - RUNTIME_TRACEBACK is applied before anything else.

const (
	defaultMemoryLimitRatio   = 0.85
	defaultMemoryLimitRefresh = 30 * time.Second
	defaultTraceback          = "all"

	// cgroup v1 reports "no limit" as a page-rounded MaxInt64; nothing real
	// is anywhere near this.
	cgroupUnlimited = int64(1) << 60
)

// validTraceback reports whether level is one of the values runtime/debug.SetTraceback understands.
func validTraceback(level string) bool {
	switch level {
	case "none", "single", "all", "system", "crash":
		return true
	default:
		return false
	}
}

// memoryLimiter keeps the runtime's soft memory limit at ratio × the cgroup
// limit. root and set are seams for the tests; in a server they are the real
// filesystem and debug.SetMemoryLimit.
type memoryLimiter struct {
	root    fs.FS
	ratio   float64
	set     func(int64) int64
	log     *slog.Logger
	applied int64 // what was last handed to set; 0 = nothing yet
}

// sync reads the cgroup limit and moves the runtime limit when it changed.
func (m *memoryLimiter) sync(ctx context.Context) {
	target := int64(math.MaxInt64) // the runtime's "no limit"

	limit, limited := cgroupMemoryLimit(m.root)
	if limited {
		target = int64(float64(limit) * m.ratio)
	}

	if target == m.applied || (!limited && m.applied == 0) {
		return
	}

	m.set(target)
	m.applied = target

	if !limited {
		m.log.WarnContext(ctx, "container memory limit is gone, runtime memory limit removed")
		return
	}

	m.log.InfoContext(ctx, "runtime memory limit set from the container limit",
		"container_limit_bytes", limit, "ratio", m.ratio, "memory_limit_bytes", target)
}

// run re-reads the cgroup every interval until ctx is done.
func (m *memoryLimiter) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sync(ctx)
		}
	}
}

// tuneRuntime applies the runtime settings of cfg and, when asked to, keeps
// the memory limit in step with the container until ctx is done.
func (s *Server) tuneRuntime(ctx context.Context) {
	debug.SetTraceback(s.cfg.RuntimeTraceback)

	if s.cfg.RuntimeMemoryLimitRatio < 0 {
		return
	}

	if explicit := os.Getenv("GOMEMLIMIT"); explicit != "" {
		s.log.InfoContext(ctx, "GOMEMLIMIT is set in the environment, the container limit is not consulted", "gomemlimit", explicit)
		return
	}

	limiter := &memoryLimiter{root: os.DirFS("/"), ratio: s.cfg.RuntimeMemoryLimitRatio, set: debug.SetMemoryLimit, log: s.log}
	limiter.sync(ctx)

	if limiter.applied == 0 {
		s.log.InfoContext(ctx, "no container memory limit found, runtime memory limit left unset")
	}

	if s.cfg.RuntimeMemoryLimitRefresh > 0 {
		go limiter.run(ctx, s.cfg.RuntimeMemoryLimitRefresh)
	}
}

// cgroupMemoryLimit returns the memory limit, in bytes, of the cgroup this
// process runs in: the smallest memory.max on the way from its cgroup to the
// root (a limit on a parent binds the child too), or the cgroup v1 limit.
// limited is false when there is none, or no cgroup filesystem at all.
func cgroupMemoryLimit(root fs.FS) (limit int64, limited bool) {
	limit, _, _, limited = cgroupMemoryLimitAt(root)
	return limit, limited
}

// cgroupMemoryLimitAt is [cgroupMemoryLimit] that also says which cgroup
// directory carries the limit (a path under root) and whether it is v1: the
// usage that counts against a limit is the usage of that same cgroup.
func cgroupMemoryLimitAt(root fs.FS) (limit int64, dir string, v1, limited bool) {
	if _, err := fs.Stat(root, "sys/fs/cgroup/cgroup.controllers"); err != nil {
		limit, limited = readLimit(root, cgroupV1Dir+"/memory.limit_in_bytes")
		return limit, cgroupV1Dir, true, limited
	}

	limit = math.MaxInt64

	// Inside a container the cgroup namespace makes this "/"; outside one it
	// is the real path and the ancestors matter.
	for group := cgroupV2Path(root); ; group = path.Dir(group) {
		at := path.Join("sys/fs/cgroup", group)
		if value, ok := readLimit(root, at+"/memory.max"); ok && value < limit {
			limit, dir, limited = value, at, true
		}

		if group == "/" || group == "." {
			return limit, dir, false, limited
		}
	}
}

const cgroupV1Dir = "sys/fs/cgroup/memory"

// cgroupMemoryUsage returns what the limited cgroup uses against its limit:
// the working set — usage minus inactive file cache, the figure the kubelet
// reports and evicts on. Page cache the kernel can drop at no cost is not
// memory pressure. ok is false without a limit.
func cgroupMemoryUsage(root fs.FS) (usage, limit int64, ok bool) {
	limit, dir, v1, limited := cgroupMemoryLimitAt(root)
	if !limited {
		return 0, 0, false
	}

	usageFile, inactiveKey := "memory.current", "inactive_file"
	if v1 {
		usageFile, inactiveKey = "memory.usage_in_bytes", "total_inactive_file"
	}

	usage, ok = readLimit(root, dir+"/"+usageFile)
	if !ok {
		return 0, 0, false
	}

	if inactive := cgroupStat(root, dir+"/memory.stat", inactiveKey); inactive < usage {
		usage -= inactive
	}

	return usage, limit, true
}

// cgroupStat reads one "key value" line of a memory.stat file; 0 when the
// file or the key is missing.
func cgroupStat(root fs.FS, name, key string) int64 {
	raw, err := fs.ReadFile(root, name)
	if err != nil {
		return 0
	}

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		if field, value, found := strings.Cut(scanner.Text(), " "); found && field == key {
			n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil || n < 0 {
				return 0
			}

			return n
		}
	}

	return 0
}

// cgroupV2Path is this process's cgroup, from the "0::<path>" line of
// /proc/self/cgroup; "/" when it cannot be read.
func cgroupV2Path(root fs.FS) string {
	raw, err := fs.ReadFile(root, "proc/self/cgroup")
	if err != nil {
		return "/"
	}

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		if dir, ok := strings.CutPrefix(scanner.Text(), "0::"); ok && strings.HasPrefix(dir, "/") {
			return path.Clean(dir)
		}
	}

	return "/"
}

// readLimit parses one cgroup byte-count file. "max" (v2), a value at or
// above cgroupUnlimited (v1), a missing file and garbage all mean "nothing
// here".
func readLimit(root fs.FS, name string) (int64, bool) {
	raw, err := fs.ReadFile(root, name)
	if err != nil {
		return 0, false
	}

	value, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || value <= 0 || value >= cgroupUnlimited {
		return 0, false
	}

	return value, true
}
