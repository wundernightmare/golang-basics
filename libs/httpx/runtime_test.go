package httpx

import (
	"log/slog"
	"math"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func file(content string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(content)} }

func TestCgroupMemoryLimit(t *testing.T) {
	t.Parallel()

	const controllers = "sys/fs/cgroup/cgroup.controllers"

	cases := []struct {
		name    string
		root    fstest.MapFS
		limit   int64
		limited bool
	}{
		{
			name: "v2 inside a container: the namespaced root carries the limit",
			root: fstest.MapFS{
				controllers:                 file("cpu memory\n"),
				"proc/self/cgroup":          file("0::/\n"),
				"sys/fs/cgroup/memory.max":  file("268435456\n"),
				"sys/fs/cgroup/memory.high": file("max\n"),
			},
			limit: 268435456, limited: true,
		},
		{
			name: "v2 without a limit",
			root: fstest.MapFS{
				controllers:                file("cpu memory\n"),
				"proc/self/cgroup":         file("0::/\n"),
				"sys/fs/cgroup/memory.max": file("max\n"),
			},
		},
		{
			name: "v2 outside a namespace: the tightest ancestor binds",
			root: fstest.MapFS{
				controllers:        file("cpu memory\n"),
				"proc/self/cgroup": file("0::/kubepods/pod1/app\n"),
				"sys/fs/cgroup/kubepods/pod1/app/memory.max": file("max\n"),
				"sys/fs/cgroup/kubepods/pod1/memory.max":     file("536870912\n"),
				"sys/fs/cgroup/kubepods/memory.max":          file("8589934592\n"),
			},
			limit: 536870912, limited: true,
		},
		{
			name: "v2 with an unreadable /proc/self/cgroup falls back to the root",
			root: fstest.MapFS{
				controllers:                file("memory\n"),
				"sys/fs/cgroup/memory.max": file("1048576\n"),
			},
			limit: 1048576, limited: true,
		},
		{
			name: "v1 limit",
			root: fstest.MapFS{
				"sys/fs/cgroup/memory/memory.limit_in_bytes": file("134217728\n"),
			},
			limit: 134217728, limited: true,
		},
		{
			name: "v1 reports no limit as a huge number",
			root: fstest.MapFS{
				"sys/fs/cgroup/memory/memory.limit_in_bytes": file("9223372036854771712\n"),
			},
		},
		{
			name: "garbage is not a limit",
			root: fstest.MapFS{
				controllers:                file("memory\n"),
				"proc/self/cgroup":         file("0::/\n"),
				"sys/fs/cgroup/memory.max": file("lots\n"),
			},
		},
		{name: "no cgroup filesystem at all", root: fstest.MapFS{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			limit, limited := cgroupMemoryLimit(tc.root)
			assert.Equal(t, tc.limited, limited)

			if tc.limited {
				assert.Equal(t, tc.limit, limit)
			}
		})
	}
}

// The limiter follows the container: it sets ratio × limit, does nothing
// while the limit stands still, moves on a resize, and lifts the runtime
// limit when the container limit disappears.
func TestMemoryLimiter_FollowsTheContainerLimit(t *testing.T) {
	t.Parallel()

	root := fstest.MapFS{
		"sys/fs/cgroup/cgroup.controllers": file("memory\n"),
		"proc/self/cgroup":                 file("0::/\n"),
		"sys/fs/cgroup/memory.max":         file("1000000000\n"),
	}

	var calls []int64

	limiter := &memoryLimiter{
		root: root, ratio: 0.8, log: slog.New(slog.DiscardHandler),
		set: func(v int64) int64 { calls = append(calls, v); return 0 },
	}

	limiter.sync(t.Context())
	limiter.sync(t.Context())
	require.Equal(t, []int64{800000000}, calls, "an unchanged limit is set once")

	root["sys/fs/cgroup/memory.max"] = file("2000000000\n") // in-place resize

	limiter.sync(t.Context())
	require.Equal(t, []int64{800000000, 1600000000}, calls)

	root["sys/fs/cgroup/memory.max"] = file("max\n")

	limiter.sync(t.Context())
	limiter.sync(t.Context())
	require.Equal(t, []int64{800000000, 1600000000, math.MaxInt64}, calls, "no container limit: the runtime limit is lifted, once")
}

func TestMemoryLimiter_NoLimitTouchesNothing(t *testing.T) {
	t.Parallel()

	limiter := &memoryLimiter{
		root: fstest.MapFS{}, ratio: 0.8, log: slog.New(slog.DiscardHandler),
		set: func(int64) int64 { t.Error("the runtime limit must not be touched"); return 0 },
	}

	limiter.sync(t.Context())
	assert.Zero(t, limiter.applied)
}

func TestConfig_RuntimeKeys(t *testing.T) {
	t.Parallel()

	base := map[string]string{"ADMIN_INSECURE": "true"}

	cfg, err := LoadConfigFrom("", base)
	require.NoError(t, err)
	assert.InDelta(t, 0.85, cfg.RuntimeMemoryLimitRatio, 1e-9)
	assert.Equal(t, defaultMemoryLimitRefresh, cfg.RuntimeMemoryLimitRefresh)
	assert.Equal(t, "all", cfg.RuntimeTraceback)

	for key, value := range map[string]string{
		"RUNTIME_MEMORY_LIMIT_RATIO": "1.5",
		"RUNTIME_TRACEBACK":          "everything",
	} {
		_, err := LoadConfigFrom("", map[string]string{"ADMIN_INSECURE": "true", key: value})
		require.ErrorContains(t, err, key)
	}

	off, err := LoadConfigFrom("", map[string]string{"ADMIN_INSECURE": "true", "RUNTIME_MEMORY_LIMIT_RATIO": "-1", "RUNTIME_MEMORY_LIMIT_REFRESH": "-1s"})
	require.NoError(t, err)
	assert.Negative(t, off.RuntimeMemoryLimitRatio)
}
