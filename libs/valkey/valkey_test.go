package valkey_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"

	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := valkey.LoadConfig("TASKS_")
	require.NoError(t, err)
	require.Equal(t, "localhost:6379", cfg.Addr)
	require.Equal(t, 0, cfg.DB)

	t.Setenv("TASKS_VALKEY_ADDR", "cache:6380")
	t.Setenv("TASKS_VALKEY_DB", "3")
	cfg, err = valkey.LoadConfig("TASKS_")
	require.NoError(t, err)
	require.Equal(t, "cache:6380", cfg.Addr)
	require.Equal(t, 3, cfg.DB)
}

// startValkey spins up an ephemeral Valkey via testcontainers and returns a
// Config pointing at it. Skips under -short or when Docker is unavailable.
func startValkey(t *testing.T) valkey.Config {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}

	ctx := context.Background()
	container, err := tcvalkey.Run(ctx, "valkey/valkey:9.0")
	if err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			require.NoError(t, err, "valkey container must start in CI")
		}
		t.Skipf("could not start valkey container (docker unavailable?): %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	return valkey.Config{URL: uri, DialTimeout: 5 * time.Second}
}

func TestCacheGetSetDel(t *testing.T) {
	cfg := startValkey(t)
	ctx := context.Background()

	c, err := valkey.New(cfg, nil)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.ReadyCheck()(ctx))

	// Miss on an absent key.
	_, ok, err := c.Get(ctx, "absent")
	require.NoError(t, err)
	require.False(t, ok)

	// Round-trip a value.
	require.NoError(t, c.Set(ctx, "greeting", "pong", time.Minute))
	got, ok, err := c.Get(ctx, "greeting")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pong", got)

	// TTL is honoured.
	require.NoError(t, c.Set(ctx, "ephemeral", "x", time.Second))
	require.Eventually(t, func() bool {
		_, ok, _ := c.Get(ctx, "ephemeral")
		return !ok
	}, 4*time.Second, 200*time.Millisecond, "key should expire")

	// Delete removes it.
	require.NoError(t, c.Del(ctx, "greeting"))
	_, ok, err = c.Get(ctx, "greeting")
	require.NoError(t, err)
	require.False(t, ok)
}

type widget struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestAsideLoadsOnceThenServesFromCache(t *testing.T) {
	cfg := startValkey(t)
	ctx := context.Background()

	c, err := valkey.New(cfg, nil)
	require.NoError(t, err)
	defer c.Close()

	calls := 0
	load := func(ctx context.Context) (widget, error) {
		calls++
		return widget{ID: 7, Name: "gadget"}, nil
	}

	// First call: miss → loads and caches.
	w, hit, err := valkey.Aside(ctx, c, "widget:7", time.Minute, load)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, widget{ID: 7, Name: "gadget"}, w)
	require.Equal(t, 1, calls)

	// Second call: hit → served from cache, loader not invoked again.
	w, hit, err = valkey.Aside(ctx, c, "widget:7", time.Minute, load)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, widget{ID: 7, Name: "gadget"}, w)
	require.Equal(t, 1, calls, "loader must not be called on a cache hit")
}
