package valkey_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// One Valkey per test binary (testx), tests own their keys by prefix.
func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }

func TestLoadConfigDefaults(t *testing.T) {
	testx.Run(t, func(t testx.T) {
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
	}, "valkey", "unit")
}

func sharedCache(t testing.TB) *valkey.Cache {
	t.Helper()
	c, err := valkey.New(valkey.Config{URL: testx.Valkey(t), DialTimeout: 5 * time.Second}, nil)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestCacheGetSetDel(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		c := sharedCache(t)
		ctx := context.Background()
		k := testx.Unique("k") + ":"

		require.NoError(t, c.ReadyCheck()(ctx))

		// Miss on an absent key.
		_, ok, err := c.Get(ctx, k+"absent")
		require.NoError(t, err)
		require.False(t, ok)

		// Round-trip a value.
		require.NoError(t, c.Set(ctx, k+"greeting", "pong", time.Minute))
		got, ok, err := c.Get(ctx, k+"greeting")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "pong", got)

		// TTL is honoured.
		require.NoError(t, c.Set(ctx, k+"ephemeral", "x", time.Second))
		require.Eventually(t, func() bool {
			_, ok, _ := c.Get(ctx, k+"ephemeral")
			return !ok
		}, 4*time.Second, 200*time.Millisecond, "key should expire")

		// Delete removes it.
		require.NoError(t, c.Del(ctx, k+"greeting"))
		_, ok, err = c.Get(ctx, k+"greeting")
		require.NoError(t, err)
		require.False(t, ok)
	}, "valkey", "integration")
}

type widget struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestAsideLoadsOnceThenServesFromCache(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		c := sharedCache(t)
		ctx := context.Background()
		key := testx.Unique("widget") + ":7"

		calls := 0
		load := func(ctx context.Context) (widget, error) {
			calls++
			return widget{ID: 7, Name: "gadget"}, nil
		}

		// First call: miss → loads and caches.
		w, hit, err := valkey.Aside(ctx, c, key, time.Minute, load)
		require.NoError(t, err)
		require.False(t, hit)
		require.Equal(t, widget{ID: 7, Name: "gadget"}, w)
		require.Equal(t, 1, calls)

		// Second call: hit → served from cache, loader not invoked again.
		w, hit, err = valkey.Aside(ctx, c, key, time.Minute, load)
		require.NoError(t, err)
		require.True(t, hit)
		require.Equal(t, widget{ID: 7, Name: "gadget"}, w)
		require.Equal(t, 1, calls, "loader must not be called on a cache hit")
	}, "valkey", "integration")
}

// Commands are client spans in the caller's trace; hit / miss counters match
// what the cache actually did.
func TestCommandsAreTracedAndLookupsCounted(t *testing.T) {
	testx.Run(t, func(t testx.T) {
		sr := testx.Recorder(t) // before the client: valkeyotel captures the provider then
		c := sharedCache(t)
		ctx := context.Background()
		key := testx.Unique("telemetry")
		reg := prometheus.NewRegistry()
		reg.MustRegister(c.Collectors()...)

		cctx, parent := otel.Tracer("test").Start(ctx, "handler")
		_, ok, err := c.Get(cctx, key)
		require.NoError(t, err)
		require.False(t, ok)
		require.NoError(t, c.Set(cctx, key, "v", time.Minute))
		v, ok, err := c.Get(cctx, key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "v", v)
		parent.End()

		require.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "miss"}))
		require.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "hit"}))
		require.Equal(t, -1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "error"}))
		testx.LintMetrics(t, reg)

		names := map[string]int{}
		for _, s := range sr.Ended() {
			if s.SpanContext().TraceID() == parent.SpanContext().TraceID() && s.SpanKind() == trace.SpanKindClient {
				names[strings.ToUpper(strings.Fields(s.Name())[0])]++
			}
		}
		require.Equal(t, 2, names["GET"], "two GET spans in the handler's trace")
		require.Equal(t, 1, names["SET"])
	}, "valkey", "integration", "telemetry")
}
