package valkey_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// Commands are client spans in the caller's trace; hit / miss counters match
// what the cache actually did. Real Valkey (testcontainers).
func TestCommandsAreTracedAndLookupsCounted(t *testing.T) {
	cfg := startValkey(t)
	sr := installRecorder(t)
	ctx := context.Background()

	c, err := valkey.New(cfg, nil)
	require.NoError(t, err)
	defer c.Close()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)

	cctx, parent := otel.Tracer("test").Start(ctx, "handler")
	_, ok, err := c.Get(cctx, "telemetry:k")
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, c.Set(cctx, "telemetry:k", "v", time.Minute))
	v, ok, err := c.Get(cctx, "telemetry:k")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v", v)
	parent.End()

	require.Equal(t, 1.0, metricValue(t, reg, "cache_lookups_total", map[string]string{"result": "miss"}))
	require.Equal(t, 1.0, metricValue(t, reg, "cache_lookups_total", map[string]string{"result": "hit"}))
	require.Equal(t, -1.0, metricValue(t, reg, "cache_lookups_total", map[string]string{"result": "error"}))
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)

	names := map[string]int{}
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() == parent.SpanContext().TraceID() && s.SpanKind() == trace.SpanKindClient {
			names[strings.ToUpper(strings.Fields(s.Name())[0])]++
		}
	}
	require.Equal(t, 2, names["GET"], "two GET spans in the handler's trace")
	require.Equal(t, 1, names["SET"])
}
