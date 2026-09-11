package valkey

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetricsAreLintClean(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := newLookupCounter()
	reg.MustRegister(c)
	for _, r := range []string{"hit", "miss", "error"} {
		c.WithLabelValues(r).Inc()
	}
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	require.Empty(t, problems)
}
