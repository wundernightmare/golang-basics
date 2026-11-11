package s3

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func TestMetricsAreLintClean(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics()
	reg.MustRegister(m.duration, m.errors)

	for _, op := range []string{
		opPut, opGet, opHead, opList, opDelete, opHeadBucket, opCreateBucket,
		opCreateUpload, opUploadPart, opCompleteUpload, opAbortUpload,
	} {
		m.duration.WithLabelValues(op).Observe(0.001)
		m.errors.WithLabelValues(op).Inc()
	}

	testx.LintMetrics(t, reg)
}
