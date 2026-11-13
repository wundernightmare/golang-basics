package s3

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
}

func newMetrics() *metrics {
	return &metrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "s3_request_duration_seconds",
			Help:    "S3 call latency in seconds by operation, the client's retries included.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"operation"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "s3_request_errors_total",
			Help: "S3 calls that failed (timeouts included; a missing key on get/head is not a failure) by operation.",
		}, []string{"operation"}),
	}
}

// Collectors returns the client's Prometheus collectors for the service to
// register on its metrics registry:
//
//	s3_request_duration_seconds{operation}
//	s3_request_errors_total{operation}
//
// operation is one of put, get, head, list, delete, head_bucket,
// create_bucket, create_upload, upload_part, complete_upload, abort_upload.
// The duration covers the whole call, retries included: it is what the
// caller waited, the number S3_OP_TIMEOUT is set against.
func (c *Client) Collectors() []prometheus.Collector {
	return []prometheus.Collector{c.metrics.duration, c.metrics.errors}
}
