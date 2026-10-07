package valkey

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	lookups  *prometheus.CounterVec
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
}

func newMetrics() *metrics {
	return &metrics{
		lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_lookups_total",
			Help: "Cache GETs by outcome: hit, miss (absent or tombstoned), or error (lookup itself failed).",
		}, []string{"result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_command_duration_seconds",
			Help:    "Cache command latency in seconds by operation (get, set, del).",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5},
		}, []string{"op"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_command_errors_total",
			Help: "Cache commands that failed (timeouts included; a missing key is not a failure) by operation.",
		}, []string{"op"}),
	}
}

// Collectors returns the cache's Prometheus collectors for the service to
// register on its metrics registry:
//
//	cache_lookups_total{result="hit|miss|error"}
//	cache_command_duration_seconds{op="get|set|del"}
//	cache_command_errors_total{op="get|set|del"}
//
// The hit ratio is rate(hit) / rate(hit + miss). Errors rising while the
// readiness check stays green usually means OpTimeout is too tight for the
// latency the histogram shows.
func (c *Cache) Collectors() []prometheus.Collector {
	return []prometheus.Collector{c.metrics.lookups, c.metrics.duration, c.metrics.errors}
}
