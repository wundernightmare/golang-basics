package tiercache

import (
	"github.com/VictoriaMetrics/fastcache"
	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds the counters resolved once per cache, so the read path pays
// an atomic add and no label lookup.
type metrics struct {
	fresh, stale, fromL2, loaded, staleError, notFound, failed prometheus.Counter
	refreshOK, refreshError, refreshDropped                    prometheus.Counter
	invalidatedKey, invalidatedAll                             prometheus.Counter
	bypassed, oversize                                         prometheus.Counter

	collectors []prometheus.Collector
}

func newMetrics(name string, local *fastcache.Cache) *metrics {
	labels := prometheus.Labels{"cache": name}

	reads := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "tiercache_reads_total",
		Help:        "Reads by outcome: fresh or stale (served from L1), l2, load (source of truth), stale_error (expired value served because the source failed), not_found, error.",
		ConstLabels: labels,
	}, []string{"result"})
	refreshes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "tiercache_refreshes_total",
		Help:        "Background refreshes of stale entries by outcome: ok, error, dropped (the refresh limit was reached).",
		ConstLabels: labels,
	}, []string{"result"})
	invalidations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "tiercache_invalidations_total",
		Help:        "L1 invalidations by scope: key (one entry), all (a reset after the subscription was re-established).",
		ConstLabels: labels,
	}, []string{"scope"})
	bypassed := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "tiercache_l2_bypassed_total",
		Help:        "Lookups that skipped L2 because its circuit breaker was open.",
		ConstLabels: labels,
	})
	oversize := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "tiercache_l1_oversize_total",
		Help:        "Entries not stored in L1 because they exceed its per-entry limit.",
		ConstLabels: labels,
	})
	size := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "tiercache_l1_bytes",
		Help:        "Bytes of off-heap memory L1 currently holds (bounded by its configured capacity).",
		ConstLabels: labels,
	}, func() float64 {
		var stats fastcache.Stats

		local.UpdateStats(&stats)

		return float64(stats.BytesSize)
	})

	return &metrics{
		fresh:          reads.WithLabelValues("fresh"),
		stale:          reads.WithLabelValues("stale"),
		fromL2:         reads.WithLabelValues("l2"),
		loaded:         reads.WithLabelValues("load"),
		staleError:     reads.WithLabelValues("stale_error"),
		notFound:       reads.WithLabelValues("not_found"),
		failed:         reads.WithLabelValues("error"),
		refreshOK:      refreshes.WithLabelValues("ok"),
		refreshError:   refreshes.WithLabelValues("error"),
		refreshDropped: refreshes.WithLabelValues("dropped"),
		invalidatedKey: invalidations.WithLabelValues("key"),
		invalidatedAll: invalidations.WithLabelValues("all"),
		bypassed:       bypassed,
		oversize:       oversize,
		collectors:     []prometheus.Collector{reads, refreshes, invalidations, bypassed, oversize, size},
	}
}

// Collectors returns the cache's Prometheus collectors for the service to
// register on its metrics registry; every series carries cache="<Config.Name>":
//
//	tiercache_reads_total{result="fresh|stale|l2|load|stale_error|not_found|error"}
//	tiercache_refreshes_total{result="ok|error|dropped"}
//	tiercache_invalidations_total{scope="key|all"}
//	tiercache_l2_bypassed_total
//	tiercache_l1_oversize_total
//	tiercache_l1_bytes
//
// The L1 hit ratio is rate(fresh + stale) / rate(all results); refreshes are
// the traffic stale-while-revalidate adds to L2 and the source.
func (c *Cache[T]) Collectors() []prometheus.Collector { return c.metrics.collectors }
