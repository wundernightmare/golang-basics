package compact

import "github.com/prometheus/client_golang/prometheus"

// Label values.
const (
	resultOK          = "ok"
	resultFailed      = "failed"
	resultWritten     = "written"
	resultDuplicate   = "duplicate"
	resultMerged      = "merged"
	resultQuarantined = "quarantined"
)

type metrics struct {
	passes        *prometheus.CounterVec
	hours         *prometheus.CounterVec
	events        *prometheus.CounterVec
	objects       *prometheus.CounterVec
	lastSuccess   prometheus.Gauge
	oldestPending prometheus.Gauge
}

func newMetrics() *metrics {
	return &metrics{
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_compaction_passes_total",
			Help: "Compaction passes over the archive, by result (ok, failed: at least one hour was not compacted).",
		}, []string{"result"}),
		hours: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_compaction_partitions_total",
			Help: "Closed hour partitions visited by compaction, by result (ok, failed).",
		}, []string{"result"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_compaction_events_total",
			Help: "Events read from raw objects, by result: written (into a compacted part) or duplicate (an id already seen in the run, dropped).",
		}, []string{"result"}),
		objects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_compaction_objects_total",
			Help: "Raw objects handled, by result: merged, or quarantined (not a readable shard, or too large).",
		}, []string{"result"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ingest_compaction_last_success_timestamp_seconds",
			Help: "Unix time of the last pass that compacted every closed hour.",
		}),
		oldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ingest_compaction_oldest_failed_hour_age_seconds",
			Help: "Age of the oldest hour the last pass could not compact (0 when none).",
		}),
	}
}

// Collectors returns the compactor's Prometheus collectors:
//
//	ingest_compaction_passes_total{result="ok|failed"}
//	ingest_compaction_partitions_total{result="ok|failed"}
//	ingest_compaction_events_total{result="written|duplicate"}
//	ingest_compaction_objects_total{result="merged|quarantined"}
//	ingest_compaction_last_success_timestamp_seconds
//	ingest_compaction_oldest_failed_hour_age_seconds
//
// Alert on time() - last_success growing past a few intervals, and on any
// quarantined object: it holds events the compacted hour does not.
func (c *Compactor) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		c.metrics.passes, c.metrics.hours, c.metrics.events, c.metrics.objects, c.metrics.lastSuccess, c.metrics.oldestPending,
	}
}
