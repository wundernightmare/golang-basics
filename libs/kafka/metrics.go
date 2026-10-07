package kafka

import (
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Buckets for per-record handler / per-publish latency: sub-millisecond for a
// local ack up to the seconds a slow downstream or a broker hiccup costs.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// producerMetrics is the instrument set behind [Producer.Collectors].
type producerMetrics struct {
	records  *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newProducerMetrics() *producerMetrics {
	return &producerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_publish_total",
			Help: "Records published, by topic and result (ok|timeout|too_large|auth|unknown_topic|other).",
		}, []string{"topic", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafka_producer_publish_duration_seconds",
			Help:    "Time from Publish to broker acknowledgement (or failure), by topic.",
			Buckets: latencyBuckets,
		}, []string{"topic"}),
	}
}

// Collectors returns the producer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_publish_total{topic,result="ok|timeout|too_large|auth|unknown_topic|other"}
//	kafka_producer_publish_duration_seconds{topic}
func (p *Producer) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.metrics.records, p.metrics.duration}
}

// Consume results, one per record, by final outcome.
const (
	consumeOK           = "ok"            // handled on the first attempt
	consumeRetried      = "retried"       // handled after one or more retries
	consumeDeadLettered = "dead_lettered" // parked on the DLQ topic and committed
	consumeFailed       = "failed"        // not handled and not parked: the consumer stopped
)

// consumerMetrics is the instrument set behind [Consumer.Collectors].
type consumerMetrics struct {
	records    *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	commitErrs prometheus.Counter
	dlq        *prometheus.CounterVec
	fetchErrs  *prometheus.CounterVec
	assigned   *prometheus.GaugeVec
	rebalances *prometheus.CounterVec
	lag        *lagCollector
}

func newConsumerMetrics() *consumerMetrics {
	return &consumerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consume_total",
			Help: "Records consumed, by topic and final outcome (ok|retried|dead_lettered|failed).",
		}, []string{"topic", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafka_consumer_handler_duration_seconds",
			Help:    "Duration of one handler attempt, by topic.",
			Buckets: latencyBuckets,
		}, []string{"topic"}),
		commitErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_consumer_commit_errors_total",
			Help: "Offset commits that failed (the records stay pending and are retried or redelivered).",
		}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_dlq_total",
			Help: "Records written to a dead-letter topic, by dead-letter topic.",
		}, []string{"topic"}),
		fetchErrs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_fetch_errors_total",
			Help: "Fetch errors reported by the client, by topic.",
		}, []string{"topic"}),
		assigned: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_assigned_partitions",
			Help: "Partitions currently assigned to this group member, by topic.",
		}, []string{"topic"}),
		rebalances: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_rebalances_total",
			Help: "Group rebalance callbacks seen by this member, by event (assigned|revoked|lost).",
		}, []string{"event"}),
		lag: newLagCollector(),
	}
}

// Collectors returns the consumer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_consume_total{topic,result="ok|retried|dead_lettered|failed"}
//	kafka_consumer_handler_duration_seconds{topic}
//	kafka_consumer_commit_errors_total
//	kafka_consumer_dlq_total{topic}          (the dead-letter topic)
//	kafka_consumer_fetch_errors_total{topic}
//	kafka_consumer_assigned_partitions{topic}
//	kafka_consumer_rebalances_total{event="assigned|revoked|lost"}
//	kafka_consumer_group_lag{topic,partition} (partitions this member owns)
//	kafka_consumer_lag_poll_errors_total
//	kafka_consumer_lag_last_success_timestamp_seconds
//
// Lag is the consumer's availability indicator: sum by (topic) of the gauge
// growing while kafka_consume_total is flat means the worker is stuck,
// growing while it climbs means it is under-provisioned. Each member reports
// only the partitions it owns, so a sum across replicas counts each
// partition once.
func (c *Consumer) Collectors() []prometheus.Collector {
	m := c.metrics
	return []prometheus.Collector{m.records, m.duration, m.commitErrs, m.dlq, m.fetchErrs, m.assigned, m.rebalances, m.lag}
}

// lagPoint is one partition's lag in a snapshot.
type lagPoint struct {
	topic     string
	partition int32
	lag       int64
}

// lagCollector exposes the latest lag snapshot. The poller builds a whole new
// snapshot and swaps it in atomically, so a scrape sees either the previous
// set or the next one — never a half-reset vector (the old GaugeVec.Reset +
// Set sequence could be scraped in between).
type lagCollector struct {
	snapshot    atomic.Pointer[[]lagPoint]
	lastSuccess atomic.Int64 // unix nanos, 0 = never
	pollErrors  prometheus.Counter

	lagDesc, lastDesc *prometheus.Desc
}

func newLagCollector() *lagCollector {
	return &lagCollector{
		pollErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_consumer_lag_poll_errors_total",
			Help: "Consumer-lag polls that failed (the previous snapshot stays exposed).",
		}),
		lagDesc: prometheus.NewDesc("kafka_consumer_group_lag",
			"Records not yet committed by this consumer group, per partition this member owns (polled every KAFKA_LAG_INTERVAL).",
			[]string{"topic", "partition"}, nil),
		lastDesc: prometheus.NewDesc("kafka_consumer_lag_last_success_timestamp_seconds",
			"Unix time of the last successful consumer-lag poll (0 = none yet).", nil, nil),
	}
}

// set replaces the snapshot.
func (l *lagCollector) set(points []lagPoint) { l.snapshot.Store(&points) }

// drop removes the given partitions from the snapshot (they were revoked or
// lost, so this member must stop reporting them before the next poll).
func (l *lagCollector) drop(gone map[string][]int32) {
	cur := l.snapshot.Load()
	if cur == nil {
		return
	}
	isGone := func(topic string, p int32) bool {
		for _, q := range gone[topic] {
			if q == p {
				return true
			}
		}
		return false
	}
	next := make([]lagPoint, 0, len(*cur))
	for _, pt := range *cur {
		if !isGone(pt.topic, pt.partition) {
			next = append(next, pt)
		}
	}
	l.set(next)
}

// Describe implements prometheus.Collector.
func (l *lagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- l.lagDesc
	ch <- l.lastDesc
	l.pollErrors.Describe(ch)
}

// Collect implements prometheus.Collector.
func (l *lagCollector) Collect(ch chan<- prometheus.Metric) {
	if pts := l.snapshot.Load(); pts != nil {
		for _, pt := range *pts {
			ch <- prometheus.MustNewConstMetric(l.lagDesc, prometheus.GaugeValue, float64(pt.lag),
				pt.topic, strconv.Itoa(int(pt.partition)))
		}
	}
	var ts float64
	if ns := l.lastSuccess.Load(); ns > 0 {
		ts = float64(ns) / 1e9
	}
	ch <- prometheus.MustNewConstMetric(l.lastDesc, prometheus.GaugeValue, ts)
	l.pollErrors.Collect(ch)
}
