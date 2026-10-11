package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Buckets for per-record handler / per-send latency: sub-millisecond for a
// local ack up to the seconds a slow downstream or a broker hiccup costs.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// The metric names are the same whichever provider is linked: a dashboard
// or an alert never has to know which client a service was built with
// (kafka_client_info{provider} says it). No topic or partition label on the
// client gauges — per-partition lag is the broker side's job (exported
// once, not per pod), and a service may consume hundreds of topics.

// producerMetrics is the instrument set behind [Producer.Collectors].
type producerMetrics struct {
	records  *prometheus.CounterVec
	duration *prometheus.HistogramVec
	client   *clientCollector
}

func newProducerMetrics(stats func() ClientStats, provider string) *producerMetrics {
	return &producerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_publish_total",
			Help: "Records sent, by topic and result (ok, or the SendError kind: queue_full|unavailable|not_provisioned|fatal|rejected|unknown).",
		}, []string{"topic", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafka_producer_publish_duration_seconds",
			Help:    "Time from Send to broker acknowledgement (or failure), by topic.",
			Buckets: latencyBuckets,
		}, []string{"topic"}),
		client: newClientCollector("producer", provider, stats),
	}
}

// Collectors returns the producer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_publish_total{topic,result}
//	kafka_producer_publish_duration_seconds{topic}
//	kafka_client_info{client="producer",provider}
//	kafka_client_brokers_up{client="producer"}
//	kafka_client_request_errors_total{client="producer"}
//	kafka_producer_queue_bytes          bytes waiting in the local send queue …
//	kafka_producer_queue_max_bytes      … and its bound (KAFKA_PRODUCER_QUEUE_MAX_KBYTES): full → queue_full
func (p *Producer) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.metrics.records, p.metrics.duration, p.metrics.client}
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
	fetchErrs  prometheus.Counter
	assigned   prometheus.Gauge
	rebalances *prometheus.CounterVec
	client     *clientCollector
}

func newConsumerMetrics(stats func() ClientStats, provider string) *consumerMetrics {
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
		fetchErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_consumer_fetch_errors_total",
			Help: "Fetch errors reported by the client.",
		}),
		assigned: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kafka_consumer_assigned_partitions",
			Help: "Partitions currently assigned to this group member.",
		}),
		rebalances: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_rebalances_total",
			Help: "Group rebalance callbacks seen by this member, by event (assigned|revoked|lost).",
		}, []string{"event"}),
		client: newClientCollector("consumer", provider, stats),
	}
}

// Collectors returns the consumer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_consume_total{topic,result="ok|retried|dead_lettered|failed"}
//	kafka_consumer_handler_duration_seconds{topic}
//	kafka_consumer_commit_errors_total
//	kafka_consumer_dlq_total{topic}          (the dead-letter topic)
//	kafka_consumer_fetch_errors_total
//	kafka_consumer_assigned_partitions
//	kafka_consumer_rebalances_total{event="assigned|revoked|lost"}
//	kafka_client_info{client="consumer",provider}
//	kafka_client_brokers_up{client="consumer"}
//	kafka_client_request_errors_total{client="consumer"}
//	kafka_consumer_lag_max                   worst assigned partition (records behind its end)
//	kafka_consumer_lag                   total over the assigned partitions
//
// Lag is the consumer's availability indicator: lag_sum summed over the
// replicas growing while kafka_consume_total is flat means the worker is
// stuck, growing while it climbs means it is under-provisioned. Each member
// reports only the partitions it owns, so a sum across replicas counts each
// partition once. The client gauges refresh every
// KAFKA_STATISTICS_INTERVAL_MS and disappear when the client stops
// reporting.
func (c *Consumer) Collectors() []prometheus.Collector {
	m := c.metrics
	return []prometheus.Collector{m.records, m.duration, m.commitErrs, m.dlq, m.fetchErrs, m.assigned, m.rebalances, m.client}
}

// clientCollector exposes a driver's [ClientStats] at scrape time.
type clientCollector struct {
	role  string
	stats func() ClientStats

	info, brokersUp, requestErrors, queueBytes, queueMax, lagMax, lagSum *prometheus.Desc
}

func newClientCollector(role, provider string, stats func() ClientStats) *clientCollector {
	l := prometheus.Labels{"client": role}
	return &clientCollector{
		role:  role,
		stats: stats,
		info: prometheus.NewDesc("kafka_client_info", "The Kafka client library this service was built with (always 1).",
			nil, prometheus.Labels{"client": role, "provider": provider}),
		brokersUp: prometheus.NewDesc("kafka_client_brokers_up",
			"Brokers this client has a live connection to.", nil, l),
		requestErrors: prometheus.NewDesc("kafka_client_request_errors_total",
			"Request timeouts and transport errors, summed over brokers.", nil, l),
		queueBytes: prometheus.NewDesc("kafka_producer_queue_bytes",
			"Bytes waiting in the producer's local send queue.", nil, nil),
		queueMax: prometheus.NewDesc("kafka_producer_queue_max_bytes",
			"The local send queue's bound (KAFKA_PRODUCER_QUEUE_MAX_KBYTES); full → queue_full.", nil, nil),
		lagMax: prometheus.NewDesc("kafka_consumer_lag_max",
			"Largest lag (records behind the partition end) over the assigned partitions.", nil, nil),
		lagSum: prometheus.NewDesc("kafka_consumer_lag",
			"Total lag over the assigned partitions.", nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *clientCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.info
	ch <- c.brokersUp
	ch <- c.requestErrors
	if c.role == "producer" {
		ch <- c.queueBytes
		ch <- c.queueMax
	} else {
		ch <- c.lagMax
		ch <- c.lagSum
	}
}

// Collect implements prometheus.Collector.
func (c *clientCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1)
	s := c.stats()
	if !s.Valid {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.brokersUp, prometheus.GaugeValue, float64(s.BrokersUp))
	ch <- prometheus.MustNewConstMetric(c.requestErrors, prometheus.CounterValue, float64(s.RequestErrors))
	if c.role == "producer" {
		ch <- prometheus.MustNewConstMetric(c.queueBytes, prometheus.GaugeValue, float64(s.QueueBytes))
		ch <- prometheus.MustNewConstMetric(c.queueMax, prometheus.GaugeValue, float64(s.QueueMaxBytes))
		return
	}
	ch <- prometheus.MustNewConstMetric(c.lagMax, prometheus.GaugeValue, float64(s.LagMax))
	ch <- prometheus.MustNewConstMetric(c.lagSum, prometheus.GaugeValue, float64(s.LagSum))
}
