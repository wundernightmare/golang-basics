package kafka

import (
	"errors"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Buckets for per-record handler / per-send latency: sub-millisecond for a
// local ack up to the minute a slow downstream or a broker outage costs.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

// The metric names are the workspace's metrics contract (docs/METRICS.md),
// shared with nodejs-basics and the same whichever provider is linked: a
// dashboard or an alert never has to know which client a service was built
// with (kafka_client_info{provider} says it). No partition label anywhere,
// and no consumer-group lag: lag is the broker's to export (Redpanda's
// /public_metrics, kafka-exporter or Burrow for Kafka) — once per group and
// partition, also while no member is running, instead of once per pod from
// whatever the pod happens to own.

// sendMetrics are the producer families. A [Producer] records its sends in
// them, a [Consumer] its dead-letter publishes; the const client label keeps
// the two apart, so both can be registered on one registry.
type sendMetrics struct {
	records  *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newSendMetrics(role string) *sendMetrics {
	l := prometheus.Labels{"client": role}
	return &sendMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "kafka_producer_records_total",
			Help:        "Records sent, by topic and result (ok, or the SendError kind: queue_full|unavailable|not_provisioned|fatal|rejected|unknown).",
			ConstLabels: l,
		}, []string{"topic", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "kafka_producer_send_duration_seconds",
			Help:        "Time from send to broker acknowledgement (or failure), by topic.",
			Buckets:     latencyBuckets,
			ConstLabels: l,
		}, []string{"topic"}),
	}
}

// observe records one send of a record to topic that started at start.
func (m *sendMetrics) observe(topic string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = KindUnknown
		var se *SendError
		if errors.As(err, &se) {
			result = se.Kind
		}
	}
	m.duration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	m.records.WithLabelValues(topic, result).Inc()
}

// producerMetrics is the instrument set behind [Producer.Collectors].
type producerMetrics struct {
	send   *sendMetrics
	client *clientCollector
}

func newProducerMetrics(stats func() ClientStats, provider string) *producerMetrics {
	return &producerMetrics{send: newSendMetrics("producer"), client: newClientCollector("producer", provider, stats)}
}

// Collectors returns the producer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_producer_records_total{client="producer",topic,result}
//	kafka_producer_send_duration_seconds{client="producer",topic}
//	kafka_client_info{client="producer",provider}
//	kafka_client_brokers_up{client="producer"}
//	kafka_client_request_errors_total{client="producer"}
//	kafka_producer_queue_bytes          bytes waiting in the local send queue …
//	kafka_producer_queue_max_bytes      … and its bound (KAFKA_PRODUCER_QUEUE_MAX_KBYTES): full → queue_full
func (p *Producer) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.metrics.send.records, p.metrics.send.duration, p.metrics.client}
}

// Consume results, one per record, by final outcome.
const (
	consumeOK           = "ok"            // handled on the first attempt
	consumeRetried      = "retried"       // handled after one or more retries
	consumeDeadLettered = "dead_lettered" // parked on the DLQ topic and committed
	consumeFailed       = "failed"        // not handled and not parked: the consumer stopped
)

// otherTopic labels a record from a topic the configuration does not name
// literally (a ^regex subscription): the label set stays what the config
// says, not whatever the cluster grows.
const otherTopic = "other"

// consumerMetrics is the instrument set behind [Consumer.Collectors].
type consumerMetrics struct {
	records    *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	commitErrs prometheus.Counter
	fetchErrs  prometheus.Counter
	assigned   prometheus.Gauge
	rebalances *prometheus.CounterVec
	dlq        *sendMetrics
	client     *clientCollector
	topics     map[string]bool // the literally configured topics
}

func newConsumerMetrics(stats func() ClientStats, provider string, topics []string) *consumerMetrics {
	known := map[string]bool{}
	for _, t := range topics {
		if !strings.HasPrefix(t, "^") {
			known[t] = true
		}
	}
	return &consumerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_records_total",
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
		dlq:    newSendMetrics("consumer"),
		client: newClientCollector("consumer", provider, stats),
		topics: known,
	}
}

// topic is the label for a record from t.
func (m *consumerMetrics) topic(t string) string {
	if m.topics[t] {
		return t
	}
	return otherTopic
}

// Collectors returns the consumer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_consumer_records_total{topic,result="ok|retried|dead_lettered|failed"}
//	kafka_consumer_handler_duration_seconds{topic}
//	kafka_consumer_commit_errors_total
//	kafka_consumer_fetch_errors_total
//	kafka_consumer_assigned_partitions
//	kafka_consumer_rebalances_total{event="assigned|revoked|lost"}
//	kafka_producer_records_total{client="consumer",topic,result}   dead-letter publishes (topic: the DLQ topic)
//	kafka_producer_send_duration_seconds{client="consumer",topic}
//	kafka_client_info{client="consumer",provider}
//	kafka_client_brokers_up{client="consumer"}
//	kafka_client_request_errors_total{client="consumer"}
//
// topic is a configured topic name, or "other" for one matched by a ^regex
// subscription. How far behind the group is comes from the broker, not from
// here: kafka_consumer_records_total flat while the broker's group lag grows
// means the worker is stuck, both climbing means it is under-provisioned.
// The client gauges refresh every KAFKA_STATISTICS_INTERVAL_MS and disappear
// when the client stops reporting.
func (c *Consumer) Collectors() []prometheus.Collector {
	m := c.metrics
	return []prometheus.Collector{m.records, m.duration, m.commitErrs, m.fetchErrs, m.assigned, m.rebalances,
		m.dlq.records, m.dlq.duration, m.client}
}

// clientCollector exposes a driver's [ClientStats] at scrape time.
type clientCollector struct {
	role  string
	stats func() ClientStats

	info, brokersUp, requestErrors, queueBytes, queueMax *prometheus.Desc
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
	}
}
