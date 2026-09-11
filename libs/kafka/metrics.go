package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Buckets for per-record handler / per-publish latency: sub-millisecond for a
// local ack up to the seconds a slow downstream or a broker hiccup costs.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// producerMetrics is the instrument set behind [Producer.Collectors].
type producerMetrics struct {
	records  *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newProducerMetrics() *producerMetrics {
	return &producerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_producer_records_total",
			Help: "Records published, by topic and result (ok|error).",
		}, []string{"topic", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafka_producer_publish_duration_seconds",
			Help:    "Time from Publish to broker acknowledgement, by topic.",
			Buckets: latencyBuckets,
		}, []string{"topic"}),
	}
}

// Collectors returns the producer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_producer_records_total{topic,result="ok|error"}
//	kafka_producer_publish_duration_seconds{topic}
func (p *Producer) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.metrics.records, p.metrics.duration}
}

// consumerMetrics is the instrument set behind [Consumer.Collectors].
type consumerMetrics struct {
	records     *prometheus.CounterVec
	handlerErrs *prometheus.CounterVec
	fetchErrs   *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	lag         *prometheus.GaugeVec
}

func newConsumerMetrics() *consumerMetrics {
	return &consumerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_records_total",
			Help: "Records handed to the handler, by topic.",
		}, []string{"topic"}),
		handlerErrs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_handler_errors_total",
			Help: "Records the handler rejected (left uncommitted for redelivery), by topic.",
		}, []string{"topic"}),
		fetchErrs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_fetch_errors_total",
			Help: "Fetch errors reported by the broker, by topic.",
		}, []string{"topic"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafka_consumer_handle_duration_seconds",
			Help:    "Handler latency per record, by topic.",
			Buckets: latencyBuckets,
		}, []string{"topic"}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_group_lag",
			Help: "Records not yet committed by this consumer group, per partition (polled from the broker every KAFKA_LAG_INTERVAL).",
		}, []string{"topic", "partition"}),
	}
}

// Collectors returns the consumer's Prometheus collectors for the service to
// register on its metrics registry:
//
//	kafka_consumer_records_total{topic}
//	kafka_consumer_handler_errors_total{topic}
//	kafka_consumer_fetch_errors_total{topic}
//	kafka_consumer_handle_duration_seconds{topic}
//	kafka_consumer_group_lag{topic,partition}
//
// Lag is the consumer's availability indicator: sum by (topic) of the gauge
// growing while records_total is flat means the worker is stuck, growing while
// records_total climbs means it is under-provisioned.
func (c *Consumer) Collectors() []prometheus.Collector {
	m := c.metrics
	return []prometheus.Collector{m.records, m.handlerErrs, m.fetchErrs, m.duration, m.lag}
}
