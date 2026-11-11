package pipeline

import "github.com/prometheus/client_golang/prometheus"

// Label values.
const (
	outcomeKafka      = "kafka"
	outcomeArchive    = "archive"
	outcomeNoSink     = "no_sink"
	outcomeOverloaded = "overloaded"
	outcomeCanceled   = "canceled"

	reasonCircuitOpen = "circuit_open"
	reasonBusy        = "busy"
)

type metrics struct {
	requests     *prometheus.CounterVec
	kafkaSkipped *prometheus.CounterVec
	inFlight     prometheus.GaugeFunc
	inFlightMax  prometheus.Gauge
}

func newMetrics(pipe *Pipeline) *metrics {
	set := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_batches_total",
			Help: "Client batches by outcome: kafka / archive (the sink that confirmed first), no_sink (503), overloaded (429), canceled (the client left or the request deadline passed).",
		}, []string{"outcome"}),
		kafkaSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_kafka_skipped_events_total",
			Help: "Events not offered to Kafka, by reason: circuit_open, busy (the send bulkhead was full). They are in the archive only.",
		}, []string{"reason"}),
		inFlight: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "ingest_in_flight_bytes",
			Help: "Event bytes accepted and not yet settled by both sinks.",
		}, func() float64 { return float64(pipe.inFlight.Load()) }),
		inFlightMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ingest_in_flight_max_bytes",
			Help: "The bound of ingest_in_flight_bytes (MAX_IN_FLIGHT_BYTES); past it batches are answered 429.",
		}),
	}
	set.inFlightMax.Set(float64(pipe.cfg.MaxInFlightBytes))

	return set
}

// Collectors returns the pipeline's Prometheus collectors:
//
//	ingest_batches_total{outcome="kafka|archive|no_sink|overloaded|canceled"}
//	ingest_kafka_skipped_events_total{reason="circuit_open|busy"}
//	ingest_in_flight_bytes, ingest_in_flight_max_bytes
//
// The share of outcome="archive" is the share of traffic answered at archive
// latency: near zero while Kafka is healthy, everything while it is away.
func (p *Pipeline) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.metrics.requests, p.metrics.kafkaSkipped, p.metrics.inFlight, p.metrics.inFlightMax}
}
