package resilient

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics are the client's Prometheus instruments:
//
//	http_client_requests_total{target,method,outcome}        one per attempt
//	http_client_request_duration_seconds{target,method}      attempts that reached the network
//	http_client_retries_total{target}
//	http_client_retry_budget_exhausted_total{target}
//	circuit_breaker_state{target}                            0 closed, 1 open, 2 half-open
//	circuit_breaker_transitions_total{target,from,to}
//	http_client_bulkhead_in_flight{target}
//	http_client_bulkhead_rejected_total{target}
//
// outcome is 2xx, 3xx, 4xx, 5xx, timeout, connection, redirect,
// circuit_open, rate_limited, bulkhead_full, shutdown or canceled.
type metrics struct {
	requests        *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	retries         *prometheus.CounterVec
	budgetExhausted *prometheus.CounterVec
	breakerState    *prometheus.GaugeVec
	transitions     *prometheus.CounterVec
	bulkheadInUse   *prometheus.GaugeVec
	bulkheadReject  *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_client_requests_total",
			Help: "Outbound HTTP attempts by target, method and outcome.",
		}, []string{"target", "method", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_client_request_duration_seconds",
			Help:    "Duration of outbound HTTP attempts that reached the network, until response headers.",
			Buckets: prometheus.DefBuckets,
		}, []string{"target", "method"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_client_retries_total",
			Help: "Retries sent (attempts after the first).",
		}, []string{"target"}),
		budgetExhausted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_client_retry_budget_exhausted_total",
			Help: "Retries not sent because the target's retry budget was spent.",
		}, []string{"target"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker state: 0 closed, 1 open, 2 half-open.",
		}, []string{"target"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "circuit_breaker_transitions_total",
			Help: "Circuit breaker state transitions.",
		}, []string{"target", "from", "to"}),
		bulkheadInUse: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_client_bulkhead_in_flight",
			Help: "Requests holding a bulkhead slot.",
		}, []string{"target"}),
		bulkheadReject: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_client_bulkhead_rejected_total",
			Help: "Requests rejected because the bulkhead stayed full.",
		}, []string{"target"}),
	}
	for _, c := range []prometheus.Collector{
		m.requests, m.duration, m.retries, m.budgetExhausted,
		m.breakerState, m.transitions, m.bulkheadInUse, m.bulkheadReject,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// methodLabel bounds the method label to the standard methods.
func methodLabel(m string) string {
	switch m {
	case "":
		return http.MethodGet
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return m
	}
	return "OTHER"
}

// statusOutcome is the outcome label of an answered request.
func statusOutcome(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	}
	return "2xx"
}
