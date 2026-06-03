package resilient

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus instruments for one [Client], registered on a
// private [prometheus.Registry] so multiple clients (or tests) never collide on
// the global default registry.
//
// Exposed series (all labelled by outbound_target):
//
//	http_outbound_requests_total{outbound_target,template_url,method,status,error_type}
//	http_outbound_request_duration_seconds{outbound_target,method}
//	circuit_breaker_state{outbound_target}                  0=closed 1=open 2=half_open
//	http_outbound_coalesce_hits_total{outbound_target}
//	http_outbound_fallback_hits_total{outbound_target}
//	http_outbound_retry_attempts_total{outbound_target}
//	http_outbound_adaptive_concurrency_limit{outbound_target}
type Metrics struct {
	registry *prometheus.Registry

	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	cbState         *prometheus.GaugeVec
	coalesceHits    *prometheus.CounterVec
	fallbackHits    *prometheus.CounterVec
	retryAttempts   *prometheus.CounterVec
	adaptiveLimit   *prometheus.GaugeVec
}

// NewMetrics builds and registers the instrument set on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_outbound_requests_total",
			Help: "Total outbound HTTP requests, by target, method, status and error type.",
		}, []string{"outbound_target", "template_url", "method", "status", "error_type"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_outbound_request_duration_seconds",
			Help:    "Outbound HTTP request latency in seconds, by target and method.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"outbound_target", "method"}),
		cbState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=open, 2=half_open).",
		}, []string{"outbound_target"}),
		coalesceHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_outbound_coalesce_hits_total",
			Help: "Requests served from an in-flight coalesced result.",
		}, []string{"outbound_target"}),
		fallbackHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_outbound_fallback_hits_total",
			Help: "Requests served from a stale-cache or static fallback.",
		}, []string{"outbound_target"}),
		retryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_outbound_retry_attempts_total",
			Help: "Retry attempts (excludes the initial attempt).",
		}, []string{"outbound_target"}),
		adaptiveLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_outbound_adaptive_concurrency_limit",
			Help: "Current AIMD adaptive-concurrency limit.",
		}, []string{"outbound_target"}),
	}

	reg.MustRegister(
		m.requestsTotal,
		m.requestDuration,
		m.cbState,
		m.coalesceHits,
		m.fallbackHits,
		m.retryAttempts,
		m.adaptiveLimit,
	)
	return m
}

// Registry returns the private registry backing these metrics, for wiring into
// a /metrics handler or merging into a process-wide gatherer.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the registry in the Prometheus text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// --- recording helpers (called from the send path) ---

func (m *Metrics) recordRequest(target, template, method string, status int, errorType string, elapsed time.Duration) {
	m.requestsTotal.WithLabelValues(target, template, method, strconv.Itoa(status), errorType).Inc()
	m.requestDuration.WithLabelValues(target, method).Observe(elapsed.Seconds())
}

func (m *Metrics) recordCBState(target string, state CBState) {
	m.cbState.WithLabelValues(target).Set(float64(state))
}

func (m *Metrics) recordCoalesceHit(target string) { m.coalesceHits.WithLabelValues(target).Inc() }

func (m *Metrics) recordFallbackHit(target string) { m.fallbackHits.WithLabelValues(target).Inc() }

func (m *Metrics) recordRetryAttempt(target string) { m.retryAttempts.WithLabelValues(target).Inc() }

func (m *Metrics) recordAdaptiveLimit(target string, limit int) {
	m.adaptiveLimit.WithLabelValues(target).Set(float64(limit))
}
