package httpx

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
)

// Metrics owns a private Prometheus registry plus the standard HTTP request
// metrics. Each [Server] gets its own Metrics, so multiple servers in one
// process never fight over the global default registry. Libraries expose a
// prometheus.Collector (pgx pool stats, cache hit/miss, Kafka lag, …) that the
// service registers here, so one /metrics scrape carries everything.
//
// Metrics are Prometheus-native by design: the workspace pulls (scrapes) its
// metrics and pushes only traces through OpenTelemetry. The OTel instrumentation
// libraries the data libs use (otelpgx, kotel, valkeyotel) emit their metrics to
// the OTel meter, which stays a no-op here — the equivalent signals are the
// libs' own collectors.
type Metrics struct {
	Registry *prometheus.Registry
	reqTotal *prometheus.CounterVec
	reqDur   *prometheus.HistogramVec
	reqSize  *prometheus.HistogramVec
	respSize *prometheus.HistogramVec
	inFlight prometheus.Gauge
	panics   prometheus.Counter
}

// Latency buckets tuned for a service that answers in milliseconds: the
// default Prometheus buckets start at 5ms and would put a whole cached read
// path in the first bucket. Native histograms are enabled alongside, so a
// scraper that speaks them (Prometheus ≥ 2.40, VictoriaMetrics) gets exact
// quantiles at no extra series.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// sizeBuckets cover a JSON API: from a bare status line to a few megabytes.
var sizeBuckets = []float64{256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304}

// NewMetrics builds a registry pre-populated with the Go-runtime and process
// collectors, the build identity, and the canonical HTTP metrics:
//
//	build_info{service,version,revision,go_version}   always 1
//	http_requests_total{method,route,status}
//	http_request_duration_seconds{method,route}       histogram (classic + native)
//	http_request_size_bytes{method,route}             histogram of Content-Length
//	http_response_size_bytes{method,route}            histogram of bytes written
//	http_requests_in_flight
//	http_panics_total                                 handler panics recovered
//	log_dropped_total{level}                          records dropped by log sampling
//
// route is the matched route template ("/tasks/{id}"), never the raw URL, and
// method is one of the standard methods or "_OTHER", so no client can grow the
// series. When the request's span is sampled, the counter and the latency
// histogram carry its trace_id as an exemplar, which is how a dashboard jumps
// from "p99 spiked here" to one trace of it.
func NewMetrics(build BuildInfo, log *slog.Logger) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "build_info",
		Help: "Build identity of the running binary; always 1.",
		ConstLabels: prometheus.Labels{
			"service": build.Service, "version": build.Version,
			"revision": build.Revision, "go_version": build.GoVersion,
		},
	})
	buildInfo.Set(1)

	labels := []string{"method", "route"}
	m := &Metrics{
		Registry: reg,
		reqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests handled, by method, route template and status.",
		}, []string{"method", "route", "status"}),
		reqDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "http_request_duration_seconds",
			Help:                            "HTTP request latency in seconds, by method and route template.",
			Buckets:                         latencyBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, labels),
		reqSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_size_bytes",
			Help:    "HTTP request body size in bytes (Content-Length), by method and route template.",
			Buckets: sizeBuckets,
		}, labels),
		respSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_response_size_bytes",
			Help:    "HTTP response body size in bytes, by method and route template.",
			Buckets: sizeBuckets,
		}, labels),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "HTTP requests currently being handled.",
		}),
		panics: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "http_panics_total",
			Help: "Handler panics recovered by the server (each one answered 500).",
		}),
	}
	reg.MustRegister(buildInfo, m.reqTotal, m.reqDur, m.reqSize, m.respSize, m.inFlight, m.panics)

	if dropped := logDropped(log); dropped != nil {
		reg.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "log_dropped_total", Help: "Log records dropped by sampling.",
				ConstLabels: prometheus.Labels{"level": "debug"},
			}, func() float64 { d, _ := dropped(); return float64(d) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "log_dropped_total", Help: "Log records dropped by sampling.",
				ConstLabels: prometheus.Labels{"level": "info"},
			}, func() float64 { _, i := dropped(); return float64(i) }),
		)
	}
	return m
}

// Middleware records request count, latency, sizes and the in-flight gauge
// for every request, in a defer so a panic that escapes recovery is still
// counted and the gauge never sticks. Outside a [Server] chain (no request
// info on the context) it wraps the writer itself and labels the route
// "unmatched".
func (m *Metrics) Middleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := infoFrom(r.Context())
			if info == nil {
				rw := &responseWriter{ResponseWriter: w}
				info = &reqInfo{rw: rw}
				w = rw
			}
			m.inFlight.Inc()
			start := time.Now()
			defer func() {
				m.inFlight.Dec()
				method, route := methodLabel(r.Method), info.routeLabel()
				status := strconv.Itoa(info.status())
				elapsed := time.Since(start).Seconds()
				if ex := exemplar(r); ex != nil {
					m.reqTotal.WithLabelValues(method, route, status).(prometheus.ExemplarAdder).AddWithExemplar(1, ex)
					m.reqDur.WithLabelValues(method, route).(prometheus.ExemplarObserver).ObserveWithExemplar(elapsed, ex)
				} else {
					m.reqTotal.WithLabelValues(method, route, status).Inc()
					m.reqDur.WithLabelValues(method, route).Observe(elapsed)
				}
				if r.ContentLength > 0 {
					m.reqSize.WithLabelValues(method, route).Observe(float64(r.ContentLength))
				}
				m.respSize.WithLabelValues(method, route).Observe(float64(info.rw.bytes))
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// exemplar returns the trace_id exemplar labels for a sampled request, or nil.
func exemplar(r *http.Request) prometheus.Labels {
	sc := trace.SpanContextFromContext(r.Context())
	if !sc.IsValid() || !sc.IsSampled() {
		return nil
	}
	return prometheus.Labels{"trace_id": sc.TraceID().String()}
}

// Handler serves the registry in the Prometheus text exposition format, with
// OpenMetrics negotiation on so native histograms and exemplars can be
// scraped by clients that ask for them.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
