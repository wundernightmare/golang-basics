package httpx

import (
	"log/slog"
	"net/http"
	"regexp"
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
	inFlight prometheus.Gauge
	// unhandled counts requests answered 500 by the recovery middleware (a
	// handler panic): the 500 a handler returns on purpose is already in
	// http_requests_total, this one is a bug.
	unhandled prometheus.Counter
}

// Latency buckets tuned for a service that answers in milliseconds: the
// default Prometheus buckets start at 5ms and would put a whole cached read
// path in the first bucket. Native histograms are enabled alongside, so a
// scraper that speaks them (Prometheus ≥ 2.40, VictoriaMetrics) gets exact
// quantiles at no extra series.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// schedLatencies selects the scheduler-latency histogram from runtime/metrics
// (and nothing else: MetricsScheduler would also add a goroutine count that
// duplicates go_goroutines).
var schedLatencies = collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile(`^/sched/latencies:seconds$`)}

// NewMetrics builds a registry pre-populated with the Go-runtime and process
// collectors, the build identity, and the canonical HTTP metrics:
//
//	build_info{version,revision}                      always 1
//	http_requests_total{method,route,status}
//	http_request_duration_seconds{method,route}       histogram (classic + native)
//	http_requests_in_flight
//	http_unhandled_errors_total                       handler panics recovered (answered 500)
//	log_dropped_total{level}                          records dropped by log sampling
//
// The names are the workspace's metrics contract, shared with nodejs-basics.
// route is the matched route template ("/tasks/{id}"), never the raw URL
// ("unmatched" when none matched), method is one of the standard methods or
// "_OTHER", and status is the code sent — 499 when the client went away
// first — so no client can grow the series. build_info carries no service
// label (the scrape config attaches it) and no Go version (go_info has it).
// Request and response sizes are not measured: no alert or panel reads them,
// and a histogram per route costs more series than the rest together. When
// the request's span is sampled, the counter and the latency histogram carry
// its trace_id as an exemplar, which is how a dashboard jumps from "p99
// spiked here" to one trace of it.
//
// The Go collector adds one runtime/metrics series to its defaults:
// go_sched_latencies_seconds, the time a runnable goroutine waited for a
// thread — Go's counterpart of Node's event-loop delay, read by the runtime
// dashboard.
func NewMetrics(build BuildInfo, log *slog.Logger) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(schedLatencies)),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "build_info",
		Help: "Build identity of the running binary; always 1.",
		ConstLabels: prometheus.Labels{
			"version": build.Version, "revision": build.Revision,
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
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "HTTP requests currently being handled.",
		}),
		unhandled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "http_unhandled_errors_total",
			Help: "Requests that failed outside the handler's own error handling (a recovered panic, answered 500).",
		}),
	}
	reg.MustRegister(buildInfo, m.reqTotal, m.reqDur, m.inFlight, m.unhandled)

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

// Middleware records request count, latency and the in-flight gauge
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
				status := strconv.Itoa(info.status(r.Context()))
				elapsed := time.Since(start).Seconds()
				if ex := exemplar(r); ex != nil {
					m.reqTotal.WithLabelValues(method, route, status).(prometheus.ExemplarAdder).AddWithExemplar(1, ex)
					m.reqDur.WithLabelValues(method, route).(prometheus.ExemplarObserver).ObserveWithExemplar(elapsed, ex)
				} else {
					m.reqTotal.WithLabelValues(method, route, status).Inc()
					m.reqDur.WithLabelValues(method, route).Observe(elapsed)
				}
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
