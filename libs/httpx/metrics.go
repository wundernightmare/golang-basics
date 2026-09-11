package httpx

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a private Prometheus registry plus the standard HTTP request
// metrics. Each [Server] gets its own Metrics, so multiple servers in one
// process never fight over the global default registry. Libraries expose a
// prometheus.Collector (pgx pool stats, cache hit/miss, Kafka lag, …) that the
// service registers here, so one /metrics scrape carries everything.
type Metrics struct {
	Registry *prometheus.Registry
	reqTotal *prometheus.CounterVec
	reqDur   *prometheus.HistogramVec
	inFlight prometheus.Gauge
}

// Latency buckets tuned for a service that answers in milliseconds: the
// default Prometheus buckets start at 5ms and would put a whole cached read
// path in the first bucket. Native histograms are enabled alongside, so a
// scraper that speaks them (Prometheus ≥ 2.40, VictoriaMetrics) gets exact
// quantiles at no extra series.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// NewMetrics builds a registry pre-populated with the Go-runtime and process
// collectors, the build identity, and the canonical HTTP metrics:
//
//	build_info{service,version,revision,go_version}   always 1
//	http_requests_total{method,path,status}
//	http_request_duration_seconds{method,path}        histogram (classic + native)
//	http_requests_in_flight
//	log_dropped_total{level}                          records dropped by log sampling
//
// path is the matched route template, never the raw URL.
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

	reqTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests handled, by method, route template and status.",
	}, []string{"method", "path", "status"})

	reqDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "http_request_duration_seconds",
		Help:                            "HTTP request latency in seconds, by method and route template.",
		Buckets:                         latencyBuckets,
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"method", "path"})

	inFlight := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "HTTP requests currently being handled.",
	})

	reg.MustRegister(buildInfo, reqTotal, reqDur, inFlight)

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

	return &Metrics{Registry: reg, reqTotal: reqTotal, reqDur: reqDur, inFlight: inFlight}
}

// Middleware records request count, latency and in-flight gauge for every
// request. It labels by the matched route template (gin's FullPath) rather
// than the raw URL, so high-cardinality path parameters never explode the
// metric series; a request that matched no route is labelled "unmatched".
func (m *Metrics) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		m.inFlight.Inc()
		start := time.Now()
		c.Next()
		m.inFlight.Dec()

		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		method := c.Request.Method

		m.reqTotal.WithLabelValues(method, path, strconv.Itoa(c.Writer.Status())).Inc()
		m.reqDur.WithLabelValues(method, path).Observe(time.Since(start).Seconds())
	}
}

// Handler serves the registry in the Prometheus text exposition format, with
// OpenMetrics negotiation on so native histograms and exemplars can be
// scraped by clients that ask for them.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
