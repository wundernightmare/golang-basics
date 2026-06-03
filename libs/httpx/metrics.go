package httpx

import (
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
// process never fight over the global default registry.
type Metrics struct {
	Registry *prometheus.Registry
	reqTotal *prometheus.CounterVec
	reqDur   *prometheus.HistogramVec
}

// NewMetrics builds a registry pre-populated with Go-runtime and process
// collectors and the two canonical HTTP metrics:
//
//	http_requests_total{method,path,status}
//	http_request_duration_seconds{method,path,status}
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	reqTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests handled, by method, route and status.",
	}, []string{"method", "path", "status"})

	reqDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency in seconds, by method, route and status.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path", "status"})

	reg.MustRegister(reqTotal, reqDur)

	return &Metrics{Registry: reg, reqTotal: reqTotal, reqDur: reqDur}
}

// Middleware records request count and latency for every request. It labels
// by the matched route template (gin's FullPath) rather than the raw URL, so
// high-cardinality path parameters never explode the metric series.
func (m *Metrics) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		status := strconv.Itoa(c.Writer.Status())
		method := c.Request.Method

		m.reqTotal.WithLabelValues(method, path, status).Inc()
		m.reqDur.WithLabelValues(method, path, status).Observe(time.Since(start).Seconds())
	}
}

// Handler serves the registry in the Prometheus text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
