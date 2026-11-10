package grpcx

import (
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
)

// outcomeCircuitOpen is the outcome label of a call the breaker rejected.
const outcomeCircuitOpen = "circuit_open"

// metrics are the client's Prometheus instruments:
//
//	grpc_client_requests_total{target,method,outcome}      one per call (not per attempt)
//	grpc_client_request_duration_seconds{target,method}    calls that reached gRPC, until they end
//	grpc_client_retries_total{target}                      attempts after a call's first
//	grpc_client_circuit_breaker_state{target}              0 closed, 1 open, 2 half-open
//	grpc_client_circuit_breaker_transitions_total{target,from,to}
//
// outcome is the status code in lower case (ok, unavailable,
// deadline_exceeded, …) or circuit_open; method is the full method name
// (/pkg.Service/Method), which comes from generated code. The names are the
// workspace's metrics contract (docs/METRICS.md).
type metrics struct {
	requests     *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	retries      *prometheus.CounterVec
	breakerState *prometheus.GaugeVec
	transitions  *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	instruments := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_client_requests_total",
			Help: "Outbound gRPC calls by target, method and outcome.",
		}, []string{"target", "method", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_client_request_duration_seconds",
			Help:    "Duration of outbound gRPC calls that reached gRPC, retries included, until the call or stream ended.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"target", "method"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_client_retries_total",
			Help: "Attempts sent after a call's first (gRPC retries, transparent ones included).",
		}, []string{"target"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "grpc_client_circuit_breaker_state",
			Help: "Circuit breaker state: 0 closed, 1 open, 2 half-open.",
		}, []string{"target"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_client_circuit_breaker_transitions_total",
			Help: "Circuit breaker state transitions.",
		}, []string{"target", "from", "to"}),
	}
	for _, c := range []prometheus.Collector{
		instruments.requests, instruments.duration, instruments.retries, instruments.breakerState, instruments.transitions,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err //nolint:wrapcheck // wrapped by New
		}
	}

	return instruments, nil
}

// codeOutcomes are the outcome labels of the status codes gRPC defines: the
// canonical names (the ones retry_codes takes), in lower case — but for
// CANCELLED, spelt "canceled" like the HTTP client's outcome, so one filter
// matches both.
var codeOutcomes = [...]string{ //nolint:gochecknoglobals // read-only lookup table
	codes.OK:                 "ok",
	codes.Canceled:           "canceled",
	codes.Unknown:            "unknown",
	codes.InvalidArgument:    "invalid_argument",
	codes.DeadlineExceeded:   "deadline_exceeded",
	codes.NotFound:           "not_found",
	codes.AlreadyExists:      "already_exists",
	codes.PermissionDenied:   "permission_denied",
	codes.ResourceExhausted:  "resource_exhausted",
	codes.FailedPrecondition: "failed_precondition",
	codes.Aborted:            "aborted",
	codes.OutOfRange:         "out_of_range",
	codes.Unimplemented:      "unimplemented",
	codes.Internal:           "internal",
	codes.Unavailable:        "unavailable",
	codes.DataLoss:           "data_loss",
	codes.Unauthenticated:    "unauthenticated",
}

// codeOutcome is the outcome label of a status code. A code gRPC does not
// define is "unknown": the label set stays bounded whatever a server sends.
func codeOutcome(c codes.Code) string {
	if int(c) >= len(codeOutcomes) {
		return "unknown"
	}

	return codeOutcomes[c]
}
