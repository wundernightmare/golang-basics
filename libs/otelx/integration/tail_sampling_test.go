package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/otelx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// Durations of the suite's spans, set as timestamps (nothing sleeps). The
// shipped slow threshold is 500ms and the suite does not override it.
const (
	fast = 5 * time.Millisecond
	slow = 800 * time.Millisecond
)

// service is one otelx.Init: what a service's main does, pointed at the
// suite's collector. Every span ends before close, which flushes them.
type service struct {
	t      *testing.T
	tracer trace.Tracer
	close  func()
}

// newService builds the SDK through otelx.Init — the exporter, the batcher
// and the head sampler a real service gets — under its own service name.
func newService(t *testing.T, endpoint, name string, headRatio float64) *service {
	t.Helper()

	tel, err := otelx.Init(context.Background(), otelx.Config{
		Enabled:      true,
		ServiceName:  name,
		Endpoint:     endpoint,
		Insecure:     true, // the suite's collector is plaintext, as the local stack's is
		SamplerRatio: headRatio,
		BatchTimeout: 100 * time.Millisecond,
	}, nil)
	require.NoError(t, err)

	closed := false
	closeOnce := func() {
		if closed {
			return
		}

		closed = true

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		require.NoError(t, tel.Shutdown(ctx), "flush to the collector")
	}
	t.Cleanup(closeOnce)

	return &service{t: t, tracer: tel.TracerProvider.Tracer("otelx/integration"), close: closeOnce}
}

// root emits a one-span trace of the given duration and returns its trace id.
func (s *service) root(name string, d time.Duration, attrs ...attribute.KeyValue) string {
	s.t.Helper()

	start := time.Now().Add(-2 * time.Second)
	_, span := s.tracer.Start(context.Background(), name, trace.WithTimestamp(start), trace.WithAttributes(attrs...))
	span.End(trace.WithTimestamp(start.Add(d)))

	return span.SpanContext().TraceID().String()
}

// failed emits a fast two-span trace whose root is fine and whose child has
// status ERROR: the shape of a request that failed in a dependency call.
func (s *service) failed() string {
	s.t.Helper()

	start := time.Now().Add(-2 * time.Second)
	ctx, root := s.tracer.Start(context.Background(), "GET /failed", trace.WithTimestamp(start))
	_, child := s.tracer.Start(ctx, "dependency", trace.WithTimestamp(start.Add(time.Millisecond)))
	child.SetStatus(codes.Error, "boom")
	child.End(trace.WithTimestamp(start.Add(fast - time.Millisecond)))
	root.End(trace.WithTimestamp(start.Add(fast)))

	return root.SpanContext().TraceID().String()
}

func (s *service) times(n int, emit func() string) []string {
	ids := make([]string, 0, n)
	for range n {
		ids = append(ids, emit())
	}

	return ids
}

// stored asks Jaeger which traces it holds for a service: trace id → number
// of spans. This is the suite's ground truth — what came out of the collector.
//
// The question goes to Jaeger's /api/v3/traces, which answers with a stream
// of OTLP/JSON chunks (trace ids in hex), and with 404 when it holds nothing.
func stored(ctx context.Context, backend store, serviceName string) (map[string]int, error) {
	now := time.Now().UTC()
	query := url.Values{
		"query.service_name":   {serviceName},
		"query.start_time_min": {now.Add(-time.Hour).Format(time.RFC3339)},
		"query.start_time_max": {now.Add(time.Minute).Format(time.RFC3339)},
		"query.search_depth":   {"10000"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.QueryURL+"/api/v3/traces?"+query.Encode(), http.NoBody)
	if err != nil {
		return nil, err
	}

	// One connection per question: nothing is left behind for the leak check.
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	out := map[string]int{}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound: // "No traces found"
		return out, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))

		return nil, fmt.Errorf("jaeger: GET /api/v3/traces: %s: %s", resp.Status, body)
	}

	for dec := json.NewDecoder(resp.Body); ; {
		var chunk struct {
			Result struct {
				ResourceSpans []struct {
					ScopeSpans []struct {
						Spans []struct {
							TraceID string `json:"traceId"`
						} `json:"spans"`
					} `json:"scopeSpans"`
				} `json:"resourceSpans"`
			} `json:"result"`
		}

		if err := dec.Decode(&chunk); errors.Is(err, io.EOF) {
			return out, nil
		} else if err != nil {
			return nil, err
		}

		for _, rs := range chunk.Result.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					out[span.TraceID]++
				}
			}
		}
	}
}

// settled waits until Jaeger holds every trace in want for the service, and
// returns everything it holds then.
//
// It is how the suite proves an absence without sleeping: the caller puts in
// want a trace sent *after* the ones that must be missing — a trace the
// collector certainly keeps. Once that one is stored, the collector has
// decided on everything that arrived before it.
func settled(t *testing.T, backend store, serviceName string, want []string) map[string]int {
	t.Helper()

	var got map[string]int

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var err error

		got, err = stored(ctx, backend, serviceName)
		if !assert.NoError(c, err) {
			return
		}

		assert.Subset(c, slices.Collect(maps.Keys(got)), want)
	}, 60*time.Second, 200*time.Millisecond, "the collector did not forward the traces it must keep")

	return got
}

// The image the suite runs is the image the stack runs: a collector upgrade
// in docker/observability.yml that the suite does not follow would leave the
// shipped pair — image and configuration — untested.
func TestCollectorImageIsTheShippedOne(t *testing.T) {
	t.Parallel()

	compose, err := os.ReadFile(composeFile)
	require.NoError(t, err)
	assert.Contains(t, string(compose), "image: ${DOCKER_HUB:-docker.io}/"+collectorImage+"\n")
}

// The policies of the shipped configuration, with the baseline share at 0 so
// that the verdict on a plain trace is not a matter of chance: a trace with
// an error, a slow trace and a trace marked debug.subject are all kept, whole;
// a fast successful trace is dropped.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestTailSamplingKeepsErrorsSlowAndDebugTraces(t *testing.T) {
	backend := jaeger(t)
	endpoint := collector(t, "0")
	name := testx.Unique("tail")

	svc := newService(t, endpoint, name, 1.0)

	// The traces to drop go first, so that everything sent after them is
	// decided no earlier.
	plain := svc.times(40, func() string { return svc.root("GET /fast", fast) })
	failed := svc.times(5, svc.failed)
	slowOnes := svc.times(5, func() string { return svc.root("GET /slow", slow) })
	marked := append(
		svc.times(4, func() string { return svc.root("GET /fast", fast, attribute.String("debug.subject", "tenant-42")) }),
		// Presence is the rule, not the value: an empty string and a number count.
		svc.root("GET /fast", fast, attribute.String("debug.subject", "")),
		svc.root("GET /fast", fast, attribute.Int("debug.subject", 42)),
	)
	// Sent last, certainly kept: when it is stored, the verdicts above are in.
	sentinel := svc.failed()
	svc.close()

	keep := slices.Concat(failed, slowOnes, marked, []string{sentinel})
	got := settled(t, backend, name, keep)

	for _, id := range failed {
		assert.Equal(t, 2, got[id], "an error in a child span keeps the whole trace, root included (trace %s)", id)
	}

	for _, id := range slices.Concat(slowOnes, marked) {
		assert.Equal(t, 1, got[id], "trace %s", id)
	}

	for _, id := range plain {
		assert.NotContains(t, got, id, "a fast successful trace is dropped when the baseline share is 0")
	}

	assert.Len(t, got, len(keep), "nothing but the traces the policies keep")
}

// The baseline share is a knob that works: at 100 every plain trace comes
// through the same configuration. (At 0, above, none does — together the two
// show OTEL_TAIL_SAMPLING_PERCENTAGE reaches the policy.)
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestTailSamplingBaselineShareIsConfigurable(t *testing.T) {
	backend := jaeger(t)
	endpoint := collector(t, "100")
	name := testx.Unique("baseline")

	svc := newService(t, endpoint, name, 1.0)
	plain := svc.times(20, func() string { return svc.root("GET /fast", fast) })
	svc.close()

	got := settled(t, backend, name, plain)
	assert.Len(t, got, len(plain))
}

// The rule the documentation states: tail sampling sees only what the service
// sent. With head sampling off in the service, traces with an error — which
// the collector keeps at 100% — never reach it, so nothing can keep them.
//
// The ratio is negative, otelx's "never sample": the deterministic end of
// "a low ratio". (A ratio of exactly 0 in otelx.Config means "not set" and
// becomes 1.0.)
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestHeadSamplingDecidesWhatTailSamplingSees(t *testing.T) {
	backend := jaeger(t)
	endpoint := collector(t, "0")
	name := testx.Unique("head")

	unsampled := newService(t, endpoint, name, -1)
	lost := unsampled.times(10, unsampled.failed)
	unsampled.close()

	// The same service name, the same kind of trace, head sampling at 1.0 and
	// sent afterwards: it arrives, so the collector and the path to it work.
	sampled := newService(t, endpoint, name, 1.0)
	sentinel := sampled.failed()
	sampled.close()

	got := settled(t, backend, name, []string{sentinel})

	for _, id := range lost {
		assert.NotContains(t, got, id, "an error trace the SDK did not sample cannot be kept by the collector")
	}

	assert.Len(t, got, 1)
}
