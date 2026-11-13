package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// api is the one target every test client declares.
const api = "api"

// Full names of the methods the suite calls.
const (
	exportMethod     = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	findTracesMethod = "/jaeger.api_v3.QueryService/FindTraces"
)

// policy is DefaultTarget(api, addr) with short retry delays, then mut. TLS
// stays on unless mut says otherwise: that is the library's default.
func policy(addr string, mut ...func(*grpcx.TargetConfig)) grpcx.TargetConfig {
	tc := grpcx.DefaultTarget(api, addr)
	tc.RetryBaseDelay = 10 * time.Millisecond
	tc.RetryMaxDelay = 50 * time.Millisecond

	for _, m := range mut {
		m(&tc)
	}

	return tc
}

func cleartext(tc *grpcx.TargetConfig)     { tc.Insecure = true }
func noRetries(tc *grpcx.TargetConfig)     { tc.RetryMaxAttempts = 1 }
func touchyBreaker(tc *grpcx.TargetConfig) { tc.MinRequests = 1 }

func timeout(d time.Duration) func(*grpcx.TargetConfig) {
	return func(tc *grpcx.TargetConfig) { tc.Timeout = d }
}

// dial builds a client over tc, closed when the test ends, and its connection.
func dial(tb testing.TB, tc grpcx.TargetConfig, opts ...grpcx.Option) (*grpcx.Client, *grpc.ClientConn) {
	tb.Helper()

	client, err := grpcx.New(grpcx.DefaultConfig(tc), opts...)
	require.NoError(tb, err)
	tb.Cleanup(func() { assert.NoError(tb, client.Close()) })

	conn, err := client.Conn(api)
	require.NoError(tb, err)

	return client, conn
}

// trusting returns TLS credentials over the suite's CA, changed by mut — for
// what the library's default credentials cannot express.
func trusting(mut func(*tls.Config)) grpcx.Option {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certs.caPEM)

	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if mut != nil {
		mut(cfg)
	}

	return grpcx.WithDialOptions(grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
}

func metric(tb testing.TB, client *grpcx.Client, name string, labels map[string]string) float64 {
	tb.Helper()

	return max(testx.Metric(tb, client.Registry(), name, labels), 0) // an absent series is zero
}

// calls is grpc_client_requests_total for an outcome ("ok", "unavailable", …).
func calls(tb testing.TB, client *grpcx.Client, outcome string) float64 {
	tb.Helper()

	return metric(tb, client, "grpc_client_requests_total", map[string]string{"outcome": outcome})
}

func retries(tb testing.TB, client *grpcx.Client) float64 {
	tb.Helper()

	return metric(tb, client, "grpc_client_retries_total", nil)
}

func breaker(tb testing.TB, client *grpcx.Client) resilience.State {
	tb.Helper()

	state, err := client.BreakerState(api)
	require.NoError(tb, err)

	return state
}

// traceID is a fresh random trace id.
func traceID(tb testing.TB) []byte {
	tb.Helper()

	id := make([]byte, 16)
	_, err := rand.Read(id)
	require.NoError(tb, err)

	return id
}

// spans builds an OTLP export of n spans of one trace under service. Each
// span carries pad bytes of attribute, so a batch has a size worth cutting.
func spans(trace []byte, n int, service string, pad int) *coltracepb.ExportTraceServiceRequest {
	text := func(s string) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
	}

	now := uint64(time.Now().UnixNano())

	batch := make([]*tracepb.Span, n)
	for i := range batch {
		id := make([]byte, 8)
		_, _ = rand.Read(id)

		batch[i] = &tracepb.Span{
			TraceId: trace, SpanId: id, Name: "work",
			StartTimeUnixNano: now, EndTimeUnixNano: now + 1000,
			Attributes: []*commonpb.KeyValue{{Key: "pad", Value: text(strings.Repeat("x", pad))}},
		}
	}

	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{Key: "service.name", Value: text(service)}}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: batch}},
	}}}
}

// export sends n small spans of a fresh trace under service.
func export(ctx context.Context, tb testing.TB, conn *grpc.ClientConn, n int, service string) error {
	tb.Helper()

	_, err := coltracepb.NewTraceServiceClient(conn).Export(ctx, spans(traceID(tb), n, service, 16))

	return err //nolint:wrapcheck // the status under test
}

var arrivedLine = regexp.MustCompile(`(?m)^otelcol_receiver_accepted_spans\{receiver="otlp/counted"[^}]*\} (\d+)$`)

// arrived is the server's own count of the spans that came in through the
// "counted" listener, read from its metrics endpoint: how many times a
// request arrived, whatever the store then made of it. Only the test that
// owns that listener may rely on it.
func arrived(tb testing.TB) int {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+jaeger(tb).Metrics+"/metrics", http.NoBody)
	require.NoError(tb, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(tb, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(tb, err)

	m := arrivedLine.FindSubmatch(body)
	if m == nil {
		return 0 // the series appears with the first span
	}

	n, err := strconv.Atoi(string(m[1]))
	require.NoError(tb, err)

	return n
}

// rawCodec passes message bytes through untouched. The suite calls Jaeger's
// query API without its generated code: a request is a few protobuf fields
// written by hand, an answer is an OTLP TracesData the suite does have the
// type for.
type rawCodec struct{}

var (
	_ encoding.CodecV2 = rawCodec{}

	errNotBytes = errors.New("rawCodec carries *[]byte only")
)

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errNotBytes, v)
	}

	return mem.BufferSlice{mem.SliceBuffer(*b)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("%w: got %T", errNotBytes, v)
	}

	*b = data.Materialize()

	return nil
}

// findTraces opens jaeger.api_v3.QueryService/FindTraces for every trace of
// service: a server-streaming call, one message per trace.
//
//	FindTracesRequest{ query = 1: TraceQueryParameters{
//	    service_name = 1, start_time_min = 4, start_time_max = 5, search_depth = 8 } }
func findTraces(ctx context.Context, tb testing.TB, conn *grpc.ClientConn, service string) (grpc.ClientStream, error) {
	tb.Helper()

	timestamp := func(at time.Time) []byte {
		return protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), uint64(at.Unix()))
	}

	var query []byte

	query = protowire.AppendString(protowire.AppendTag(query, 1, protowire.BytesType), service)
	query = protowire.AppendBytes(protowire.AppendTag(query, 4, protowire.BytesType), timestamp(time.Now().Add(-time.Hour)))
	query = protowire.AppendBytes(protowire.AppendTag(query, 5, protowire.BytesType), timestamp(time.Now().Add(time.Hour)))
	query = protowire.AppendVarint(protowire.AppendTag(query, 8, protowire.VarintType), 10_000)
	request := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), query)

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, findTracesMethod, grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		return nil, err //nolint:wrapcheck // the status under test
	}

	if err := stream.SendMsg(&request); err != nil {
		return nil, err //nolint:wrapcheck // the status under test
	}

	return stream, stream.CloseSend() //nolint:wrapcheck // the status under test
}

// next reads one message of a FindTraces stream and counts its spans.
func next(stream grpc.ClientStream) (int, error) {
	var frame []byte
	if err := stream.RecvMsg(&frame); err != nil {
		return 0, err //nolint:wrapcheck // io.EOF or the status under test
	}

	var data tracepb.TracesData
	if err := proto.Unmarshal(frame, &data); err != nil {
		return 0, fmt.Errorf("a FindTraces message is not OTLP TracesData: %w", err)
	}

	count := 0

	for _, resource := range data.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			count += len(scope.GetSpans())
		}
	}

	return count, nil
}

// seed stores traces of spansEach fat spans under a service of its own and
// returns the service name: something for a stream to carry.
func seed(tb testing.TB, traces, spansEach int) string {
	tb.Helper()

	_, conn := dial(tb, policy(jaeger(tb).Plain, cleartext))
	service := testx.Unique("svc")

	for range traces {
		_, err := coltracepb.NewTraceServiceClient(conn).Export(tb.Context(), spans(traceID(tb), spansEach, service, 200))
		require.NoError(tb, err)
	}

	return service
}

// stored is how many spans the server holds for service, read through its
// query API on a direct connection: the ground truth of what the server kept.
// Jaeger folds spans that are identical to the last byte, so the very same
// batch delivered twice is still one batch here — see [arrived] for arrivals.
func stored(tb testing.TB, service string) int {
	tb.Helper()

	_, conn := dial(tb, policy(jaeger(tb).Query, cleartext, timeout(20*time.Second)))

	stream, err := findTraces(context.Background(), tb, conn, service)
	require.NoError(tb, err)

	total := 0

	for {
		n, err := next(stream)
		if errors.Is(err, io.EOF) {
			return total
		}

		require.NoError(tb, err)

		total += n
	}
}
