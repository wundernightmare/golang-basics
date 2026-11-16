package otelx

// Init down to the wire: the exporter and the batch processor it assembles,
// observed by a collector that is an httptest server.

import (
	"context"
	"encoding/pem"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/wundernightmare/golang-basics/libs/httpx"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

// collector accepts OTLP/HTTP exports and remembers the requests.
type collector struct {
	*httptest.Server

	// held makes every request wait for release.
	held    chan struct{}
	release func()

	mu       sync.Mutex
	requests []*http.Request
}

func newCollector(t *testing.T, start func(http.Handler) *httptest.Server, hold bool) *collector {
	t.Helper()

	col := &collector{held: make(chan struct{})}
	col.release = sync.OnceFunc(func() { close(col.held) })

	if !hold {
		col.release()
	}

	col.Server = start(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		col.mu.Lock()
		col.requests = append(col.requests, r)
		col.mu.Unlock()

		<-col.held
	}))

	t.Cleanup(func() {
		col.release() // Close waits for the requests in flight
		col.Close()
	})

	return col
}

func (col *collector) endpoint() string {
	_, hostPort, _ := strings.Cut(col.URL, "://")

	return hostPort
}

func (col *collector) seen() []*http.Request {
	col.mu.Lock()
	defer col.mu.Unlock()

	return append([]*http.Request(nil), col.requests...)
}

// caFile is the collector's certificate as a CA bundle on disk.
func (col *collector) caFile(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ca.pem")
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: col.Certificate().Raw})
	require.NoError(t, os.WriteFile(path, bundle, 0o600))

	return path
}

// initTracing is Init for an enabled service with the httpx logger; the
// batch processor exports on shutdown or on a full batch, never on a timer.
func initTracing(t *testing.T, cfg Config) (*Telemetry, *slog.Logger, *testx.LogBuffer) {
	t.Helper()
	restoreGlobals(t)

	cfg.Enabled, cfg.ServiceName, cfg.BatchTimeout = true, "svc", time.Hour

	lines := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "svc", Level: "info", Format: "json", Writer: lines})

	tel, err := Init(context.Background(), cfg, log)
	require.NoError(t, err)
	t.Cleanup(tel.Stop)

	return tel, log, lines
}

func endSpans(tel *Telemetry, n int) {
	tracer := tel.TracerProvider.Tracer("t")
	for range n {
		_, span := tracer.Start(context.Background(), "work")
		span.End()
	}
}

// refusing is an address nothing listens on.
func refusing(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, lis.Close())

	return lis.Addr().String()
}

// The promises of README "Export" that are one line of wiring each: the CA
// file is the trust store of the connection, the headers travel, the queue
// and the batch have the configured sizes.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestInit_ExportsOverTLSAsConfigured(t *testing.T) {
	col := newCollector(t, httptest.NewTLSServer, true)

	tel, _, _ := initTracing(t, Config{
		SamplerRatio: 1,
		Endpoint:     col.endpoint(), CACert: col.caFile(t), Headers: map[string]string{"x-tenant": "acme"},
		MaxQueueSize: 4, MaxExportBatchSize: 2,
	})

	// A full batch is exported at once and waits in the collector; of what
	// ends meanwhile the queue takes its size and drops the rest.
	endSpans(tel, 2)
	require.Eventually(t, func() bool { return len(col.seen()) == 1 }, 5*time.Second, 5*time.Millisecond,
		"a batch of the configured size is exported without waiting for the timer")
	endSpans(tel, 50)
	col.release()

	require.NoError(t, tel.Shutdown(context.Background()))

	requests := col.seen()
	require.Len(t, requests, 3, "the batch in flight and the queue of 4 in batches of 2")

	for _, r := range requests {
		assert.Equal(t, "/v1/traces", r.URL.Path)
		assert.Equal(t, "acme", r.Header.Get("X-Tenant"))
	}

	// The same collector without its CA: the system roots do not know it.
	stranger, _, _ := initTracing(t, Config{Endpoint: col.endpoint(), SamplerRatio: 1})
	endSpans(stranger, 1)
	require.ErrorContains(t, stranger.Shutdown(context.Background()), "x509")
	assert.Len(t, col.seen(), 3)
}

// What a deployment sets for another SDK — the standard variables without
// the service's prefix — does not configure this exporter: Config does.
//
//nolint:paralleltest // serial: t.Setenv, and otelx.Init installs the OpenTelemetry globals
func TestInit_TheUnprefixedExporterVariablesAreIgnored(t *testing.T) {
	plain := newCollector(t, httptest.NewServer, false)
	secure := newCollector(t, httptest.NewTLSServer, false)

	for key, value := range map[string]string{
		"ENDPOINT":        "http://elsewhere.invalid:1/ambient",
		"TRACES_ENDPOINT": "http://elsewhere.invalid:1/ambient/traces",
		"INSECURE":        "true",
		"CERTIFICATE":     secure.caFile(t),
		"HEADERS":         "x-ambient=leaked",
		"COMPRESSION":     "gzip",
		"PROTOCOL":        "http/json",
		"TIMEOUT":         "1",
	} {
		t.Setenv("OTEL_EXPORTER_OTLP_"+key, value)
	}

	for name, tc := range map[string]struct {
		col *collector
		cfg Config
	}{
		"TLS":       {secure, Config{Endpoint: secure.endpoint(), CACert: secure.caFile(t), SamplerRatio: 1}},
		"plaintext": {plain, Config{Endpoint: plain.endpoint(), Insecure: true, SamplerRatio: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			tel, _, _ := initTracing(t, tc.cfg)
			endSpans(tel, 1)
			require.NoError(t, tel.Shutdown(context.Background()))

			requests := tc.col.seen()
			require.Len(t, requests, 1)
			assert.Equal(t, "/v1/traces", requests[0].URL.Path)
			assert.Empty(t, requests[0].Header.Get("X-Ambient"))
			assert.Empty(t, requests[0].Header.Get("Content-Encoding"))
			assert.Equal(t, "application/x-protobuf", requests[0].Header.Get("Content-Type"))
		})
	}
}

// The last export has no later line to be counted in: its failure reaches
// the caller and the log even while the export errors are throttled.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestShutdown_ReportsTheSpansItCouldNotExport(t *testing.T) {
	tel, _, lines := initTracing(t, Config{Endpoint: refusing(t), Insecure: true, SamplerRatio: 1})

	otel.Handle(errors.New("a background export failed")) // opens the throttle window, as an outage does
	endSpans(tel, 1)

	require.ErrorContains(t, tel.Shutdown(context.Background()), "connection refused")

	line := lines.Find(t, map[string]any{"msg": "tracing shutdown: spans may have been lost"})
	require.NotNil(t, line)
	assert.Contains(t, line["err"], "connection refused")
}

// A service's context is cancelled by the signal that stops it: the flush
// happens all the same.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestShutdown_FlushesUnderACancelledContext(t *testing.T) {
	col := newCollector(t, httptest.NewServer, false)
	tel, _, _ := initTracing(t, Config{Endpoint: col.endpoint(), Insecure: true, SamplerRatio: 1})

	endSpans(tel, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, tel.Shutdown(ctx))
	assert.Len(t, col.seen(), 1)
}

// trace_id is a link to a trace: after shutdown nothing can be exported, so
// a warn line of an unsampled trace keeps the id that is not a link.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestShutdown_AWarnLineNoLongerClaimsATrace(t *testing.T) {
	tel, log, lines := initTracing(t, Config{Endpoint: refusing(t), Insecure: true})

	ctx, span := tel.TracerProvider.Tracer("t").Start(context.Background(), "work")
	defer span.End()

	tel.Stop()
	log.WarnContext(ctx, "late failure")

	line := lines.Find(t, map[string]any{"msg": "late failure"})
	require.NotNil(t, line)
	assert.NotContains(t, line, "trace_id")
	assert.Contains(t, line, "trace_unsampled_id")
}

// OTEL_TRACES_SAMPLER_RATIO=0 is "sample no root", in the environment as in
// a Config built by hand; the default of 1 belongs to the loader.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals
func TestInit_ARatioOfZeroSamplesNoRoot(t *testing.T) {
	cfg, err := LoadConfigFrom(envPrefix, map[string]string{
		envPrefix + "OTEL_TRACES_SAMPLER_RATIO":   "0",
		envPrefix + "OTEL_EXPORTER_OTLP_ENDPOINT": refusing(t),
		envPrefix + "OTEL_EXPORTER_OTLP_INSECURE": "true",
	})
	require.NoError(t, err)

	tel, _, lines := initTracing(t, cfg)

	_, span := tel.TracerProvider.Tracer("t").Start(context.Background(), "work")
	defer span.End()

	assert.False(t, span.SpanContext().IsSampled())
	assert.InDelta(t, 0, lines.Find(t, map[string]any{"msg": "tracing enabled"})["sampler_ratio"], 0)
}
