package otelx

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/wundernightmare/golang-basics/libs/otelx/retro"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

// The environment is read through a prefix no deployment uses.
const envPrefix = "OTELXTEST_"

// restoreGlobals puts back the OpenTelemetry globals Init replaces.
func restoreGlobals(t *testing.T) {
	t.Helper()

	tp, prop, eh := otel.GetTracerProvider(), otel.GetTextMapPropagator(), otel.GetErrorHandler()

	t.Cleanup(func() {
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(prop)
		otel.SetErrorHandler(eh)
		otel.SetLogger(logr.Discard())
	})
}

func TestLoadConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig(envPrefix)
	require.NoError(t, err)
	assert.False(t, cfg.Enabled, "export is opt-in")
	assert.Equal(t, "localhost:4318", cfg.Endpoint, "OTLP over HTTP")
	assert.False(t, cfg.Insecure, "TLS by default")
	assert.Empty(t, cfg.Headers)
	assert.Equal(t, 10*time.Second, cfg.ExportTimeout)
	assert.InDelta(t, 1.0, cfg.SamplerRatio, 1e-9)
	assert.False(t, cfg.RedrawRemoteParent)
	assert.Equal(t, 5*time.Second, cfg.BatchTimeout)
	assert.Equal(t, 2048, cfg.MaxQueueSize)
	assert.Equal(t, 512, cfg.MaxExportBatchSize)
}

// The process-environment path of LoadConfig (LoadConfigFrom with nil);
// everything else reads a map through LoadConfigFrom.
//
// Serial on purpose: exercises the process-environment path (t.Setenv cannot run in parallel).
func TestLoadConfig_ReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv(envPrefix+"OTEL_SERVICE_NAME", "from-env")

	cfg, err := LoadConfig(envPrefix)
	require.NoError(t, err)
	assert.Equal(t, "from-env", cfg.ServiceName, "LoadConfig reads the process environment")

	t.Setenv(envPrefix+"OTEL_TRACES_TRUST_REMOTE_PARENT", "false")

	_, err = LoadConfig(envPrefix)
	require.ErrorContains(t, err, "is no longer read", "the retired key is looked up in the process environment too")
}

func TestLoadConfig_FromEnv(t *testing.T) {
	t.Parallel()

	environ := map[string]string{}
	for k, v := range map[string]string{
		"OTEL_ENABLED":                     "true",
		"OTEL_SERVICE_NAME":                "tasks",
		"OTEL_SERVICE_VERSION":             "1.2.3",
		"OTEL_DEPLOYMENT_ENVIRONMENT":      "staging",
		"OTEL_EXPORTER_OTLP_ENDPOINT":      "collector:4318",
		"OTEL_EXPORTER_OTLP_INSECURE":      "true",
		"OTEL_EXPORTER_OTLP_HEADERS":       "authorization=Bearer x,x-tenant=acme",
		"OTEL_TRACES_SAMPLER_RATIO":        "0.25",
		"OTEL_TRACES_REDRAW_REMOTE_PARENT": "true",
		"OTEL_BSP_MAX_QUEUE_SIZE":          "100",
	} {
		environ[envPrefix+k] = v
	}

	cfg, err := LoadConfigFrom(envPrefix, environ)
	require.NoError(t, err)
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "tasks", cfg.ServiceName)
	assert.Equal(t, "1.2.3", cfg.Version)
	assert.Equal(t, "staging", cfg.Environment)
	assert.Equal(t, "collector:4318", cfg.Endpoint)
	assert.True(t, cfg.Insecure)
	assert.Equal(t, map[string]string{"authorization": "Bearer x", "x-tenant": "acme"}, cfg.Headers)
	assert.InDelta(t, 0.25, cfg.SamplerRatio, 1e-9)
	assert.True(t, cfg.RedrawRemoteParent)
	assert.Equal(t, 100, cfg.MaxQueueSize)

	environ[envPrefix+"OTEL_BSP_MAX_QUEUE_SIZE"] = "-7"
	_, err = LoadConfigFrom(envPrefix, environ)
	require.ErrorContains(t, err, "OTEL_BSP_MAX_QUEUE_SIZE must not be negative", "a loaded config is a validated one")

	environ[envPrefix+"OTEL_TRACES_SAMPLER_RATIO"] = "most"
	_, err = LoadConfigFrom(envPrefix, environ)
	require.ErrorContains(t, err, "otelx: parse config")
}

// The retired key meant the opposite of the one that replaced it, so a
// deployment still setting it must not start as if it had said nothing.
func TestLoadConfig_RefusesTheRetiredTrustRemoteParentKey(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"false", "true", ""} {
		_, err := LoadConfigFrom(envPrefix, map[string]string{envPrefix + "OTEL_TRACES_TRUST_REMOTE_PARENT": value})
		require.ErrorContains(t, err, envPrefix+"OTEL_TRACES_TRUST_REMOTE_PARENT", "value %q", value)
		require.ErrorContains(t, err, envPrefix+"OTEL_TRACES_REDRAW_REMOTE_PARENT", "the error names the key to set instead")
	}

	_, err := LoadConfigFrom(envPrefix, map[string]string{"OTHER_OTEL_TRACES_TRUST_REMOTE_PARENT": "false"})
	require.NoError(t, err, "another service's key is not this one's")
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]Config{
		"zero":                 {},
		"host and port":        {Endpoint: "collector:4318"},
		"host alone":           {Endpoint: "collector"},
		"IPv6":                 {Endpoint: "[::1]:4318"},
		"never sample":         {SamplerRatio: -1},
		"buffer off":           {Retro: retro.Config{BufferMiB: -1}},
		"TLS files unread off": {CACert: "absent.pem", ClientCert: "absent.pem"},
		"plaintext reads none": {Enabled: true, Insecure: true, CACert: "absent.pem"},
	} {
		require.NoError(t, cfg.Validate(), name)
	}

	for name, tc := range map[string]struct {
		cfg  Config
		want string
	}{
		"URL":              {Config{Endpoint: "https://collector:4318"}, "OTEL_EXPORTER_OTLP_ENDPOINT must be host:port"},
		"path":             {Config{Endpoint: "collector:4318/v1/traces"}, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		"credentials":      {Config{Endpoint: "user:pass@collector:4318"}, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		"port":             {Config{Endpoint: "collector:otlp"}, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		"export timeout":   {Config{ExportTimeout: -time.Second}, "OTEL_EXPORTER_OTLP_TIMEOUT must not be negative"},
		"batch timeout":    {Config{BatchTimeout: -time.Second}, "OTEL_BSP_SCHEDULE_DELAY must not be negative"},
		"queue":            {Config{MaxQueueSize: -1}, "OTEL_BSP_MAX_QUEUE_SIZE must not be negative, got -1"},
		"batch":            {Config{MaxExportBatchSize: -1}, "OTEL_BSP_MAX_EXPORT_BATCH_SIZE must not be negative, got -1"},
		"ratio":            {Config{SamplerRatio: math.NaN()}, "OTEL_TRACES_SAMPLER_RATIO"},
		"cert without key": {Config{Enabled: true, ClientCert: "client.pem"}, "must both be set"},
	} {
		err := tc.cfg.Validate()
		require.ErrorContains(t, err, tc.want, name)
		assert.NotContains(t, err.Error(), "pass", name)
	}

	err := Config{MaxQueueSize: -1, MaxExportBatchSize: -1}.Validate()
	require.ErrorContains(t, err, "QUEUE")
	assert.ErrorContains(t, err, "BATCH", "every problem at once")
}

func TestConfig_WithDefaultsFillsAHandBuiltConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{SamplerRatio: -1}
	cfg.withDefaults()
	assert.Equal(t, "localhost:4318", cfg.Endpoint)
	assert.Equal(t, 10*time.Second, cfg.ExportTimeout)
	assert.InDelta(t, -1.0, cfg.SamplerRatio, 1e-9, "a negative ratio (never) is kept")
	assert.Equal(t, 5*time.Second, cfg.BatchTimeout)
	assert.Equal(t, 2048, cfg.MaxQueueSize)
	assert.Equal(t, 512, cfg.MaxExportBatchSize)
}

//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals; this test checks exactly that
func TestInit_DisabledInstallsPropagatorsOnly(t *testing.T) {
	restoreGlobals(t)

	before := otel.GetTracerProvider()
	buf := &testx.LogBuffer{}
	tel, err := Init(context.Background(), Config{Enabled: false, ServiceName: "x"}, slog.New(slog.NewJSONHandler(buf, nil)))
	require.NoError(t, err)
	require.NotNil(t, tel)
	_, span := tel.TracerProvider.Tracer("t").Start(context.Background(), "s")
	assert.False(t, span.IsRecording(), "disabled: the returned provider is a no-op")
	span.End()

	// Propagators are installed even when export is off, so cross-hop context
	// still flows.
	fields := otel.GetTextMapPropagator().Fields()
	assert.Contains(t, fields, "traceparent")
	assert.Contains(t, fields, "baggage")
	assert.Equal(t, before, otel.GetTracerProvider(), "no provider is installed")
	assert.NotNil(t, buf.Find(t, map[string]any{"msg": "tracing disabled (propagation only)"}))
	assert.Equal(t, otel.GetTextMapPropagator(), tel.Propagator, "the returned propagator is the installed one")
	require.NoError(t, tel.Shutdown(context.Background()))

	// Stop is Shutdown for a defer: safe to call, again and after Shutdown.
	tel.Stop()
}

// The flush is bounded whatever the caller passes, and a cancelled context
// does not cancel it.
func TestFlushContext(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	soon, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	want, _ := soon.Deadline()

	for name, tc := range map[string]struct {
		ctx      context.Context //nolint:containedctx // the input of the table
		deadline time.Time
	}{
		"no deadline: the budget":      {context.Background(), time.Now().Add(shutdownBudget)},
		"cancelled: the budget":        {cancelled, time.Now().Add(shutdownBudget)},
		"a deadline: the caller's own": {soon, want},
	} {
		flush, cancel := flushContext(tc.ctx)
		t.Cleanup(cancel)

		got, ok := flush.Deadline()
		require.True(t, ok, name)
		assert.WithinDuration(t, tc.deadline, got, time.Second, name)
		assert.NoError(t, flush.Err(), name)
	}
}

//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals; this test checks exactly that
func TestInit_EnabledInstallsAnSDKProvider(t *testing.T) {
	restoreGlobals(t)

	buf := &testx.LogBuffer{}
	// The exporter connects lazily: nothing listens on this port, and nothing
	// needs to while no span is exported.
	tel, err := Init(context.Background(), Config{
		Enabled: true, ServiceName: "svc", Version: "1.0.0", Endpoint: "127.0.0.1:1", Insecure: true,
		Headers: map[string]string{"x-tenant": "acme"},
	}, slog.New(slog.NewJSONHandler(buf, nil)))
	require.NoError(t, err)

	_, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	assert.True(t, ok, "the SDK provider is the global one")
	assert.Same(t, otel.GetTracerProvider(), tel.TracerProvider, "the returned provider is the installed one")
	line := buf.Find(t, map[string]any{"msg": "tracing enabled"})
	require.NotNil(t, line)
	assert.Equal(t, "127.0.0.1:1", line["endpoint"])
	assert.Equal(t, true, line["insecure"])
	assert.Equal(t, "svc", line["service_name"])
	assert.Equal(t, false, line["redraw_remote_parent"])
	assert.InDelta(t, 1, line["sampler_ratio"], 0, "the ratio in effect, not the zero of the Config")
	assert.InDelta(t, retro.DefaultBufferMiB, line["retro_buffer_mib"], 0, "the buffer allocated, not the zero of the Config")
	assert.NotContains(t, buf.String(), "acme", "headers (credentials) are never logged")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, tel.Shutdown(ctx))
}

//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals; this test checks exactly that
func TestInit_RefusesAnInvalidConfig(t *testing.T) {
	restoreGlobals(t)

	_, err := Init(context.Background(), Config{Enabled: true, CACert: filepath.Join(t.TempDir(), "absent.pem")}, nil)
	require.ErrorContains(t, err, "OTLP CA certificate: open ")

	_, err = Init(context.Background(), Config{Enabled: true, MaxQueueSize: -1}, nil)
	require.ErrorContains(t, err, "OTEL_BSP_MAX_QUEUE_SIZE")

	_, err = Init(context.Background(), Config{Enabled: true, Endpoint: "http://127.0.0.1:4318"}, nil)
	require.ErrorContains(t, err, "OTEL_EXPORTER_OTLP_ENDPOINT")
}

func TestTLSConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certPEM, keyPEM := selfSigned(t)
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, b, 0o600))

		return p
	}
	ca, cert, key := write("ca.pem", certPEM), write("client.pem", certPEM), write("client.key", keyPEM)
	junk := write("junk.pem", []byte("not a certificate"))

	t.Run("system roots by default", func(t *testing.T) {
		t.Parallel()

		tc, err := tlsConfig(Config{})
		require.NoError(t, err)
		assert.Nil(t, tc.RootCAs, "nil means the system pool")
		assert.Empty(t, tc.Certificates)
		assert.Equal(t, uint16(tls.VersionTLS12), tc.MinVersion)
	})
	t.Run("custom CA", func(t *testing.T) {
		t.Parallel()

		tc, err := tlsConfig(Config{CACert: ca})
		require.NoError(t, err)

		block, _ := pem.Decode(certPEM)
		authority, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)

		only := x509.NewCertPool()
		only.AddCert(authority)
		assert.True(t, tc.RootCAs.Equal(only), "the bundle is the trust store: the system roots are not added to it")
	})
	t.Run("mTLS", func(t *testing.T) {
		t.Parallel()

		tc, err := tlsConfig(Config{ClientCert: cert, ClientKey: key})
		require.NoError(t, err)
		assert.Len(t, tc.Certificates, 1)
	})

	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing CA file", Config{CACert: filepath.Join(dir, "absent.pem")}, "OTLP CA certificate: open "},
		{"CA without PEM", Config{CACert: junk}, "holds no PEM certificate"},
		{"cert without key", Config{ClientCert: cert}, "must both be set"},
		{"key without cert", Config{ClientKey: key}, "must both be set"},
		{"mismatched pair", Config{ClientCert: junk, ClientKey: key}, "load OTLP client certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tlsConfig(tc.cfg)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func selfSigned(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "otelx-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	kder, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder})
}

func TestErrorHandler_ThrottlesAndCountsSuppressed(t *testing.T) {
	t.Parallel()

	buf := &testx.LogBuffer{}
	handler := &errorHandler{log: slog.New(slog.NewJSONHandler(buf, nil))}

	handler.Handle(nil) // ignored

	for range 5 {
		handler.Handle(errors.New("export failed: connection refused"))
	}

	lines := buf.Lines(t)
	require.Len(t, lines, 1, "one line per interval, however many batches fail")
	assert.Equal(t, "WARN", lines[0]["level"])
	assert.Equal(t, "opentelemetry: export error", lines[0]["msg"])
	assert.Equal(t, "export failed: connection refused", lines[0]["err"])
	assert.Equal(t, float64(0), lines[0]["suppressed_since_last"])

	// The interval has passed: the next error is logged with the count of
	// what was swallowed since the previous line.
	handler.lastLogged.Store(time.Now().Add(-errorLogInterval - time.Second).UnixNano())
	handler.Handle(errors.New("still failing"))

	lines = buf.Lines(t)
	require.Len(t, lines, 2)
	assert.Equal(t, float64(4), lines[1]["suppressed_since_last"])
}

func TestNewResource_DescribesTheProcess(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "team=core")

	res, err := newResource(context.Background(), Config{ServiceName: "svc", Version: "1.2.3", Environment: "staging"})
	require.NoError(t, err)

	got := map[attribute.Key]string{}
	for _, kv := range res.Attributes() {
		got[kv.Key] = kv.Value.String()
	}

	host, _ := os.Hostname()

	assert.Equal(t, "svc", got[semconv.ServiceNameKey])
	assert.Equal(t, "1.2.3", got[semconv.ServiceVersionKey])
	assert.Equal(t, host, got[semconv.ServiceInstanceIDKey], "replicas are told apart by hostname (the pod name)")
	assert.Equal(t, "staging", got[semconv.DeploymentEnvironmentNameKey])
	assert.Equal(t, "core", got["team"], "OTEL_RESOURCE_ATTRIBUTES is merged in")
	assert.Equal(t, "go", got[semconv.TelemetrySDKLanguageKey])
	assert.NotEmpty(t, got[semconv.HostNameKey])
	assert.NotEmpty(t, got[semconv.ProcessPIDKey])
	assert.NotEmpty(t, got[semconv.ProcessRuntimeNameKey])

	res, err = newResource(context.Background(), Config{ServiceName: "svc"})
	require.NoError(t, err)

	for _, kv := range res.Attributes() {
		assert.NotEqual(t, semconv.DeploymentEnvironmentNameKey, kv.Key, "no environment attribute when unset")
	}
}

func TestNewResource_UnsetNameFallsBackToTheEnvironment(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "from-env")

	res, err := newResource(context.Background(), Config{})
	require.NoError(t, err)

	name, ok := res.Set().Value(semconv.ServiceNameKey)
	require.True(t, ok)
	assert.Equal(t, "from-env", name.AsString(), "an empty ServiceName does not blank the standard variable")

	_, ok = res.Set().Value(semconv.ServiceVersionKey)
	assert.False(t, ok, "no empty service.version")
}

// Init's propagator is the W3C pair: a traceparent survives a round trip.
//
//nolint:paralleltest // serial: otelx.Init installs the OpenTelemetry globals; this test checks exactly that
func TestInit_PropagatesTraceContext(t *testing.T) {
	restoreGlobals(t)

	_, err := Init(context.Background(), Config{}, nil)
	require.NoError(t, err)

	carrier := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", sc.TraceID().String())

	out := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, out)
	assert.Equal(t, carrier["traceparent"], out["traceparent"])
}
