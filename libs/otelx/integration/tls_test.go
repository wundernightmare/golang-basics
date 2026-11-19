package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/wundernightmare/golang-basics/libs/otelx"
	"github.com/wundernightmare/golang-basics/libs/testx"
	"github.com/wundernightmare/golang-basics/libs/testx/containers"
)

type tlsCollector struct {
	Addr string
	PKI  *containers.PKI
}

func collectorWithMTLS(tb testing.TB) tlsCollector {
	tb.Helper()

	backend := jaeger(tb)

	return containers.Fixture(tb, "collector-mtls", func(ctx context.Context) (tlsCollector, testcontainers.Container, error) {
		pki, err := containers.NewPKI()
		if err != nil {
			return tlsCollector{}, nil, err
		}

		config := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
        tls:
          cert_file: /certs/server.crt
          key_file: /certs/server.key
          client_ca_file: /certs/ca.crt
exporters:
  otlp/jaeger:
    endpoint: host.docker.internal:%s
    tls:
      insecure: true
extensions:
  health_check:
    endpoint: 0.0.0.0:13133
service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/jaeger]
`, backend.OTLPPort)
		file := func(path string, content []byte) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader(content), ContainerFilePath: path, FileMode: 0o644}
		}

		ctr, err := testcontainers.Run(ctx, collectorImage,
			testcontainers.WithExposedPorts(collectorOTLP, collectorHealth),
			testcontainers.WithCmd("--config=/etc/otelcol-contrib/config.yaml"),
			testcontainers.WithFiles(
				file("/etc/otelcol-contrib/config.yaml", []byte(config)),
				file("/certs/ca.crt", pki.CA), file("/certs/server.crt", pki.ServerCert), file("/certs/server.key", pki.ServerKey),
			),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			}),
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForHTTP("/").WithPort(collectorHealth).WithStatusCodeMatcher(func(code int) bool { return code == 200 }),
				wait.ForListeningPort(collectorOTLP).SkipInternalCheck(),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return tlsCollector{}, ctr, err
		}

		addr, err := ctr.PortEndpoint(ctx, collectorOTLP, "")

		return tlsCollector{Addr: addr, PKI: pki}, ctr, err
	})
}

func initTLSService(t *testing.T, endpoint string, pki *containers.PKI, exportTimeout time.Duration) (*otelx.Telemetry, string) {
	t.Helper()

	caFile, certFile, keyFile := pki.ClientFiles(t)
	name := testx.Unique("otel-mtls")
	cfg := otelx.Config{
		Enabled: true, ServiceName: name, Endpoint: endpoint, SamplerRatio: 1,
		CACert: caFile, ClientCert: certFile, ClientKey: keyFile,
		ExportTimeout: exportTimeout, BatchTimeout: 20 * time.Millisecond,
	}
	cfg.Retro.BufferMiB = -1

	tel, err := otelx.Init(context.Background(), cfg, nil)
	require.NoError(t, err)

	return tel, name
}

func TestExporterSendsItsClientCertificateToARealCollector(t *testing.T) {
	t.Parallel()

	srv := collectorWithMTLS(t)
	tel, name := initTLSService(t, srv.Addr, srv.PKI, 2*time.Second)
	_, span := tel.TracerProvider.Tracer("otelx/integration").Start(t.Context(), "mtls")
	id := span.SpanContext().TraceID().String()
	span.End()
	require.NoError(t, tel.Shutdown(context.Background()))

	require.Eventually(t, func() bool {
		traces, err := stored(t.Context(), jaeger(t), name)

		return err == nil && traces[id] == 1
	}, 5*time.Second, 50*time.Millisecond)
}

func TestExporterTimeoutBoundsARealExport(t *testing.T) {
	t.Parallel()

	srv := collectorWithMTLS(t)
	proxy := containers.ProxyTo(t, "collector-mtls-timeout", containers.FixtureContainer(t, "collector-mtls"), collectorOTLP)
	tel, _ := initTLSService(t, proxy.Addr, srv.PKI, 250*time.Millisecond)
	_, span := tel.TracerProvider.Tracer("otelx/integration").Start(t.Context(), "timeout")
	span.End()
	proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0})

	start := time.Now()
	err := tel.Shutdown(context.Background())
	require.ErrorContains(t, err, "processor export timeout")
	assert.Less(t, time.Since(start), 3*time.Second, "the export timeout, not the five-second shutdown budget")
}
