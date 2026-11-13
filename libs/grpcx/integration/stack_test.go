package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// jaegerImage is the suite's real gRPC server: Jaeger v2, an OpenTelemetry
// Collector distribution with a trace store and a query API.
const jaegerImage = "jaegertracing/jaeger:2.21.0"

// Ports inside the Jaeger container.
const (
	otlpPlain   = "4317/tcp"  // OTLP/gRPC, cleartext
	otlpTLS     = "4417/tcp"  // OTLP/gRPC over TLS
	otlpMTLS    = "4517/tcp"  // OTLP/gRPC, client certificate required
	otlpTLS13   = "4617/tcp"  // OTLP/gRPC, TLS 1.3 only
	otlpExpired = "4717/tcp"  // OTLP/gRPC behind an expired certificate
	otlpCounted = "4817/tcp"  // OTLP/gRPC, cleartext, for the one test that reads this listener's counter
	queryGRPC   = "16685/tcp" // the query API (server-streaming), cleartext
	metricsHTTP = "8888/tcp"  // the server's own Prometheus metrics
	healthHTTP  = "13133/tcp"
)

// tracing is where the suite reaches Jaeger, as "host:port".
type tracing struct {
	Plain, TLS, MTLS, TLS13, Expired, Counted, Query, Metrics string
}

const jaegerConfig = `
service:
  extensions: [jaeger_storage, jaeger_query, healthcheckv2]
  pipelines:
    traces:
      receivers: [otlp/plain, otlp/tls, otlp/mtls, otlp/tls13, otlp/expired, otlp/counted]
      exporters: [jaeger_storage_exporter]
  telemetry:
    metrics:
      level: detailed
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
    logs:
      level: warn

extensions:
  jaeger_query:
    storage:
      traces: store
    grpc:
      endpoint: 0.0.0.0:16685
    http:
      endpoint: 0.0.0.0:16686
  jaeger_storage:
    backends:
      store:
        memory:
          max_traces: 100000
  healthcheckv2:
    use_v2: true
    http:
      endpoint: 0.0.0.0:13133

receivers:
  otlp/plain:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
  otlp/tls:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4417
        tls:
          cert_file: /certs/server.crt
          key_file: /certs/server.key
  otlp/mtls:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4517
        tls:
          cert_file: /certs/server.crt
          key_file: /certs/server.key
          client_ca_file: /certs/ca.crt
  otlp/tls13:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4617
        tls:
          cert_file: /certs/server.crt
          key_file: /certs/server.key
          min_version: "1.3"
  otlp/counted:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4817
  otlp/expired:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4717
        tls:
          cert_file: /certs/expired.crt
          key_file: /certs/expired.key

exporters:
  jaeger_storage_exporter:
    trace_storage: store
`

// jaeger starts the suite's Jaeger.
func jaeger(tb testing.TB) tracing {
	tb.Helper()

	return containers.Fixture(tb, "jaeger", func(ctx context.Context) (tracing, testcontainers.Container, error) {
		file := func(path string, content []byte) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader(content), ContainerFilePath: path, FileMode: 0o644}
		}

		ctr, err := testcontainers.Run(ctx, jaegerImage,
			testcontainers.WithExposedPorts(otlpPlain, otlpTLS, otlpMTLS, otlpTLS13, otlpExpired, otlpCounted, queryGRPC, metricsHTTP, healthHTTP),
			testcontainers.WithCmd("--config", "/etc/jaeger/config.yaml"),
			testcontainers.WithFiles(
				file("/etc/jaeger/config.yaml", []byte(jaegerConfig)),
				file("/certs/ca.crt", certs.caPEM),
				file("/certs/server.crt", certs.serverCert),
				file("/certs/server.key", certs.serverKey),
				file("/certs/expired.crt", certs.expiredCert),
				file("/certs/expired.key", certs.expiredKey),
			),
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForHTTP("/status").WithPort(healthHTTP).WithStatusCodeMatcher(func(code int) bool { return code == 200 }),
				wait.ForListeningPort(otlpPlain),
				wait.ForListeningPort(queryGRPC),
			).WithStartupTimeoutDefault(90*time.Second)))
		if err != nil {
			return tracing{}, ctr, err
		}

		at := func(port string) string { return "localhost:" + containers.HostPort(tb, ctr, port) }

		return tracing{
			Plain: at(otlpPlain), TLS: at(otlpTLS), MTLS: at(otlpMTLS), TLS13: at(otlpTLS13), Expired: at(otlpExpired), Counted: at(otlpCounted),
			Query: at(queryGRPC), Metrics: at(metricsHTTP),
		}, ctr, nil
	})
}

// caddyImage is an HTTP server that is not a gRPC server: what a call meets
// when a gateway, a load balancer or a misrouted address answers instead of
// the service.
const caddyImage = "caddy:2.10.2"

const (
	gatewayPlain = "8080/tcp" // h2c
	gatewayTLS   = "8443/tcp" // HTTP/2 over TLS
)

// gatewayConfig answers every method of a "service" named after an HTTP
// outcome with that outcome: /http503.Gateway/Call gets a 503, and so on.
const gatewayConfig = `{
	admin off
	auto_https off
	default_sni localhost
	servers :8080 {
		protocols h1 h2c
	}
}

(outcomes) {
	handle /http401.Gateway/* {
		respond "who are you" 401
	}
	handle /http403.Gateway/* {
		respond "not you" 403
	}
	handle /http404.Gateway/* {
		respond "no such route" 404
	}
	handle /http429.Gateway/* {
		respond "slow down" 429
	}
	handle /http502.Gateway/* {
		respond "bad gateway" 502
	}
	handle /http503.Gateway/* {
		respond "no healthy upstream" 503
	}
	handle /http504.Gateway/* {
		respond "upstream timed out" 504
	}
	handle /html200.Gateway/* {
		header Content-Type text/html
		respond "<html>maintenance</html>" 200
	}
	handle {
		respond "unrouted" 404
	}
}

:8080 {
	import outcomes
}

:8443 {
	tls /certs/server.crt /certs/server.key
	import outcomes
}
`

// gateway is where the suite reaches Caddy, as "host:port".
type gateway struct{ Plain, TLS string }

func httpGateway(tb testing.TB) gateway {
	tb.Helper()

	return containers.Fixture(tb, "gateway", func(ctx context.Context) (gateway, testcontainers.Container, error) {
		file := func(path string, content []byte) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader(content), ContainerFilePath: path, FileMode: 0o644}
		}

		ctr, err := testcontainers.Run(ctx, caddyImage,
			testcontainers.WithExposedPorts(gatewayPlain, gatewayTLS),
			testcontainers.WithFiles(
				file("/etc/caddy/Caddyfile", []byte(gatewayConfig)),
				file("/certs/server.crt", certs.serverCert),
				file("/certs/server.key", certs.serverKey),
			),
			testcontainers.WithWaitStrategy(wait.ForListeningPort(gatewayPlain).WithStartupTimeout(60*time.Second)))
		if err != nil {
			return gateway{}, ctr, err
		}

		return gateway{
			Plain: "localhost:" + containers.HostPort(tb, ctr, gatewayPlain),
			TLS:   "localhost:" + containers.HostPort(tb, ctr, gatewayTLS),
		}, ctr, nil
	})
}
