package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// collectorConfig is the file under test: the configuration the repository
// ships for the observability stack, not a copy of it.
const collectorConfig = "../../../docker/observability/otel-collector.yml"

// composeFile is the stack that runs collectorConfig; the suite reads the
// collector image out of it, so the version tested is the version shipped.
const composeFile = "../../../docker/observability.yml"

// collectorImage must be the image docker/observability.yml runs (without the
// registry prefix); TestCollectorImageIsTheShippedOne holds the two together.
const collectorImage = "otel/opentelemetry-collector-contrib:0.162.0"

// jaegerImage is the trace store behind the collector: Jaeger v2, in memory.
// The suite asks its query API what the collector let through.
const jaegerImage = "jaegertracing/jaeger:2.21.0"

// Ports inside the containers.
const (
	collectorOTLP   = "4318/tcp" // OTLP/HTTP — what otelx exports to
	collectorHealth = "13133/tcp"
	jaegerOTLP      = "4317/tcp" // OTLP/gRPC — what the collector exports to
	jaegerQuery     = "16686/tcp"
	jaegerHealth    = "13133/tcp"
)

// decisionWait is OTEL_TAIL_DECISION_WAIT for the suite: the shipped default
// (10s) is sized for real traces, the suite's traces arrive in one batch.
const decisionWait = "1s"

const jaegerConfig = `
service:
  extensions: [jaeger_storage, jaeger_query, healthcheckv2]
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [jaeger_storage_exporter]
  telemetry:
    logs:
      level: warn

extensions:
  jaeger_query:
    storage:
      traces: store
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
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  jaeger_storage_exporter:
    trace_storage: store
`

// store is where the suite reaches Jaeger: the query API's base URL, and the
// host port of its OTLP/gRPC listener for the collectors.
type store struct {
	QueryURL string
	OTLPPort string
}

// jaeger starts the suite's trace store.
func jaeger(tb testing.TB) store {
	tb.Helper()

	return containers.Fixture(tb, "jaeger", func(ctx context.Context) (store, testcontainers.Container, error) {
		ctr, err := testcontainers.Run(ctx, jaegerImage,
			testcontainers.WithExposedPorts(jaegerOTLP, jaegerQuery, jaegerHealth),
			testcontainers.WithCmd("--config", "/etc/jaeger/config.yaml"),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				Reader: bytes.NewReader([]byte(jaegerConfig)), ContainerFilePath: "/etc/jaeger/config.yaml", FileMode: 0o644,
			}),
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForHTTP("/status").WithPort(jaegerHealth).WithStatusCodeMatcher(func(code int) bool { return code == 200 }),
				wait.ForListeningPort(jaegerOTLP),
				wait.ForListeningPort(jaegerQuery),
			).WithStartupTimeoutDefault(90*time.Second)))
		if err != nil {
			return store{}, ctr, err
		}

		return store{
			QueryURL: "http://localhost:" + containers.HostPort(tb, ctr, jaegerQuery),
			OTLPPort: containers.HostPort(tb, ctr, jaegerOTLP),
		}, ctr, nil
	})
}

// rejected remembers, per collector fixture, that the collector refused the
// shipped configuration, so that every test asking for that collector fails
// — not only the first one.
var rejected sync.Map // fixture name → string

// collector starts an OpenTelemetry Collector with the shipped configuration
// and returns the "host:port" of its OTLP/HTTP receiver. baselinePercent is
// OTEL_TAIL_SAMPLING_PERCENTAGE: the share of otherwise uninteresting traces
// the collector keeps. Each share is a collector of its own, started once.
//
// The collector exports to the suite's Jaeger through the host
// (host.docker.internal), so no container network is needed.
//
// A collector that exits instead of becoming healthy has refused the
// configuration. That is a failure of the file under test, never a reason to
// skip: the test fails with the collector's own log.
func collector(tb testing.TB, baselinePercent string) string {
	tb.Helper()

	backend := jaeger(tb)
	name := "collector-baseline-" + baselinePercent

	endpoint := containers.Fixture(tb, name, func(ctx context.Context) (string, testcontainers.Container, error) {
		ctr, err := testcontainers.Run(ctx, collectorImage,
			testcontainers.WithExposedPorts(collectorOTLP, collectorHealth),
			testcontainers.WithCmd("--config=/etc/otelcol-contrib/config.yaml"),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath: collectorConfig, ContainerFilePath: "/etc/otelcol-contrib/config.yaml", FileMode: 0o644,
			}),
			testcontainers.WithEnv(map[string]string{
				"OTEL_TAIL_DECISION_WAIT":        decisionWait,
				"OTEL_TAIL_SAMPLING_PERCENTAGE":  baselinePercent,
				"OTEL_COLLECTOR_TRACES_ENDPOINT": "host.docker.internal:" + backend.OTLPPort,
			}),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				// Linux needs the alias; Docker Desktop / OrbStack resolve it anyway.
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			}),
			// The image has no shell: the port check is host-side only.
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForHTTP("/").WithPort(collectorHealth).WithStatusCodeMatcher(func(code int) bool { return code == 200 }),
				wait.ForListeningPort(collectorOTLP).SkipInternalCheck(),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			if reason, exited := exitReason(ctx, ctr); exited {
				_ = testcontainers.TerminateContainer(ctr)

				rejected.Store(name, reason)
				tb.Fatalf("the collector did not accept %s:\n%s", collectorConfig, reason)
			}

			return "", ctr, err
		}

		return "localhost:" + containers.HostPort(tb, ctr, collectorOTLP), ctr, nil
	})

	if reason, ok := rejected.Load(name); ok {
		tb.Fatalf("the collector did not accept %s:\n%s", collectorConfig, reason)
	}

	return endpoint
}

// exitReason reports whether a container that failed to start was created
// and has exited by itself — the collector's answer to a configuration it
// cannot load — and returns its exit code and log. A container that was never
// created, or is still running, is the container runtime's problem instead.
func exitReason(ctx context.Context, ctr testcontainers.Container) (string, bool) {
	if ctr == nil {
		return "", false
	}

	state, err := ctr.State(ctx)
	if err != nil || state == nil || state.Running {
		return "", false
	}

	var log []byte

	if logs, err := ctr.Logs(ctx); err == nil {
		log, _ = io.ReadAll(logs)
		_ = logs.Close()
	}

	return fmt.Sprintf("exit code %d\n%s", state.ExitCode, log), true
}
