package integration_test

// Peers that are not the gRPC service the call was meant for: an HTTP gateway
// answering in its own words, a database, and a server whose answer is lost.

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// Between a client and its gRPC service there is usually something that
// speaks HTTP: an ingress, a mesh sidecar, a load balancer. When the service
// is away, that is who answers — with an HTTP status and a text body. grpc-go
// maps the status to a code; this pins what grpcx then does with each: which
// are retried, and which the breaker counts.
func TestGateway_HTTPAnswersToAGRPCCall(t *testing.T) {
	t.Parallel()

	edge := httpGateway(t)

	answers := []struct {
		service string
		code    codes.Code
		retried bool
		failure bool
	}{
		{service: "http503", code: codes.Unavailable, retried: true, failure: true},
		{service: "http502", code: codes.Unavailable, retried: true, failure: true},
		{service: "http504", code: codes.Unavailable, retried: true, failure: true},
		{service: "http429", code: codes.Unavailable, retried: true, failure: true},
		{service: "http401", code: codes.Unauthenticated},
		{service: "http403", code: codes.PermissionDenied},
		{service: "http404", code: codes.Unimplemented},
		{service: "html200", code: codes.Unknown, failure: true},
	}

	for _, wire := range []struct {
		name, addr string
		mut        func(*grpcx.TargetConfig)
	}{
		{name: "h2c", addr: edge.Plain, mut: cleartext},
		{name: "HTTP/2 over TLS", addr: edge.TLS, mut: func(*grpcx.TargetConfig) {}},
	} {
		for _, answer := range answers {
			t.Run(wire.name+"/"+answer.service, func(t *testing.T) {
				t.Parallel()

				client, conn := dial(t, policy(wire.addr, wire.mut, touchyBreaker))

				err := conn.Invoke(t.Context(), "/"+answer.service+".Gateway/Call", &emptypb.Empty{}, &emptypb.Empty{})
				require.Equal(t, answer.code, status.Code(err), "%v", err)

				if answer.retried {
					assert.InDelta(t, 2, retries(t, client), 0, "retry_max_attempts 3: two retries")
				} else {
					assert.InDelta(t, 0, retries(t, client), 0)
				}

				want := resilience.StateClosed
				if answer.failure {
					want = resilience.StateOpen
				}

				assert.Equal(t, want, breaker(t, client))
			})
		}
	}
}

// PostgreSQL at the address of a gRPC target. It reads the HTTP/2 preface (or
// the ClientHello) as a startup packet and hangs up. The call fails as
// Unavailable at once and the breaker opens — not a hang, not a panic in the
// framer.
func TestNonGRPCPeer_Postgres(t *testing.T) {
	t.Parallel()

	dsn, err := url.Parse(containers.Postgres(t))
	require.NoError(t, err)

	for name, mut := range map[string]func(*grpcx.TargetConfig){"cleartext": cleartext, "TLS": func(*grpcx.TargetConfig) {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client, conn := dial(t, policy(dsn.Host, mut, touchyBreaker))

			start := time.Now()
			err := export(t.Context(), t, conn, 1, "postgres")
			require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			assert.Less(t, time.Since(start), 3*time.Second)
			assert.Equal(t, resilience.StateOpen, breaker(t, client))

			err = export(t.Context(), t, conn, 1, "postgres")
			require.ErrorIs(t, err, grpcx.ErrCircuitOpen)
		})
	}
}

// The request reaches the server and is stored; the connection dies before
// the answer comes back. What the caller sees, and how many times the request
// reaches the server, depends on the retry policy:
//
//   - no retries: the caller gets Unavailable for a write that happened;
//   - retries on (the default): gRPC sends the request again on a new
//     connection and the caller gets OK — the server received the batch
//     twice. gRPC cannot know that Export is not idempotent. Here it does no
//     harm only because this server folds byte-identical spans: its store
//     still holds one batch. A server that appends would hold two;
//   - retry_methods naming other methods only: this one is not retried.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger and owns the "counted" listener
func TestChaos_LostAcknowledgement(t *testing.T) {
	jaeger(t)

	proxy := containers.ProxyTo(t, "otlp-counted", containers.FixtureContainer(t, "jaeger"), otlpCounted)

	const batch = 11

	type outcome struct {
		err      error
		arrivals int // spans the listener received, over all attempts
		kept     int // spans the store holds
		retries  float64
	}

	// lose sends one batch and kills the connection once the server has
	// stored it and before any answer can come back.
	lose := func(t *testing.T, mut func(*grpcx.TargetConfig)) outcome {
		t.Helper()

		client, conn := dial(t, policy(proxy.Addr, cleartext, timeout(10*time.Second), mut))
		service := testx.Unique("lost-ack")

		// The connection is up before the fault: a connection whose first
		// server frame is lost never becomes ready, and nothing is sent.
		require.NoError(t, export(t.Context(), t, conn, 1, testx.Unique("warm-up")))

		before := arrived(t)

		proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0}) // from here, answers are held back

		done := make(chan error, 1)

		request := spans(traceID(t), batch, service, 16)

		go func() {
			_, err := coltracepb.NewTraceServiceClient(conn).Export(context.Background(), request)
			done <- err
		}()

		require.Eventually(t, func() bool { return stored(t, service) == batch }, 10*time.Second, 50*time.Millisecond,
			"the server has the batch")

		// Close every connection while the answer is still held back — lifting
		// the toxic first would let it through — then restore the path.
		proxy.Down()
		proxy.Reset()

		var result outcome

		select {
		case result.err = <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("the call never returned")
		}

		result.arrivals, result.kept, result.retries = arrived(t)-before, stored(t, service), retries(t, client)

		return result
	}

	t.Run("no retries: an error for a write that happened", func(t *testing.T) {
		got := lose(t, noRetries)
		require.Equal(t, codes.Unavailable, status.Code(got.err), "%v", got.err)
		assert.Equal(t, batch, got.arrivals)
		assert.Equal(t, batch, got.kept, "the caller was told it failed; the server has it")
	})

	t.Run("default retries: OK, and the server received it twice", func(t *testing.T) {
		got := lose(t, func(tc *grpcx.TargetConfig) {
			tc.RetryMaxAttempts = 5
			tc.RetryBaseDelay, tc.RetryMaxDelay = 300*time.Millisecond, 2*time.Second
		})
		require.NoError(t, got.err)
		assert.GreaterOrEqual(t, got.retries, 1.0)
		assert.Equal(t, 2*batch, got.arrivals, "the retry was a second delivery of the same write")
		assert.Equal(t, batch, got.kept, "harmless here only because this server folds identical spans")
	})

	t.Run("retry_methods without Export: not retried", func(t *testing.T) {
		got := lose(t, func(tc *grpcx.TargetConfig) {
			tc.RetryMaxAttempts = 5
			tc.RetryMethods = []string{"jaeger.api_v3.QueryService"}
		})
		require.Equal(t, codes.Unavailable, status.Code(got.err), "%v", got.err)
		assert.InDelta(t, 0, got.retries, 0)
		assert.Equal(t, batch, got.arrivals)
	})
}
