package integration_test

// The protocol layer: grpcx in front of a real gRPC server (Jaeger's OTLP
// receiver and query API) on a healthy network — every transport the library
// can be configured for, a unary call and a server stream, and the server's
// own verdicts.

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
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
)

// A unary call over each transport, and the server's word that it arrived:
// the spans are in its store. "TLS" here is the library's default — TLS with
// the system roots, no option given.
func TestExport_ArrivesOverEveryTransport(t *testing.T) {
	t.Parallel()

	srv := jaeger(t)

	transports := []struct {
		name, addr string
		mut        func(*grpcx.TargetConfig)
		opts       []grpcx.Option
	}{
		{name: "cleartext", addr: srv.Plain, mut: cleartext},
		{name: "TLS, library default", addr: srv.TLS},
		{name: "TLS 1.3 only server", addr: srv.TLS13},
		{
			name: "mutual TLS", addr: srv.MTLS,
			opts: []grpcx.Option{trusting(func(c *tls.Config) { c.Certificates = []tls.Certificate{certs.client} })},
		},
	}

	for _, via := range transports {
		t.Run(via.name, func(t *testing.T) {
			t.Parallel()

			mut := via.mut
			if mut == nil {
				mut = func(*grpcx.TargetConfig) {}
			}

			client, conn := dial(t, policy(via.addr, mut), via.opts...)
			service := testx.Unique("transport")

			require.NoError(t, export(t.Context(), t, conn, 7, service))
			require.NoError(t, export(t.Context(), t, conn, 5, service))

			assert.Equal(t, 12, stored(t, service), "the server has both batches")
			assert.InDelta(t, 2, calls(t, client, "ok"), 0)
			assert.InDelta(t, 0, retries(t, client), 0)
		})
	}
}

// What a real server says about a request is an answer, not a failure of the
// dependency: the breaker stays closed and nothing is retried.
func TestExport_TheServersVerdictIsAnAnswer(t *testing.T) {
	t.Parallel()

	srv := jaeger(t)
	client, conn := dial(t, policy(srv.Plain, cleartext, touchyBreaker))

	// Eight megabytes against the server's four-megabyte limit.
	_, err := coltracepb.NewTraceServiceClient(conn).Export(t.Context(), spans(traceID(t), 30_000, "too-big", 200))
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
	require.ErrorContains(t, err, "larger than max")

	// A method this server does not have.
	err = conn.Invoke(t.Context(), "/integration.Nothing/Here", &emptypb.Empty{}, &emptypb.Empty{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)

	assert.Equal(t, resilience.StateClosed, breaker(t, client), "with breaker_min_requests 1 a single failure would have opened it")
	assert.InDelta(t, 0, retries(t, client), 0)
	assert.InDelta(t, 1, calls(t, client, "resource_exhausted"), 0)
	assert.InDelta(t, 1, calls(t, client, "unimplemented"), 0)
}

// A server stream of a hundred messages: every one arrives, and the stream is
// one call — counted, timed and reported to the breaker once, when it ends.
func TestStream_EveryMessageAndOneCall(t *testing.T) {
	t.Parallel()

	const traces, each = 100, 20

	service := seed(t, traces, each)
	client, conn := dial(t, policy(jaeger(t).Query, cleartext))

	stream, err := findTraces(t.Context(), t, conn, service)
	require.NoError(t, err)

	method := map[string]string{"method": findTracesMethod}
	messages, total := 0, 0

	for {
		n, err := next(stream)
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)

		messages++
		total += n

		assert.InDelta(t, 0, metric(t, client, "grpc_client_requests_total", method), 0, "not counted while it is still running")
	}

	assert.Equal(t, traces, messages, "one message per trace")
	assert.Equal(t, traces*each, total)
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"method": findTracesMethod, "outcome": "ok"}), 0)
}

// The target's timeout is the deadline of the whole call — for a stream, of
// the whole stream, not of each message. A reader that takes longer than
// that loses the stream mid-way, and the breaker counts it. A stream meant to
// stay open needs timeout 0 on its target (or its own, longer deadline).
func TestStream_TheDefaultDeadlineCoversTheWholeStream(t *testing.T) {
	t.Parallel()

	service := seed(t, 60, 20)
	srv := jaeger(t)

	t.Run("a slow reader runs out of time", func(t *testing.T) {
		t.Parallel()

		client, conn := dial(t, policy(srv.Query, cleartext, touchyBreaker, timeout(400*time.Millisecond)))

		stream, err := findTraces(t.Context(), t, conn, service)
		require.NoError(t, err)

		read := 0

		for {
			_, err = next(stream)
			if err != nil {
				break
			}

			read++

			time.Sleep(50 * time.Millisecond) // sixty messages at this pace need three seconds
		}

		assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
		assert.Positive(t, read)
		assert.Less(t, read, 60)
		assert.Equal(t, resilience.StateOpen, breaker(t, client))
	})

	t.Run("timeout 0 lets it take its time", func(t *testing.T) {
		t.Parallel()

		_, conn := dial(t, policy(srv.Query, cleartext, timeout(0)))

		stream, err := findTraces(t.Context(), t, conn, service)
		require.NoError(t, err)

		read := 0

		for {
			_, err = next(stream)
			if err != nil {
				break
			}

			read++

			time.Sleep(10 * time.Millisecond)
		}

		require.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 60, read)
	})
}

// The caller walks away from a stream. That says nothing about the server:
// the breaker ignores it, the call is counted as canceled, and the connection
// carries the next call.
func TestStream_CallerCancelsMidStream(t *testing.T) {
	t.Parallel()

	service := seed(t, 100, 20)
	client, conn := dial(t, policy(jaeger(t).Query, cleartext, touchyBreaker))

	ctx, cancel := context.WithCancel(t.Context())

	stream, err := findTraces(ctx, t, conn, service)
	require.NoError(t, err)

	for range 3 {
		_, err := next(stream)
		require.NoError(t, err)
	}

	cancel()

	require.Eventually(t, func() bool { return calls(t, client, "canceled") == 1 }, 5*time.Second, 10*time.Millisecond,
		"the abandoned stream reports when its context ends, without another read")
	assert.Equal(t, resilience.StateClosed, breaker(t, client))

	again, err := findTraces(t.Context(), t, conn, service)
	require.NoError(t, err)

	_, err = next(again)
	require.NoError(t, err, "the same connection serves a new stream")

	drained := again
	for err == nil {
		_, err = next(drained)
	}

	require.ErrorIs(t, err, io.EOF)
}
