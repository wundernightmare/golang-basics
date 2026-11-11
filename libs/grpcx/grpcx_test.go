package grpcx_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const (
	checkMethod   = "/grpc.health.v1.Health/Check"
	watchMethod   = "/grpc.health.v1.Health/Watch"
	collectMethod = "/test.Collector/Collect"
)

// backend is a gRPC server whose health service does what the test says. The
// health service stands in for a real one: a unary method (Check) and a
// server-streaming one (Watch), with no generated code of our own.
type backend struct {
	healthpb.UnimplementedHealthServer

	check func(ctx context.Context) error
	watch func(stream healthpb.Health_WatchServer) error
	hits  atomic.Int32
	addr  string
}

func (b *backend) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	b.hits.Add(1)

	if b.check != nil {
		if err := b.check(ctx); err != nil {
			return nil, err
		}
	}

	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (b *backend) Watch(_ *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	b.hits.Add(1)

	if b.watch != nil {
		return b.watch(stream)
	}

	return nil
}

// collector is a client-streaming method: it reads every request and answers
// once, with the number it read in the status field.
var collector = grpc.ServiceDesc{
	ServiceName: "test.Collector",
	HandlerType: (*any)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Collect",
		ClientStreams: true,
		Handler: func(_ any, stream grpc.ServerStream) error {
			var n int32

			for {
				err := stream.RecvMsg(new(healthpb.HealthCheckRequest))
				if errors.Is(err, io.EOF) {
					return stream.SendMsg(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_ServingStatus(n)})
				}

				if err != nil {
					return err
				}

				n++
			}
		},
	}},
}

func serve(t *testing.T, b *backend, opts ...grpc.ServerOption) *backend {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(srv, b)
	srv.RegisterService(&collector, struct{}{})

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(srv.Stop)

	b.addr = ln.Addr().String()

	return b
}

// target is DefaultTarget("api", addr), plaintext, with fast retries, then mut.
func target(addr string, mut func(*grpcx.TargetConfig)) grpcx.TargetConfig {
	tc := grpcx.DefaultTarget("api", addr)
	tc.Insecure = true
	tc.RetryBaseDelay = time.Millisecond

	tc.RetryMaxDelay = 5 * time.Millisecond
	if mut != nil {
		mut(&tc)
	}

	return tc
}

// dial builds a client over tc, closed when the test ends, and its "api"
// connection.
func dial(t *testing.T, tc grpcx.TargetConfig, opts ...grpcx.Option) (*grpcx.Client, *grpc.ClientConn) {
	t.Helper()

	client, err := grpcx.New(grpcx.DefaultConfig(tc), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	conn, err := client.Conn("api")
	require.NoError(t, err)

	return client, conn
}

func check(ctx context.Context, conn *grpc.ClientConn) error {
	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})

	return err //nolint:wrapcheck // the error under test
}

func metric(t *testing.T, c *grpcx.Client, name string, labels map[string]string) float64 {
	t.Helper()
	return testx.Metric(t, c.Registry(), name, labels)
}

func state(t *testing.T, c *grpcx.Client) resilience.State {
	t.Helper()

	st, err := c.BreakerState("api")
	require.NoError(t, err)

	return st
}

func fail(code codes.Code) func(context.Context) error {
	return func(context.Context) error { return status.Error(code, "nope") }
}

func hang(ctx context.Context) error {
	<-ctx.Done()

	return status.FromContextError(ctx.Err()).Err()
}

// noRetries switches gRPC's retries off, so one call is one attempt.
func noRetries(tc *grpcx.TargetConfig) { tc.RetryMaxAttempts = 1 }

func TestCall_SuccessTracesAndMeters(t *testing.T) {
	t.Parallel()

	spans := testx.Recorder(t)

	var traceparent atomic.Value

	srv := serve(t, &backend{check: func(ctx context.Context) error {
		md, _ := metadata.FromIncomingContext(ctx)
		traceparent.Store(md.Get("traceparent"))

		return nil
	}})
	client, conn := dial(t, target(srv.addr, nil),
		grpcx.WithTracerProvider(spans.Provider), grpcx.WithPropagator(spans.Propagator))

	require.NoError(t, check(t.Context(), conn))
	assert.NotEmpty(t, traceparent.Load(), "traceparent is injected")

	ended := spans.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "grpc.health.v1.Health/Check", ended[0].Name())

	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"target": "api", "method": checkMethod, "outcome": "ok"}), 0)
	assert.InDelta(t, 1, metric(t, client, "grpc_client_request_duration_seconds", map[string]string{"target": "api", "method": checkMethod}), 0)
	assert.InDelta(t, 0, metric(t, client, "grpc_client_circuit_breaker_state", map[string]string{"target": "api"}), 0)
	assert.InDelta(t, 0, metric(t, client, "grpc_client_retries_total", map[string]string{"target": "api"}), 0)
	testx.LintMetrics(t, client.Registry())
}

func TestNew_RejectsInvalidConfigAndDuplicateRegistration(t *testing.T) {
	t.Parallel()

	_, err := grpcx.New(grpcx.DefaultConfig(grpcx.TargetConfig{Name: "api"}))
	require.ErrorContains(t, err, "address is empty")

	reg := prometheus.NewRegistry()
	first, err := grpcx.New(grpcx.DefaultConfig(target("127.0.0.1:1", nil)), grpcx.WithRegisterer(reg))
	require.NoError(t, err)

	defer func() { require.NoError(t, first.Close()) }()

	assert.Nil(t, first.Registry(), "the metrics live on the caller's registerer")
	assert.InDelta(t, 0, testx.Metric(t, reg, "grpc_client_circuit_breaker_state", map[string]string{"target": "api"}), 0)

	_, err = grpcx.New(grpcx.DefaultConfig(target("127.0.0.1:1", nil)), grpcx.WithRegisterer(reg))
	require.ErrorContains(t, err, "register metrics")
}

func TestConn_UnknownTargetAndClosed(t *testing.T) {
	t.Parallel()

	client, err := grpcx.New(grpcx.DefaultConfig(target("127.0.0.1:1", nil)))
	require.NoError(t, err)

	_, err = client.Conn("nope")
	require.ErrorIs(t, err, grpcx.ErrUnknownTarget)
	_, err = client.BreakerState("nope")
	require.ErrorIs(t, err, grpcx.ErrUnknownTarget)

	conn, err := client.Conn("api")
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.NoError(t, client.Close(), "Close is idempotent")

	_, err = client.Conn("api")
	require.ErrorIs(t, err, grpcx.ErrClosed)
	assert.Equal(t, codes.Canceled, status.Code(check(t.Context(), conn)), "a closed connection fails its calls")
}

func TestCall_TargetTimeoutIsTheDefaultDeadline(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{check: hang})
	_, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
		tc.Timeout = 50 * time.Millisecond
		noRetries(tc)
	}))

	start := time.Now()
	err := check(t.Context(), conn) // no deadline of its own
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Less(t, time.Since(start), 2*time.Second)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	start = time.Now()
	err = check(ctx, conn)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Less(t, time.Since(start), 45*time.Millisecond, "the caller's shorter deadline wins")
}

func TestCall_ZeroTimeoutIsNoDeadline(t *testing.T) {
	t.Parallel()

	var deadline atomic.Bool

	srv := serve(t, &backend{check: func(ctx context.Context) error {
		_, ok := ctx.Deadline()
		deadline.Store(ok)

		return nil
	}})
	_, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.Timeout = 0 }))

	require.NoError(t, check(t.Context(), conn))
	assert.False(t, deadline.Load(), "timeout 0 is no deadline, not an expired one")
}

func TestCall_RetriesListedCodes(t *testing.T) {
	t.Parallel()

	flaky := func(code codes.Code) *backend {
		b := &backend{}
		b.check = func(context.Context) error {
			if b.hits.Load() <= 2 {
				return status.Error(code, "try again")
			}

			return nil
		}

		return serve(t, b)
	}

	spans := testx.Recorder(t)
	srv := flaky(codes.Unavailable)
	client, conn := dial(t, target(srv.addr, nil), grpcx.WithTracerProvider(spans.Provider), grpcx.WithPropagator(spans.Propagator))
	require.NoError(t, check(t.Context(), conn))
	assert.Equal(t, int32(3), srv.hits.Load(), "two failures, then the success")
	assert.Len(t, spans.Ended(), 3, "a client span per attempt")
	assert.InDelta(t, 2, metric(t, client, "grpc_client_retries_total", nil), 0)
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "ok"}), 0, "one call, whatever the attempts")
	assert.InDelta(t, -1, metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "unavailable"}), 0)

	srv = flaky(codes.Internal)
	_, conn = dial(t, target(srv.addr, nil))
	assert.Equal(t, codes.Internal, status.Code(check(t.Context(), conn)))
	assert.Equal(t, int32(1), srv.hits.Load(), "INTERNAL is not in retry_codes")

	srv = flaky(codes.Internal)
	_, conn = dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.RetryCodes = []string{"INTERNAL", "UNAVAILABLE"} }))
	require.NoError(t, check(t.Context(), conn))
	assert.Equal(t, int32(3), srv.hits.Load())
}

func TestCall_RetryMaxAttemptsBoundsACall(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{check: fail(codes.Unavailable)})
	_, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.RetryMaxAttempts = 4 }))

	assert.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
	assert.Equal(t, int32(4), srv.hits.Load())
}

func TestCall_RetryMethodsLimitRetries(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		methods []string
		hits    int32
	}{
		{"every method", nil, 3},
		{"the service", []string{"grpc.health.v1.Health"}, 3},
		{"the method", []string{"/grpc.health.v1.Health/Check"}, 3},
		{"another method", []string{"grpc.health.v1.Health/Watch"}, 1},
		{"another service", []string{"billing.v1.Invoices"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := serve(t, &backend{check: fail(codes.Unavailable)})
			_, conn := dial(t, target(srv.addr, func(c *grpcx.TargetConfig) { c.RetryMethods = tc.methods }))

			assert.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
			assert.Equal(t, tc.hits, srv.hits.Load())
		})
	}
}

func TestCall_RetryThrottlingBoundsAmplification(t *testing.T) {
	t.Parallel()

	const calls = 10

	attempts := func(maxTokens int) int32 {
		srv := serve(t, &backend{check: fail(codes.Unavailable)})
		_, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
			tc.RetryThrottleMaxTokens = maxTokens
			tc.FailureRatio = 0 // no breaker: every call reaches gRPC
		}))

		for range calls {
			assert.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
		}

		return srv.hits.Load()
	}

	assert.Equal(t, int32(3*calls), attempts(0), "no throttling: every call makes its 3 attempts")

	throttled := attempts(4)
	assert.GreaterOrEqual(t, throttled, int32(calls))
	assert.Less(t, throttled, int32(calls+calls/2), "an outage drains the bucket: retries stop")
}

func TestCall_BreakerOpensRejectsAndRecovers(t *testing.T) {
	t.Parallel()

	var down atomic.Bool

	down.Store(true)

	srv := serve(t, &backend{check: func(context.Context) error {
		if down.Load() {
			return status.Error(codes.Unavailable, "down")
		}

		return nil
	}})
	buf := &testx.LogBuffer{}
	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.MinRequests = 2
		tc.OpenTimeout = 50 * time.Millisecond
	}), grpcx.WithLogger(slog.New(slog.NewJSONHandler(buf, nil))))

	for range 2 {
		assert.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
	}

	require.Equal(t, resilience.StateOpen, state(t, client))

	err := check(t.Context(), conn)
	require.ErrorIs(t, err, grpcx.ErrCircuitOpen)
	assert.Equal(t, codes.Unavailable, status.Code(err), "a rejection reads as Unavailable to status-only code")
	assert.Equal(t, "grpcx: api: circuit open", err.Error())
	assert.Equal(t, int32(2), srv.hits.Load(), "a rejected call never reaches the server")

	assert.InDelta(t, 2, metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "unavailable"}), 0)
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "circuit_open"}), 0)
	assert.InDelta(t, 2, metric(t, client, "grpc_client_request_duration_seconds", nil), 0, "the rejected one never reached gRPC")
	assert.InDelta(t, 1, metric(t, client, "grpc_client_circuit_breaker_state", nil), 0)
	assert.InDelta(t, 1, metric(t, client, "grpc_client_circuit_breaker_transitions_total", map[string]string{"from": "closed", "to": "open"}), 0)
	assert.NotNil(t, buf.Find(t, map[string]any{"level": "INFO", "msg": "grpcx: circuit breaker open", "target": "api", "from": "closed"}), buf.String())

	down.Store(false)
	time.Sleep(60 * time.Millisecond) // breaker_open_timeout

	require.NoError(t, check(t.Context(), conn), "the probe")
	assert.Equal(t, resilience.StateClosed, state(t, client))
	assert.InDelta(t, 0, metric(t, client, "grpc_client_circuit_breaker_state", nil), 0)
}

func TestCall_WhatTheBreakerCounts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		code codes.Code
		want resilience.State
	}{
		{codes.Unavailable, resilience.StateOpen},
		{codes.Internal, resilience.StateOpen},
		{codes.Unknown, resilience.StateOpen},
		{codes.DataLoss, resilience.StateOpen},
		{codes.DeadlineExceeded, resilience.StateOpen},
		{codes.NotFound, resilience.StateClosed},
		{codes.InvalidArgument, resilience.StateClosed},
		{codes.ResourceExhausted, resilience.StateClosed},
		{codes.PermissionDenied, resilience.StateClosed},
		{codes.Unimplemented, resilience.StateClosed},
		{codes.Canceled, resilience.StateClosed},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			t.Parallel()

			srv := serve(t, &backend{check: fail(tc.code)})
			client, conn := dial(t, target(srv.addr, func(c *grpcx.TargetConfig) {
				noRetries(c)

				c.MinRequests = 2
			}))

			for range 3 {
				_ = check(t.Context(), conn)
			}

			assert.Equal(t, tc.want, state(t, client))
		})
	}
}

func TestCall_NoBreakerNeverRejects(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{check: fail(codes.Unavailable)})
	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.BreakerConfig = resilience.BreakerConfig{}
	}))

	for range 30 {
		assert.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
	}

	assert.Equal(t, int32(30), srv.hits.Load())
	assert.Equal(t, resilience.StateClosed, state(t, client))
}

func TestCall_CallerDeadlineIsABreakerFailureCancellationIsNot(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 8)
	srv := serve(t, &backend{check: func(ctx context.Context) error {
		started <- struct{}{}

		return hang(ctx)
	}})
	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.MinRequests = 2
	}))

	for range 4 {
		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			<-started
			cancel()
		}()

		assert.Equal(t, codes.Canceled, status.Code(check(ctx, conn)))
	}

	require.Equal(t, resilience.StateClosed, state(t, client), "the caller walking away says nothing about the dependency")

	for range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		err := check(ctx, conn)

		cancel()
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	}

	assert.Equal(t, resilience.StateOpen, state(t, client), "the dependency did not answer within the caller's deadline")
}

func watch(ctx context.Context, conn *grpc.ClientConn) (healthpb.Health_WatchClient, error) {
	return healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{}) //nolint:wrapcheck // the error under test
}

func send(n int) func(healthpb.Health_WatchServer) error {
	return func(stream healthpb.Health_WatchServer) error {
		for range n {
			if err := stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}); err != nil {
				return err //nolint:wrapcheck // the stream's error, as is
			}
		}

		return nil
	}
}

func TestStream_ReadToTheEndIsOneSuccessfulCall(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{watch: send(3)})
	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.MinRequests = 1 }))

	stream, err := watch(t.Context(), conn)
	require.NoError(t, err)

	for range 3 {
		_, err = stream.Recv()
		require.NoError(t, err)

		assert.InDelta(t, -1, metric(t, client, "grpc_client_requests_total", nil), 0, "not counted before it ends")
	}

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"method": watchMethod, "outcome": "ok"}), 0)
	assert.InDelta(t, 1, metric(t, client, "grpc_client_request_duration_seconds", map[string]string{"method": watchMethod}), 0)
	assert.Equal(t, resilience.StateClosed, state(t, client))
}

func TestStream_FailureMidStreamIsABreakerFailure(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{watch: func(stream healthpb.Health_WatchServer) error {
		_ = send(1)(stream)

		return status.Error(codes.Unavailable, "going away")
	}})
	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.MinRequests = 1 }))

	stream, err := watch(t.Context(), conn)
	require.NoError(t, err)

	_, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, resilience.StateClosed, state(t, client), "a message is not the end")

	_, err = stream.Recv()
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, resilience.StateOpen, state(t, client))
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"method": watchMethod, "outcome": "unavailable"}), 0)

	_, err = watch(t.Context(), conn)
	require.ErrorIs(t, err, grpcx.ErrCircuitOpen, "a stream is rejected like a call")
	assert.Equal(t, int32(1), srv.hits.Load())
}

func TestStream_AbandonedByCancellationReportsOnce(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	srv := serve(t, &backend{watch: func(stream healthpb.Health_WatchServer) error {
		_ = send(1)(stream)

		select {
		case <-release:
		case <-stream.Context().Done():
		}

		return nil
	}})

	t.Cleanup(func() { close(release) })

	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.MinRequests = 1 }))

	ctx, cancel := context.WithCancel(t.Context())
	stream, err := watch(ctx, conn)
	require.NoError(t, err)

	_, err = stream.Recv()
	require.NoError(t, err)

	cancel() // the caller stops reading and never calls Recv again

	require.Eventually(t, func() bool {
		return metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "canceled"}) == 1
	}, 5*time.Second, 5*time.Millisecond, "an abandoned stream still reports")

	_, err = stream.Recv()
	assert.Equal(t, codes.Canceled, status.Code(err))
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", nil), 0, "and only once")
	assert.Equal(t, resilience.StateClosed, state(t, client), "the caller walking away says nothing about the dependency")
}

func TestStream_ClientStreamingEndsAtItsOneResponse(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{})
	client, conn := dial(t, target(srv.addr, nil))

	stream, err := conn.NewStream(t.Context(), &grpc.StreamDesc{StreamName: "Collect", ClientStreams: true}, collectMethod)
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, stream.SendMsg(&healthpb.HealthCheckRequest{}))
	}

	require.NoError(t, stream.CloseSend())

	var resp healthpb.HealthCheckResponse
	require.NoError(t, stream.RecvMsg(&resp))
	assert.Equal(t, int32(3), int32(resp.GetStatus()))
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"method": collectMethod, "outcome": "ok"}), 0,
		"a client-streaming call is over at its response: nobody reads on to io.EOF")
}

func TestStream_FailsToStart(t *testing.T) {
	t.Parallel()

	client, conn := dial(t, target("127.0.0.1:1", func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.MinRequests = 1
	}))

	_, err := watch(t.Context(), conn)
	if err == nil { // gRPC may report a connection failure at the first Recv instead
		t.Skip("the stream started lazily")
	}

	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, resilience.StateOpen, state(t, client))
}

func TestDial_TLSIsTheDefault(t *testing.T) {
	t.Parallel()

	// httptest lends its self-signed certificate (valid for 127.0.0.1).
	lender := httptest.NewTLSServer(nil)
	cert, leaf := lender.TLS.Certificates[0], lender.Certificate()
	lender.Close()

	srv := serve(t, &backend{}, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
	secure := func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.Insecure = false
	}

	_, conn := dial(t, target(srv.addr, secure))
	err := check(t.Context(), conn)
	assert.Equal(t, codes.Unavailable, status.Code(err), "the system roots do not trust the test certificate")
	require.ErrorContains(t, err, "certificate")

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	_, conn = dial(t, target(srv.addr, secure),
		grpcx.WithDialOptions(grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}))))
	require.NoError(t, check(t.Context(), conn), "WithDialOptions credentials replace the default ones")

	plain := serve(t, &backend{})
	_, conn = dial(t, target(plain.addr, secure))
	assert.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)), "TLS against a plaintext server fails: no silent downgrade")
}

func TestDial_WaitForReadyQueuesCalls(t *testing.T) {
	t.Parallel()

	// The backend comes up after the first call was made: wait_for_ready
	// queues it instead of failing with Unavailable.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	_, conn := dial(t, target(addr, func(tc *grpcx.TargetConfig) {
		tc.WaitForReady = true
		tc.LoadBalancing = grpcx.PickFirst
	}))

	done := make(chan error, 1)
	go func() { done <- check(t.Context(), conn) }()

	select {
	case err := <-done:
		t.Fatalf("the call did not wait for the connection: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("the port was taken meanwhile: %v", err)
	}

	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, &backend{})

	go func() { _ = srv.Serve(listener) }()

	t.Cleanup(srv.Stop)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the queued call never went through")
	}
}
