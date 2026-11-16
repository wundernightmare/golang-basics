package grpcx_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

// What a caller's own dial options meet: where its interceptors run, what
// the breaker makes of what they return, and what a resolver may not change.

func TestDialOptions_ChainedInterceptorsRunInsideThePolicyPlainOnesOutside(t *testing.T) {
	t.Parallel()

	var plain, chained, deadlines atomic.Int32

	srv := serve(t, &backend{check: fail(codes.Unavailable)})
	_, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.Breaker.MinRequests = 2
	}), grpcx.WithDialOptions(
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			plain.Add(1)

			if _, ok := ctx.Deadline(); ok {
				deadlines.Add(1)
			}

			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			chained.Add(1)

			if _, ok := ctx.Deadline(); ok {
				deadlines.Add(1)
			}

			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	))

	for range 2 {
		require.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
	}

	require.ErrorIs(t, check(t.Context(), conn), grpcx.ErrCircuitOpen)

	assert.Equal(t, int32(3), plain.Load(), "outside the breaker: it sees the rejected call")
	assert.Equal(t, int32(2), chained.Load(), "inside the breaker: only the admitted calls")
	assert.Equal(t, int32(2), deadlines.Load(), "and only inside is the call under the target's timeout")
}

func TestDialOptions_ALocalRejectionSaysNothingAboutTheDependency(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{})
	client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) { tc.Breaker.MinRequests = 2 }),
		grpcx.WithDialOptions(grpc.WithChainUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			// Unavailable: from the dependency it would open the breaker.
			return grpcx.LocalRejection(codes.Unavailable, "rate limited")
		})))

	for range 3 {
		err := check(t.Context(), conn)
		require.ErrorIs(t, err, grpcx.ErrLocalRejection)
		require.NotErrorIs(t, err, grpcx.ErrCircuitOpen)
		assert.Equal(t, codes.Unavailable, status.Code(err))
		assert.Equal(t, "rpc error: code = Unavailable desc = rate limited", err.Error())
	}

	assert.Equal(t, resilience.StateClosed, state(t, client))
	assert.Zero(t, srv.hits.Load())
	assert.InDelta(t, 3, metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "unavailable"}), 0,
		"the outcome is the code the interceptor chose")
}

// panicky is a caller's interceptor pair that panics on demand: when a call
// starts, or when a stream it let through is read.
type panicky struct{ onCall, onRecv atomic.Bool }

func (p *panicky) options() grpcx.Option {
	return grpcx.WithDialOptions(
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if p.onCall.Load() {
				panic("interceptor blew up")
			}

			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if p.onCall.Load() {
				panic("interceptor blew up")
			}

			cs, err := streamer(ctx, desc, cc, method, opts...)

			return &panickyStream{ClientStream: cs, p: p}, err
		}),
	)
}

type panickyStream struct {
	grpc.ClientStream

	p *panicky
}

func (s *panickyStream) RecvMsg(m any) error {
	if s.p.onRecv.Load() {
		panic("codec blew up")
	}

	return s.ClientStream.RecvMsg(m)
}

// A half-open breaker waits for its probe's result. A probe that panics —
// in the caller's interceptor or codec, recovered further up — is a result
// too: without one the target would stay cut off until a restart, and the
// call would be missing from the metrics.
func TestCall_AProbeThatPanicsGivesItsSlotBack(t *testing.T) {
	t.Parallel()

	const openTimeout = 50 * time.Millisecond

	for name, probe := range map[string]func(*panicky, *grpc.ClientConn){
		"a unary call": func(p *panicky, conn *grpc.ClientConn) {
			p.onCall.Store(true)

			_ = check(t.Context(), conn)
		},
		"a stream that fails to start": func(p *panicky, conn *grpc.ClientConn) {
			p.onCall.Store(true)

			_, _ = watch(t.Context(), conn)
		},
		"a stream being read": func(p *panicky, conn *grpc.ClientConn) {
			p.onRecv.Store(true)

			stream, err := watch(t.Context(), conn)
			if err == nil {
				_, _ = stream.Recv()
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var down atomic.Bool

			down.Store(true)

			srv := serve(t, &backend{check: func(context.Context) error {
				if down.Load() {
					return status.Error(codes.Unavailable, "down")
				}

				return nil
			}})
			blowUp := &panicky{}
			client, conn := dial(t, target(srv.addr, func(tc *grpcx.TargetConfig) {
				noRetries(tc)

				tc.Breaker.MinRequests = 1
				tc.Breaker.OpenTimeout = openTimeout
			}), blowUp.options())

			require.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
			require.Equal(t, resilience.StateOpen, state(t, client))

			down.Store(false)
			time.Sleep(openTimeout + 10*time.Millisecond)

			require.Panics(t, func() { probe(blowUp, conn) })
			assert.Equal(t, resilience.StateOpen, state(t, client), "the probe failed: open again, not half-open for good")
			assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"outcome": "unknown"}), 0,
				"and the call is counted: it ended with no status")

			blowUp.onCall.Store(false)
			blowUp.onRecv.Store(false)
			time.Sleep(openTimeout + 10*time.Millisecond)

			require.NoError(t, check(t.Context(), conn), "the next probe is admitted")
			assert.Equal(t, resilience.StateClosed, state(t, client))
		})
	}
}

// The policy of a target is its config. A resolver may supply a service
// config of its own (gRPC's DNS resolver reads one from a TXT record); here
// one asks for four attempts where the target allows one.
func TestDialOptions_AResolversServiceConfigIsIgnored(t *testing.T) {
	t.Parallel()

	srv := serve(t, &backend{check: fail(codes.Unavailable)})

	names := manual.NewBuilderWithScheme("configured")
	names.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		names.InitialState(resolver.State{
			Addresses: []resolver.Address{{Addr: srv.addr}},
			ServiceConfig: cc.ParseServiceConfig(`{"methodConfig":[{"name":[{}],"retryPolicy":{
				"maxAttempts":4,"initialBackoff":"0.001s","maxBackoff":"0.001s","backoffMultiplier":1,"retryableStatusCodes":["UNAVAILABLE"]}}]}`),
		})
	}

	_, conn := dial(t, target("configured:///api", noRetries), grpcx.WithDialOptions(grpc.WithResolvers(names)))

	require.Equal(t, codes.Unavailable, status.Code(check(t.Context(), conn)))
	assert.Equal(t, int32(1), srv.hits.Load(), "one attempt, as the target says")
}
