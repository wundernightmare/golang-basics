package grpcx_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// grpcx has no DNS code of its own: a `dns:///` target is resolved by
// gRPC's resolver, which asks when the channel is built and again only when
// a connection fails. These tests say what a caller sees — against a real
// name server (testx.DNS) and that resolver.

const (
	dnsName = "billing.dns.test"

	// reResolveAfter replaces gRPC's 30-second floor between two
	// resolutions for this test binary (see TestMain).
	reResolveAfter = 200 * time.Millisecond
)

// dnsTarget is the target of name:port as the name server at dns knows it.
func dnsTarget(dns *testx.DNS, port string) string {
	return "dns://" + dns.Addr() + "/" + net.JoinHostPort(dnsName, port)
}

// A name that does not resolve fails a call with Unavailable — at once, not
// at its deadline — and the breaker counts it like any dead backend.
func TestDial_ANameThatDoesNotResolveIsUnavailable(t *testing.T) {
	t.Parallel()

	for name, breakDNS := range map[string]func(*testx.DNS){
		"no such host (NXDOMAIN)":          func(*testx.DNS) {},
		"the name server fails (SERVFAIL)": (*testx.DNS).Fail,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dns := testx.NewDNS(t)
			breakDNS(dns)

			client, conn := dial(t, target(dnsTarget(dns, "443"), func(tc *grpcx.TargetConfig) {
				noRetries(tc)

				tc.Timeout = 30 * time.Second
				tc.Breaker.MinRequests = 1
			}))

			start := time.Now()
			err := check(t.Context(), conn)

			assert.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			assert.Less(t, time.Since(start), 10*time.Second, "long before the deadline")
			assert.Equal(t, resilience.StateOpen, state(t, client))
		})
	}
}

// A name server that does not answer: until its name has resolved a channel
// has no service config, and the deadline written there does not exist yet —
// gRPC would let the call wait for the resolver (30 seconds of patience)
// with no deadline at all. The target's timeout is therefore also the
// context deadline of every call, and the breaker hears the failure.
func TestDial_ASilentNameServerIsBoundedByTheTargetTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 100 * time.Millisecond

	for name, call := range map[string]func(context.Context, *grpc.ClientConn) error{
		"a unary call": check,
		"a stream": func(ctx context.Context, conn *grpc.ClientConn) error {
			stream, err := watch(ctx, conn)
			if err != nil {
				return err
			}

			_, err = stream.Recv()

			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dns := testx.NewDNS(t)
			dns.Silence()

			client, conn := dial(t, target(dnsTarget(dns, "443"), func(tc *grpcx.TargetConfig) {
				noRetries(tc)

				tc.Timeout = timeout
				tc.Breaker.MinRequests = 1
			}))

			// Closing a channel waits for its resolver, and a lookup in
			// flight waits for its answer: let it come, late.
			t.Cleanup(dns.Answer)

			start := time.Now()
			err := call(t.Context(), conn)
			took := time.Since(start)

			assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
			assert.GreaterOrEqual(t, took, timeout)
			assert.Less(t, took, 5*time.Second, "not the resolver's patience")
			assert.Equal(t, resilience.StateOpen, state(t, client))
		})
	}
}

// A name that appears later is found by the resolver's own retry (it backs
// off from one second), with no call needed to trigger it; wait_for_ready
// holds a call until then instead of failing it.
func TestDial_ANameThatAppearsIsFound(t *testing.T) {
	t.Parallel()

	dns := testx.NewDNS(t)
	server := serve(t, &backend{})

	_, port, err := net.SplitHostPort(server.addr)
	require.NoError(t, err)

	_, conn := dial(t, target(dnsTarget(dns, port), func(tc *grpcx.TargetConfig) {
		tc.WaitForReady = true
		tc.Timeout = 30 * time.Second
	}))

	done := make(chan error, 1)
	go func() { done <- check(t.Context(), conn) }()

	select {
	case err := <-done:
		t.Fatalf("the call did not wait for the name: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	dns.Set(dnsName, "127.0.0.1")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("the name was never resolved again")
	}

	assert.EqualValues(t, 1, server.hits.Load())
}

// A channel with a live connection never asks again: after the record
// moves, calls keep going to the old server for as long as it keeps the
// connection. The new address is found when that connection fails — and no
// sooner than gRPC's floor between two resolutions (30 seconds by default)
// after the previous answer: a server that goes away right after a channel
// resolved it is Unavailable for up to that long. Retire the old server
// with a drain (GOAWAY, `MaxConnectionAge` on the server), and keep its
// address answering until the record's clients have left.
func TestDial_AMovedRecordIsSeenWhenTheConnectionFails(t *testing.T) {
	t.Parallel()

	dns := testx.NewDNS(t)

	lnOld, lnNew := samePort(t)
	oldServer, newServer := &backend{}, &backend{}

	stopOld := serveOn(t, oldServer, lnOld)
	serveOn(t, newServer, lnNew)

	_, port, err := net.SplitHostPort(oldServer.addr)
	require.NoError(t, err)

	dns.Set(dnsName, "127.0.0.1")

	_, conn := dial(t, target(dnsTarget(dns, port), func(tc *grpcx.TargetConfig) {
		noRetries(tc)

		tc.LoadBalancing = grpcx.PickFirst
	}))

	require.NoError(t, check(t.Context(), conn))

	asked := dns.Queries(dnsName)

	dns.Set(dnsName, "127.0.0.2")
	time.Sleep(2 * reResolveAfter) // no timer re-resolves: the floor passing changes nothing

	for range 5 {
		require.NoError(t, check(t.Context(), conn))
	}

	assert.EqualValues(t, 6, oldServer.hits.Load(), "the live connection is used")
	assert.Zero(t, newServer.hits.Load())
	assert.Equal(t, asked, dns.Queries(dnsName), "and nobody asked the name server")

	stopOld()

	assert.Eventually(t, func() bool {
		_ = check(t.Context(), conn) // Unavailable until the resolver asks again

		return newServer.hits.Load() > 0
	}, 10*time.Second, 10*time.Millisecond, "the lost connection makes the resolver ask again")

	assert.Greater(t, dns.Queries(dnsName), asked)
}

// samePort listens on one port of 127.0.0.1 and of 127.0.0.2: the record
// decides which listener a new connection reaches.
func samePort(t *testing.T) (first, second net.Listener) {
	t.Helper()

	// A port free on one address may be taken on the other: try again.
	for range 20 {
		first, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		_, port, err := net.SplitHostPort(first.Addr().String())
		require.NoError(t, err)

		second, err := net.Listen("tcp", net.JoinHostPort("127.0.0.2", port))
		if err != nil {
			require.NoError(t, first.Close())

			continue
		}

		return first, second
	}

	require.FailNow(t, "no port free on both 127.0.0.1 and 127.0.0.2")

	return nil, nil
}
