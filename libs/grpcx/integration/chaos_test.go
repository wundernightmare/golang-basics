package integration_test

// The network layer: Toxiproxy between grpcx and the real server. gRPC keeps
// one long-lived connection and reconnects on its own schedule, so what a
// fault costs depends on when it hits and on wait_for_ready — these tests pin
// both.
//
// The tests of this file share the proxies, so none of them is parallel.

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// through returns the proxy in front of one of Jaeger's ports.
func through(t *testing.T, name, port string) *containers.Proxy {
	t.Helper()

	jaeger(t)

	return containers.ProxyTo(t, name, containers.FixtureContainer(t, "jaeger"), port)
}

// eventually retries a call until it succeeds: after a failed connection
// grpc-go waits out a reconnect backoff (a second at first) before it dials
// again, and a call made in that window fails at once.
func eventually(t *testing.T, call func() error, msg string) {
	t.Helper()

	var last error

	require.Eventuallyf(t, func() bool {
		last = call()

		return last == nil
	}, 15*time.Second, 100*time.Millisecond, "%s (last error: %v)", msg, &last)
}

// The TCP connection is up and the TLS handshake fails underneath it. Without
// wait_for_ready a call is failed the moment gRPC gives up on the connection
// attempt, with Unavailable; while the handshake merely hangs, the call waits
// with it — and is released by the target's timeout, not by grpc-go's
// twenty-second connect timeout. After the network is whole the same client
// connects again by itself.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger's TLS listener
func TestChaos_TLSHandshake(t *testing.T) {
	proxy := through(t, "otlp-tls", otlpTLS)

	faults := []struct {
		name   string
		inject func()
		code   codes.Code
		within time.Duration
	}{
		{
			name:   "ClientHello goes into a black hole",
			inject: func() { proxy.Toxic("timeout", "upstream", map[string]any{"timeout": 0}) },
			code:   codes.DeadlineExceeded, within: 1500 * time.Millisecond,
		},
		{
			name:   "ServerHello never comes back",
			inject: func() { proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0}) },
			code:   codes.DeadlineExceeded, within: 1500 * time.Millisecond,
		},
		{
			name:   "connection reset during the handshake",
			inject: func() { proxy.Toxic("reset_peer", "downstream", map[string]any{"timeout": 0}) },
			code:   codes.Unavailable, within: time.Second,
		},
		{
			name:   "connection cut 200 bytes into the server's flight",
			inject: func() { proxy.Toxic("limit_data", "downstream", map[string]any{"bytes": 200}) },
			code:   codes.Unavailable, within: time.Second,
		},
	}

	for _, fault := range faults {
		t.Run(fault.name, func(t *testing.T) {
			client, conn := dial(t, policy(proxy.Addr, noRetries, touchyBreaker, timeout(700*time.Millisecond)))
			service := testx.Unique("handshake")

			fault.inject()

			start := time.Now()
			err := export(context.Background(), t, conn, 2, service)
			require.Equal(t, fault.code, status.Code(err), "%v", err)
			assert.Less(t, time.Since(start), fault.within)
			assert.Equal(t, resilience.StateOpen, breaker(t, client), "either way the dependency did not answer")
			assert.Zero(t, stored(t, service))

			proxy.Reset()

			// A second client, so the breaker of the first (now open) is not in the way.
			_, healthy := dial(t, policy(proxy.Addr, noRetries))
			eventually(t, func() error { return export(context.Background(), t, healthy, 2, service) }, "a fresh client connects")
			assert.Equal(t, 2, stored(t, service))
		})
	}
}

// A dependency that goes away and comes back, seen through the breaker: the
// failures open it, an open breaker answers without touching the network —
// as Unavailable to code that looks at the status, as ErrCircuitOpen to code
// that asks — and after breaker_open_timeout a probe finds the dependency
// alive and closes it.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger
func TestChaos_OutageOpensTheBreakerAndAProbeClosesIt(t *testing.T) {
	proxy := through(t, "otlp-plain", otlpPlain)
	client, conn := dial(t, policy(proxy.Addr, cleartext, noRetries, func(tc *grpcx.TargetConfig) {
		tc.MinRequests = 4
		tc.OpenTimeout = 500 * time.Millisecond
	}))

	require.NoError(t, export(t.Context(), t, conn, 1, "breaker"))

	proxy.Down()

	for range 10 {
		err := export(t.Context(), t, conn, 1, "breaker")
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)

		if breaker(t, client) == resilience.StateOpen {
			break
		}
	}

	require.Equal(t, resilience.StateOpen, breaker(t, client))

	reached := calls(t, client, "unavailable")
	start := time.Now()

	for range 50 {
		err := export(t.Context(), t, conn, 1, "breaker")
		require.ErrorIs(t, err, grpcx.ErrCircuitOpen)
		require.Equal(t, codes.Unavailable, status.Code(err))
	}

	assert.Less(t, time.Since(start), 100*time.Millisecond, "fifty rejections cost no network round trip")
	assert.InDelta(t, reached, calls(t, client, "unavailable"), 0, "rejections have their own outcome")
	assert.InDelta(t, 50, calls(t, client, "circuit_open"), 0)

	proxy.Up()

	eventually(t, func() error { return export(t.Context(), t, conn, 1, "breaker") },
		"after breaker_open_timeout, and gRPC's reconnect, a probe goes through")
	assert.Equal(t, resilience.StateClosed, breaker(t, client))
}

// A short outage — a restart, a failover — and what bridges it. gRPC's
// retries do not: they are spent within a few hundred milliseconds, while the
// channel takes about a second to dial again, so a call made during the gap
// fails even though the server is back long before its deadline.
// wait_for_ready is the setting for that: the call waits for the connection
// and goes through.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger
func TestChaos_AShortOutageAndWaitForReady(t *testing.T) {
	proxy := through(t, "otlp-plain", otlpPlain)

	duringAnOutage := func(t *testing.T, mut func(*grpcx.TargetConfig)) (time.Duration, float64, error) {
		t.Helper()

		client, conn := dial(t, policy(proxy.Addr, cleartext, timeout(10*time.Second), mut))
		require.NoError(t, export(t.Context(), t, conn, 1, "outage")) // connected

		proxy.Down()
		time.AfterFunc(700*time.Millisecond, proxy.Up)

		start := time.Now()
		err := export(context.Background(), t, conn, 1, "outage")
		took := time.Since(start)

		time.Sleep(time.Until(start.Add(800 * time.Millisecond))) // let the proxy come back before the next subtest

		return took, retries(t, client), err
	}

	t.Run("retries alone: spent before the channel reconnects", func(t *testing.T) {
		took, retried, err := duringAnOutage(t, func(tc *grpcx.TargetConfig) { tc.RetryMaxAttempts = 5 })
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
		assert.Less(t, took, 700*time.Millisecond, "it gave up while the server was still away")
		assert.GreaterOrEqual(t, retried, 1.0, "it did retry — into a channel that was not dialling yet")
	})

	t.Run("wait_for_ready: the call waits and succeeds", func(t *testing.T) {
		took, _, err := duringAnOutage(t, func(tc *grpcx.TargetConfig) { tc.WaitForReady = true })
		require.NoError(t, err)
		assert.GreaterOrEqual(t, took, 600*time.Millisecond, "it was held until the connection came back")
	})
}

// Latency on the path against the default deadline: under it the call is only
// slow, over it the caller gets DeadlineExceeded at the deadline — and a
// caller's own, shorter deadline wins over the target's.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger
func TestChaos_LatencyAgainstTheDefaultDeadline(t *testing.T) {
	proxy := through(t, "otlp-plain", otlpPlain)

	_, patient := dial(t, policy(proxy.Addr, cleartext, noRetries, timeout(5*time.Second)))
	_, hasty := dial(t, policy(proxy.Addr, cleartext, noRetries, timeout(250*time.Millisecond)))

	// Both connected before the path slows down.
	require.NoError(t, export(t.Context(), t, patient, 1, "latency"))
	require.NoError(t, export(t.Context(), t, hasty, 1, "latency"))

	proxy.Latency(600 * time.Millisecond)

	start := time.Now()

	require.NoError(t, export(t.Context(), t, patient, 1, "latency"))
	assert.GreaterOrEqual(t, time.Since(start), 600*time.Millisecond)

	start = time.Now()
	err := export(t.Context(), t, hasty, 1, "latency")
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
	assert.InDelta(t, 250, time.Since(start).Milliseconds(), 200, "at the target's timeout")

	own, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	start = time.Now()
	err = export(own, t, patient, 1, "latency")
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
	assert.Less(t, time.Since(start), 500*time.Millisecond, "the caller's shorter deadline wins over the target's five seconds")
}

// A server stream that dies half-way: the caller has read some messages and
// the next read fails. The failure is the stream's status — Unavailable — the
// breaker counts it, and it is not retried: messages were already delivered,
// so gRPC cannot replay the call behind the caller's back.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger's query API
func TestChaos_StreamCutMidWay(t *testing.T) {
	proxy := through(t, "query", queryGRPC)
	service := seed(t, 150, 60) // a couple of megabytes over 150 messages

	client, conn := dial(t, policy(proxy.Addr, cleartext, touchyBreaker))

	// Connected, then the cut: the connection ends after another 600 kB.
	warm, err := findTraces(t.Context(), t, conn, testx.Unique("nothing"))
	require.NoError(t, err)

	_, err = next(warm)
	require.ErrorIs(t, err, io.EOF)

	proxy.Toxic("limit_data", "downstream", map[string]any{"bytes": 600 << 10})

	stream, err := findTraces(t.Context(), t, conn, service)
	require.NoError(t, err)

	read := 0

	for {
		_, err = next(stream)
		if err != nil {
			break
		}

		read++
	}

	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	assert.Positive(t, read)
	assert.Less(t, read, 150, "a truncated stream is an error, never a short success")
	assert.Equal(t, resilience.StateOpen, breaker(t, client))
	assert.InDelta(t, 0, retries(t, client), 0, "a stream that delivered messages is not replayed")
	assert.InDelta(t, 1, metric(t, client, "grpc_client_requests_total", map[string]string{"method": findTracesMethod, "outcome": "unavailable"}), 0)
}

// A stream that arrives too slowly for the target's timeout ends in
// DeadlineExceeded at the deadline, in the middle of the data.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger's query API
func TestChaos_ThrottledStreamRunsIntoTheDeadline(t *testing.T) {
	proxy := through(t, "query", queryGRPC)
	service := seed(t, 150, 60)

	_, conn := dial(t, policy(proxy.Addr, cleartext, timeout(700*time.Millisecond)))

	proxy.Toxic("bandwidth", "downstream", map[string]any{"rate": 256}) // KB/s: the stream needs ten seconds

	start := time.Now()

	stream, err := findTraces(t.Context(), t, conn, service)
	require.NoError(t, err)

	read := 0

	for {
		_, err = next(stream)
		if err != nil {
			break
		}

		read++
	}

	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
	assert.InDelta(t, 700, time.Since(start).Milliseconds(), 400)
	assert.Less(t, read, 150)
}

// The connection is killed while no call is in flight. gRPC notices and
// dials again on its own; whether the next call sees any of it depends on
// wait_for_ready. With it the call waits for the new connection and succeeds,
// whatever moment the channel is at. Without it there is a window — the
// channel tried to reconnect while the path was still down, and is waiting
// out its backoff — in which a call fails at once: that window is what
// TestChaos_AShortOutageAndWaitForReady pins.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger
func TestChaos_IdleConnectionKilled(t *testing.T) {
	proxy := through(t, "otlp-plain", otlpPlain)
	client, conn := dial(t, policy(proxy.Addr, cleartext, noRetries, func(tc *grpcx.TargetConfig) { tc.WaitForReady = true }))

	require.NoError(t, export(t.Context(), t, conn, 1, "idle"))

	proxy.Down() // closes the connection
	proxy.Up()

	require.Eventually(t, func() bool { return conn.GetState() != connectivity.Ready }, 5*time.Second, 10*time.Millisecond,
		"the channel noticed its connection is gone")

	require.NoError(t, export(t.Context(), t, conn, 1, "idle"), "one call, no retries: the caller never saw the dead connection")
	assert.InDelta(t, 0, calls(t, client, "unavailable"), 0)
	assert.InDelta(t, 2, calls(t, client, "ok"), 0)
}

// A connection that went silent — packets dropped, no FIN — holds a call
// until its deadline: nothing tells gRPC the peer is gone. keepalive_time is
// the guard. With it gRPC pings the silent connection and, when the ping is
// not answered within keepalive_timeout, fails the call as Unavailable long
// before the deadline.
//
// keepalive_time cannot be under ten seconds, so this test takes that long.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger
func TestChaos_KeepaliveDetectsASilentConnection(t *testing.T) {
	proxy := through(t, "otlp-plain", otlpPlain)

	onASilentConnection := func(t *testing.T, mut func(*grpcx.TargetConfig)) (time.Duration, error) {
		t.Helper()

		_, conn := dial(t, policy(proxy.Addr, cleartext, noRetries, mut))
		require.NoError(t, export(t.Context(), t, conn, 1, "keepalive"))

		proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0})
		proxy.Toxic("timeout", "upstream", map[string]any{"timeout": 0})

		defer proxy.Reset()

		start := time.Now()
		err := export(context.Background(), t, conn, 1, "keepalive")

		return time.Since(start), err
	}

	t.Run("without keepalive: held until the deadline", func(t *testing.T) {
		took, err := onASilentConnection(t, timeout(3*time.Second))
		require.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
		assert.GreaterOrEqual(t, took, 2900*time.Millisecond)
	})

	t.Run("with keepalive: failed in ping time", func(t *testing.T) {
		took, err := onASilentConnection(t, func(tc *grpcx.TargetConfig) {
			tc.Timeout = 60 * time.Second
			tc.KeepaliveTime = 10 * time.Second
			tc.KeepaliveTimeout = time.Second
		})
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
		assert.Greater(t, took, 9*time.Second)
		assert.Less(t, took, 20*time.Second, "eleven seconds or so, not the sixty of the deadline")
	})
}

// Two addresses behind one target, one of them dead. round_robin sends every
// call to the live one — no caller sees an error; pick_first, given the dead
// address first, moves on to the second.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Jaeger
func TestChaos_LoadBalancingWithADeadAddress(t *testing.T) {
	proxy := through(t, "otlp-plain", otlpPlain)
	direct := jaeger(t).Plain

	for _, lb := range []string{grpcx.RoundRobin, grpcx.PickFirst} {
		t.Run(lb, func(t *testing.T) {
			addresses := manual.NewBuilderWithScheme("twoaddrs")
			addresses.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: proxy.Addr}, {Addr: direct}}})

			client, conn := dial(t,
				policy("twoaddrs:///jaeger", cleartext, noRetries, func(tc *grpcx.TargetConfig) { tc.LoadBalancing = lb }),
				grpcx.WithDialOptions(grpc.WithResolvers(addresses)))
			service := testx.Unique("lb")

			// Both alive.
			for range 6 {
				require.NoError(t, export(t.Context(), t, conn, 1, service))
			}

			proxy.Down()
			t.Cleanup(proxy.Up)

			// Calls in the first moments may still be on their way to the
			// address that just died; after that, none may fail.
			eventually(t, func() error { return export(t.Context(), t, conn, 1, service) }, "the balancer settled on the live address")

			failedBefore := calls(t, client, "unavailable")

			for range 20 {
				require.NoError(t, export(t.Context(), t, conn, 1, service))
			}

			assert.InDelta(t, failedBefore, calls(t, client, "unavailable"), 0, "twenty calls, none sent to the dead address")
			assert.GreaterOrEqual(t, stored(t, service), 27)
		})
	}

	// And with nowhere to go, the answer is immediate.
	t.Run("every address dead", func(t *testing.T) {
		_, conn := dial(t, policy(proxy.Addr, cleartext, noRetries))

		proxy.Down()
		t.Cleanup(proxy.Up)

		start := time.Now()
		err := export(t.Context(), t, conn, 1, "nowhere")
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
		assert.Less(t, time.Since(start), time.Second)
		assert.NotErrorIs(t, err, grpcx.ErrCircuitOpen, "the network said so, not the breaker")
	})
}
