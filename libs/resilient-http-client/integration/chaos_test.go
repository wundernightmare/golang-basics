package integration_test

// The network layer: the same real servers behind Toxiproxy, which drops,
// delays, cuts and resets the TCP connection at the point each test names —
// during the TLS handshake, before the response, in the middle of the body,
// while the connection sits idle.
//
// The tests of this file share the proxies, so none of them is parallel.

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// throughTLS returns the proxy in front of the edge's TLS listener and the
// base URL that goes through it. The certificate is for "localhost", which
// is how the proxy is addressed too.
func throughTLS(t *testing.T) (*containers.Proxy, string) {
	t.Helper()

	web(t)

	proxy := containers.ProxyTo(t, "edge-tls", containers.FixtureContainer(t, "edge"), edgeTLS)

	return proxy, "https://" + proxy.Addr
}

func throughPlain(t *testing.T) (*containers.Proxy, string) {
	t.Helper()

	web(t)

	proxy := containers.ProxyTo(t, "edge-plain", containers.FixtureContainer(t, "edge"), edgePlain)

	return proxy, "http://" + proxy.Addr
}

// The TCP connection is up and the TLS handshake fails underneath it. The
// client reports each failure for what it is, within the target's timeout —
// not net/http's ten-second handshake timeout — and the next request, once
// the network is whole, goes through on the same client.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_TLSHandshake(t *testing.T) {
	proxy, base := throughTLS(t)

	faults := []struct {
		name   string
		inject func()
		kind   resilient.Kind
		within time.Duration
	}{
		{
			name:   "ClientHello goes into a black hole",
			inject: func() { proxy.Toxic("timeout", "upstream", map[string]any{"timeout": 0}) },
			kind:   resilient.KindTimeout, within: 900 * time.Millisecond,
		},
		{
			name:   "ServerHello never comes back",
			inject: func() { proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0}) },
			kind:   resilient.KindTimeout, within: 900 * time.Millisecond,
		},
		{
			name:   "connection reset during the handshake",
			inject: func() { proxy.Toxic("reset_peer", "downstream", map[string]any{"timeout": 0}) },
			kind:   resilient.KindConnection, within: 400 * time.Millisecond,
		},
		{
			name:   "connection cut 200 bytes into the server's flight",
			inject: func() { proxy.Toxic("limit_data", "downstream", map[string]any{"bytes": 200}) },
			kind:   resilient.KindConnection, within: 400 * time.Millisecond,
		},
	}

	for _, fault := range faults {
		t.Run(fault.name, func(t *testing.T) {
			client := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.Timeout = 500 * time.Millisecond }), nil)

			fault.inject()

			start := time.Now()
			_, err := client.Send(api, get(t, base+"/whoami")) //nolint:bodyclose // error
			require.ErrorIs(t, err, fault.kind)
			assert.Less(t, time.Since(start), fault.within)
			assert.True(t, resilient.Retryable(err), "a broken handshake may succeed next time")

			proxy.Reset()

			assert.Equal(t, "HTTP/2.0", whoami(t, client, base).Proto, "no half-open connection was left in the pool")
		})
	}

	// A slow, fragmented handshake — sixteen bytes at a time — is still a
	// handshake: the record layer reassembles it.
	t.Run("handshake delivered in 16-byte pieces", func(t *testing.T) {
		client := newClient(t, policy(nil), nil)

		proxy.Toxic("slicer", "downstream", map[string]any{"average_size": 16, "size_variation": 4, "delay": 300})
		proxy.Toxic("slicer", "upstream", map[string]any{"average_size": 16, "size_variation": 4, "delay": 300})

		seen := whoami(t, client, base)
		assert.Equal(t, "HTTP/2.0", seen.Proto)
		assert.Equal(t, "tls1.3", seen.TLS)
	})
}

// A dependency that goes away and comes back, seen through the breaker: the
// failures open it, an open breaker answers without touching the network,
// and after breaker_open_timeout one probe finds the dependency alive and
// closes it.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_OutageOpensTheBreakerAndAProbeClosesIt(t *testing.T) {
	proxy, base := throughTLS(t)
	client := newClient(t, policy(func(tc *resilient.TargetConfig) {
		tc.BreakerMinRequests = 4
		tc.BreakerOpenTimeout = 400 * time.Millisecond
	}), nil)

	require.Equal(t, "HTTP/2.0", whoami(t, client, base).Proto)

	proxy.Down()

	for range 8 {
		_, err := client.Send(api, get(t, base+"/whoami")) //nolint:bodyclose // error
		require.Error(t, err)

		if breakerState(t, client) == resilient.StateOpen {
			break
		}
	}

	require.Equal(t, resilient.StateOpen, breakerState(t, client))

	attempted := requests(t, client, "connection")
	start := time.Now()

	for range 50 {
		_, err := client.Send(api, get(t, base+"/whoami")) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindCircuitOpen)
	}

	assert.Less(t, time.Since(start), 100*time.Millisecond, "fifty rejections cost no network round trip")
	assert.InDelta(t, attempted, requests(t, client, "connection"), 0, "nothing was dialled while open")
	assert.InDelta(t, 50, requests(t, client, "circuit_open"), 0)

	proxy.Up()

	require.Eventually(t, func() bool {
		resp, err := client.Send(api, get(t, base+"/whoami"))
		if err != nil {
			return false
		}

		_ = resp.Body.Close()

		return true
	}, 5*time.Second, 50*time.Millisecond, "after breaker_open_timeout a probe is let through")
	assert.Equal(t, resilient.StateClosed, breakerState(t, client))
	assert.InDelta(t, 1, metric(t, client, "http_client_circuit_breaker_transitions_total",
		map[string]string{"from": "half_open", "to": "closed"}), 0)
}

// An outage that ends while the client is still retrying: the caller sees one
// successful call. The network comes back only after the client has failed
// twice, so the success is the retries' doing.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_OutageHealsWhileTheClientRetries(t *testing.T) {
	proxy, base := throughTLS(t)
	client := newClient(t, policy(func(tc *resilient.TargetConfig) {
		tc.RetryMaxAttempts = 12
		tc.RetryBaseDelay = 100 * time.Millisecond
		tc.RetryMaxDelay = 300 * time.Millisecond
		tc.RetryBudgetMinRetries = 20
	}), nil)

	proxy.Down()

	healed := make(chan struct{})

	go func() {
		defer close(healed)

		for requests(t, client, "connection") < 2 {
			time.Sleep(2 * time.Millisecond)
		}

		proxy.Up()
	}()

	resp, err := client.SendWithRetry(api, get(t, base+"/whoami"))
	require.NoError(t, err)
	assert.Contains(t, read(t, resp), "proto=HTTP/2.0")

	<-healed
	assert.GreaterOrEqual(t, metric(t, client, "http_client_retries_total", nil), 2.0)
	assert.InDelta(t, 1, requests(t, client, "2xx"), 0)
}

// Latency on the path: under the target's timeout it is only slow, over it
// the attempt is a timeout — and the caller is never kept longer than that.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_LatencyAgainstTheAttemptTimeout(t *testing.T) {
	proxy, base := throughPlain(t)

	proxy.Latency(300 * time.Millisecond)

	patient := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.Timeout = 3 * time.Second }), nil)
	start := time.Now()

	assert.Equal(t, "HTTP/1.1", whoami(t, patient, base).Proto)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)

	hasty := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.Timeout = 100 * time.Millisecond }), nil)
	start = time.Now()
	_, err := hasty.Send(api, get(t, base+"/whoami")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindTimeout)
	assert.Less(t, time.Since(start), 280*time.Millisecond, "the caller got its answer before the network delivered one")
}

// The response starts well — status and headers arrive, the caller holds a
// 200 — and the connection dies in the middle of the body. The failure
// surfaces where the caller is: in Read, typed, and the breaker hears it
// although the call itself had "succeeded".
//
//nolint:paralleltest // serial: shares the Toxiproxy proxies in front of the edge
func TestChaos_ConnectionCutInTheMiddleOfTheBody(t *testing.T) {
	for _, via := range []struct {
		name    string
		through func(*testing.T) (*containers.Proxy, string)
	}{{"HTTP/1.1 cleartext", throughPlain}, {"HTTP/2 over TLS", throughTLS}} {
		t.Run(via.name, func(t *testing.T) {
			proxy, base := via.through(t)
			client := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }), nil)

			proxy.Toxic("limit_data", "downstream", map[string]any{"bytes": 512 << 10})

			resp, err := client.Send(api, get(t, base+"/files/blob"))
			require.NoError(t, err, "the headers made it")
			assert.Equal(t, int64(blobSize), resp.ContentLength)

			n, err := io.Copy(io.Discard, resp.Body)
			require.ErrorIs(t, err, resilient.KindConnection)
			assert.Positive(t, n)
			assert.Less(t, n, int64(blobSize), "a truncated body is an error, never a short success")
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, resilient.StateOpen, breakerState(t, client), "a response that dies in its body is the dependency failing")
			assert.InDelta(t, 1, requests(t, client, "2xx"), 0, "the request metric counted the status that arrived")
		})
	}
}

// A body that arrives too slowly: the attempt timeout covers reading it, so a
// throttled megabyte stream ends in a typed timeout at the deadline instead
// of holding the caller for as long as the bytes keep trickling in.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_ThrottledBodyRunsIntoTheAttemptTimeout(t *testing.T) {
	proxy, base := throughTLS(t)
	client := newClient(t, policy(func(tc *resilient.TargetConfig) {
		tc.Timeout = 600 * time.Millisecond
		tc.MaxConcurrent = 1
	}), nil)

	proxy.Toxic("bandwidth", "downstream", map[string]any{"rate": 256}) // KB/s: the blob needs sixteen seconds

	start := time.Now()

	resp, err := client.Send(api, get(t, base+"/files/blob"))
	require.NoError(t, err)

	n, err := io.Copy(io.Discard, resp.Body)
	require.ErrorIs(t, err, resilient.KindTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.InDelta(t, 600, time.Since(start).Milliseconds(), 300)
	assert.Less(t, n, int64(blobSize))
	require.NoError(t, resp.Body.Close())
	assert.InDelta(t, 0, metric(t, client, "http_client_bulkhead_in_flight", nil), 0)
}

// Every pooled connection is killed while the client is idle. The next
// request must simply work — on a new connection, without the caller (or the
// client's own retry loop) having to notice.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxies in front of the edge
func TestChaos_IdleConnectionsKilledBehindTheClientsBack(t *testing.T) {
	for _, via := range []struct {
		name    string
		through func(*testing.T) (*containers.Proxy, string)
	}{{"HTTP/1.1 cleartext", throughPlain}, {"HTTP/2 over TLS", throughTLS}} {
		t.Run(via.name, func(t *testing.T) {
			proxy, base := via.through(t)
			client := newClient(t, policy(nil), nil)

			before := whoami(t, client, base)

			proxy.Down() // closes every connection through the proxy
			proxy.Up()

			after := whoami(t, client, base) // a plain Send: one attempt as far as the client's policy goes
			assert.NotEqual(t, before.Port, after.Port, "a new connection")
			assert.LessOrEqual(t, metric(t, client, "http_client_retries_total", nil), 0.0)
			assert.LessOrEqual(t, requests(t, client, "connection"), 0.0, "the caller never saw the dead connection")
		})
	}
}

// An HTTP/2 connection that has gone silent — the path drops packets, nobody
// sends a FIN — looks healthy from the client's side, and a request written
// to it waits for an answer that cannot come. Without a health check only the
// attempt timeout ends that wait. http2_ping_interval is the guard: the
// client pings a connection it hears nothing from, and when the ping is not
// answered it fails the request in ping time — a connection error the caller
// can act on seconds earlier.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_HTTP2PingDetectsAConnectionThatWentSilent(t *testing.T) {
	proxy, base := throughTLS(t)

	const attemptTimeout = 3 * time.Second

	onASilentConnection := func(t *testing.T, mut func(*resilient.Config)) (time.Duration, error) {
		t.Helper()

		client := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.Timeout = attemptTimeout }), mut)
		require.Equal(t, "HTTP/2.0", whoami(t, client, base).Proto) // the connection is up and pooled

		proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0})
		proxy.Toxic("timeout", "upstream", map[string]any{"timeout": 0})

		defer proxy.Reset()

		start := time.Now()
		_, err := client.Send(api, get(t, base+"/whoami")) //nolint:bodyclose // error

		return time.Since(start), err
	}

	t.Run("with the health check", func(t *testing.T) {
		took, err := onASilentConnection(t, func(c *resilient.Config) {
			c.HTTP2PingInterval = 200 * time.Millisecond
			c.HTTP2PingTimeout = 200 * time.Millisecond
		})
		require.ErrorIs(t, err, resilient.KindConnection, "the unanswered ping condemned the connection")
		assert.Less(t, took, attemptTimeout/2, "in ping time, not at the attempt timeout")
	})

	t.Run("without it", func(t *testing.T) {
		took, err := onASilentConnection(t, func(c *resilient.Config) { c.HTTP2PingInterval = 0 })
		require.ErrorIs(t, err, resilient.KindTimeout, "nothing told the client: only its own deadline did")
		assert.GreaterOrEqual(t, took, attemptTimeout-100*time.Millisecond)
	})
}

// One HTTP/2 connection carries several requests; when it dies, all of them
// die with it — each caller gets its own typed error, promptly — and the
// client is not left with a broken connection.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of the edge
func TestChaos_HTTP2ConnectionDiesUnderConcurrentRequests(t *testing.T) {
	proxy, base := throughTLS(t)
	client := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.Timeout = 20 * time.Second }), nil)

	before := whoami(t, client, base)

	proxy.Toxic("bandwidth", "downstream", map[string]any{"rate": 64}) // every download now takes a minute

	const streams = 4

	var (
		downloads sync.WaitGroup
		errs      = make([]error, streams)
		started   sync.WaitGroup
	)

	started.Add(streams)

	for i := range streams {
		downloads.Go(func() {
			resp, err := client.Send(api, get(t, base+"/files/blob"))

			started.Done()

			if err != nil {
				errs[i] = err

				return
			}

			_, errs[i] = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		})
	}

	started.Wait() // four responses under way on the one connection

	cut := time.Now()

	proxy.Down()
	downloads.Wait()

	assert.Less(t, time.Since(cut), 2*time.Second, "nobody waited for its twenty-second timeout")

	for i, err := range errs {
		require.ErrorIsf(t, err, resilient.KindConnection, "stream %d: %v", i, err)
	}

	proxy.Reset()

	after := whoami(t, client, base)
	assert.NotEqual(t, before.Port, after.Port)
	assert.InDelta(t, 0, metric(t, client, "http_client_bulkhead_in_flight", nil)+1, 1, "nothing is still counted as in flight")
}
