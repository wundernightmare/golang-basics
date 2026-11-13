package integration_test

// Real peers that are not a well-behaved HTTP server: a broker whose answer
// gets lost on the way back, and a database on the port the client was told
// to speak HTTP to.

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// The request reaches Redpanda and is stored; the acknowledgement never comes
// back. The client reports a timeout — and what it does next decides how many
// records the topic ends up with:
//
//   - a POST is sent once. One record. The caller got an error for a write
//     that happened: only the caller can decide what that means.
//   - SendWithRetry does not change that: without an Idempotency-Key a POST
//     is not retried.
//   - with an Idempotency-Key the client retries — and this server stores
//     every attempt. The header is the caller's promise that the server
//     deduplicates; the client cannot check it, and a broken promise is
//     duplicates.
//
//nolint:paralleltest // serial: shares the Toxiproxy proxy in front of Redpanda
func TestChaos_LostAcknowledgement(t *testing.T) {
	rp := redpanda(t)
	_ = rp

	proxy := containers.ProxyTo(t, "panda", containers.FixtureContainer(t, "redpanda"), pandaProxy)
	base := "http://" + proxy.Addr

	once, unkeyed, keyed := topic(t), topic(t), topic(t)

	client := newClient(t, policy(func(tc *resilient.TargetConfig) {
		tc.Timeout = 700 * time.Millisecond
		tc.RetryMaxAttempts = 3
	}), nil)

	// Warm up through the proxy: the topics exist and the path works.
	resp, err := client.Send(api, poll(t, t.Context(), base, once, 0, 0))
	require.NoError(t, err)
	assert.Equal(t, "[]", read(t, resp))

	proxy.Toxic("timeout", "downstream", map[string]any{"timeout": 0}) // answers are dropped

	_, err = client.Send(api, produce(t, t.Context(), base, once, "sent-once")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindTimeout)

	_, err = client.SendWithRetry(api, produce(t, t.Context(), base, unkeyed, "no-key")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindTimeout)

	withKey := produce(t, t.Context(), base, keyed, "with-key")
	withKey.Header.Set("Idempotency-Key", "order-42")

	_, err = client.SendWithRetry(api, withKey) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindTimeout)
	assert.InDelta(t, 2, metric(t, client, "http_client_retries_total", nil), 0, "only the keyed request was retried")

	proxy.Reset()

	// Ground truth, read from the broker on a direct connection.
	assert.Equal(t, []string{"sent-once"}, stored(t, once), "the write happened although the client saw a timeout")
	assert.Equal(t, []string{"no-key"}, stored(t, unkeyed), "a POST without a key is never sent twice")
	assert.Equal(t, []string{"with-key", "with-key", "with-key"}, stored(t, keyed),
		"three attempts, three records: this server does not honour Idempotency-Key")
}

// PostgreSQL on the other end of an http:// (and an https://) URL — a wrong
// port in a config, a load balancer pointing at the wrong pool. The server
// reads the request as a startup packet, finds it absurd and hangs up. The
// client must come out with a typed connection error, quickly, every time,
// and its breaker must take the target out.
func TestNonHTTPPeer_Postgres(t *testing.T) {
	t.Parallel()

	dsn, err := url.Parse(containers.Postgres(t))
	require.NoError(t, err)

	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()

			client := newClient(t, policy(func(tc *resilient.TargetConfig) {
				tc.RetryMaxAttempts = 3
				tc.BreakerMinRequests = 3
				tc.Timeout = 2 * time.Second
			}), nil)

			start := time.Now()
			_, err := client.SendWithRetry(api, get(t, scheme+"://"+dsn.Host+"/")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindConnection)
			assert.Less(t, time.Since(start), time.Second, "the database hangs up at once; nothing waits for the timeout")
			assert.InDelta(t, 3, requests(t, client, "connection"), 0, "retried as a connection error")
			assert.Equal(t, resilient.StateOpen, breakerState(t, client))

			_, err = client.Send(api, get(t, scheme+"://"+dsn.Host+"/")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindCircuitOpen, "and then left alone")
		})
	}
}
