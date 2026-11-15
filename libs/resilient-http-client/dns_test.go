package resilient_test

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/wundernightmare/golang-basics/libs/resilient-http-client"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

// The client has no DNS code of its own: a name is resolved by net/http's
// dialer, once per new connection. These tests say what follows from that —
// against a real name server (testx.DNS) and Go's real resolver, so the
// errors are the ones production sees.

const dnsName = "billing.dns.test"

// resolving is a net/http client that resolves through dns and nothing else.
func resolving(t *testing.T, dns *testx.DNS) resilient.Option {
	t.Helper()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Resolver: dns.Resolver()}).DialContext
	t.Cleanup(transport.CloseIdleConnections)

	return resilient.WithHTTPClient(&http.Client{Transport: transport})
}

// named is an httptest server that answers with its own name, and the URL
// that reaches it through dnsName.
func named(t *testing.T, name string) (srv *httptest.Server, url string) {
	t.Helper()

	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, name)
	}))
	t.Cleanup(srv.Close)

	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)

	return srv, "http://" + net.JoinHostPort(dnsName, port)
}

// servedBy sends one GET and returns the name of the server that answered.
func servedBy(t *testing.T, c *resilient.Client, url string) string {
	t.Helper()

	resp, err := c.Send("api", get(t, t.Context(), url))
	require.NoError(t, err)

	defer func() { require.NoError(t, resp.Body.Close()) }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}

// A name that does not resolve is a connection failure like any other: it
// is counted by the breaker and worth another attempt — the record may be
// seconds away (a rollout, a name server that just restarted).
func TestSend_ANameThatDoesNotResolveIsAConnectionFailure(t *testing.T) {
	t.Parallel()

	for name, breakDNS := range map[string]func(*testx.DNS){
		"no such host (NXDOMAIN)":          func(*testx.DNS) {},
		"the name server fails (SERVFAIL)": (*testx.DNS).Fail,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dns := testx.NewDNS(t)
			breakDNS(dns)

			c := newClient(t, target(func(tc *resilient.TargetConfig) { tc.Breaker.MinRequests = 1 }), resolving(t, dns))

			_, err := c.Send("api", get(t, t.Context(), "http://"+dnsName+":80")) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindConnection)
			assert.True(t, resilient.Retryable(err))

			var dnsErr *net.DNSError

			require.ErrorAs(t, err, &dnsErr, "the cause is kept")
			assert.Equal(t, dnsName, dnsErr.Name)

			st, _ := c.BreakerState("api")
			assert.Equal(t, resilient.StateOpen, st, "the breaker heard it")
		})
	}
}

// A name server that does not answer costs the attempt its timeout and no
// more: the resolver's own patience (seconds per try, several tries) is cut
// by the attempt's deadline.
func TestSend_ASilentNameServerIsBoundedByTheAttemptTimeout(t *testing.T) {
	t.Parallel()

	dns := testx.NewDNS(t)
	dns.Silence()

	const timeout = 100 * time.Millisecond

	c := newClient(t, target(func(tc *resilient.TargetConfig) {
		tc.Timeout = timeout
		tc.Breaker.MinRequests = 1
	}), resolving(t, dns))

	start := time.Now()
	_, err := c.Send("api", get(t, t.Context(), "http://"+dnsName+":80")) //nolint:bodyclose // error
	took := time.Since(start)

	require.ErrorIs(t, err, resilient.KindTimeout)
	assert.GreaterOrEqual(t, took, timeout)
	assert.Less(t, took, 2*time.Second, "not the resolver's own timeout")

	st, _ := c.BreakerState("api")
	assert.Equal(t, resilient.StateOpen, st)
}

// Every attempt resolves again: a record that appears, or stops pointing at
// a dead address, while the client retries is found by the next attempt.
func TestSendWithRetry_ARecordFixedBetweenAttemptsIsFound(t *testing.T) {
	t.Parallel()

	// An address nothing listens on: the listener is closed, its port kept
	// out of reuse by nothing — hence 127.0.0.2, where no test listens.
	const dead = "127.0.0.2"

	for name, before := range map[string]func(*testx.DNS){
		"the name did not exist":               func(*testx.DNS) {},
		"the record pointed at a dead address": func(dns *testx.DNS) { dns.Set(dnsName, dead) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dns := testx.NewDNS(t)
			before(dns)

			_, url := named(t, "new")

			client := newClient(t, target(func(tc *resilient.TargetConfig) {
				tc.RetryMaxAttempts = 50
				tc.RetryBudgetMinRetries = 100
				tc.Breaker.FailureRatio = 0
			}), resolving(t, dns))

			// The record is fixed once the client has demonstrably failed on
			// the old one.
			go func() {
				for dns.Queries(dnsName) < 2 {
					time.Sleep(time.Millisecond)
				}

				dns.Set(dnsName, "127.0.0.1")
			}()

			resp, err := client.SendWithRetry("api", get(t, t.Context(), url))
			require.NoError(t, err)

			defer func() { require.NoError(t, resp.Body.Close()) }()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, "new", string(body))
			assert.GreaterOrEqual(t, dns.Queries(dnsName), 3, "each attempt asked again")
		})
	}
}

// A connection that is alive never asks again: after the record moves,
// requests keep going to the old server for as long as it keeps the
// connection open. The new address is found by the next *connection* — when
// the old server closes this one, or it idles out (idle_conn_timeout).
// Under steady traffic to a server that stays up, that is never: retire the
// old server, do not wait for clients to leave it.
func TestSend_AMovedRecordIsSeenByTheNextConnection(t *testing.T) {
	t.Parallel()

	dns := testx.NewDNS(t)

	// Two servers on the same port of two loopback addresses: the record
	// decides which one a new connection reaches.
	oldSrv := sameport(t, "old", "new")

	_, port, err := net.SplitHostPort(oldSrv.Listener.Addr().String())
	require.NoError(t, err)

	url := "http://" + net.JoinHostPort(dnsName, port)

	dns.Set(dnsName, "127.0.0.1")

	client := newClient(t, target(nil), resolving(t, dns))

	require.Equal(t, "old", servedBy(t, client, url))

	asked := dns.Queries(dnsName)

	dns.Set(dnsName, "127.0.0.2")

	for range 5 {
		require.Equal(t, "old", servedBy(t, client, url), "the live connection is reused")
	}

	assert.Equal(t, asked, dns.Queries(dnsName), "and nobody asked the name server")

	oldSrv.CloseClientConnections()

	assert.Eventually(t, func() bool { return servedBy(t, client, url) == "new" }, 5*time.Second, 5*time.Millisecond,
		"the connection that replaces it resolves again")
}

// sameport starts two servers that answer with their names on one port —
// the first on 127.0.0.1, the second on 127.0.0.2 — and returns the first.
func sameport(t *testing.T, first, second string) *httptest.Server {
	t.Helper()

	serve := func(ln net.Listener, name string) *httptest.Server {
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, name)
		}))
		_ = srv.Listener.Close()
		srv.Listener = ln
		srv.Start()
		t.Cleanup(srv.Close)

		return srv
	}

	// A port free on one address may be taken on the other: try again.
	for range 20 {
		lnA, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		_, port, err := net.SplitHostPort(lnA.Addr().String())
		require.NoError(t, err)

		lnB, err := net.Listen("tcp", net.JoinHostPort("127.0.0.2", port))
		if err != nil {
			_ = lnA.Close()

			continue
		}

		serve(lnB, second)

		return serve(lnA, first)
	}

	require.FailNow(t, "no port free on both 127.0.0.1 and 127.0.0.2")

	return nil
}
