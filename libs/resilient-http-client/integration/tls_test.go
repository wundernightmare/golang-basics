package integration_test

// The TLS layer: what the client makes of a server it must not trust, of a
// server that will not talk to it, and of the wrong protocol on the right
// port. Every case is a real handshake against Caddy.

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
)

// over returns a client on a stdlib transport with the given TLS settings.
func over(tb testing.TB, tc resilient.TargetConfig, cfg *tls.Config) *resilient.Client {
	tb.Helper()

	transport := &http.Transport{TLSClientConfig: cfg, ForceAttemptHTTP2: true}
	tb.Cleanup(transport.CloseIdleConnections)

	return newClient(tb, tc, nil, resilient.WithHTTPClient(&http.Client{Transport: transport}))
}

// trustingTheSuiteCA builds the client a service gets: its own transport and
// the process's trust store, which holds the suite's CA.
func trustingTheSuiteCA(tc resilient.TargetConfig) func(testing.TB) *resilient.Client {
	return func(tb testing.TB) *resilient.Client {
		tb.Helper()

		return newClient(tb, tc, nil)
	}
}

// A certificate the client must not accept is refused at the handshake,
// before a byte of the request is sent, and it is refused once: retrying
// would meet the same certificate. Each refusal counts against the breaker.
func TestTLS_ACertificateTheClientRejects(t *testing.T) {
	t.Parallel()

	srv := web(t)
	retrying := policy(func(tc *resilient.TargetConfig) { tc.RetryMaxAttempts = 3 })

	cases := []struct {
		name   string
		client func(testing.TB) *resilient.Client
		url    string
		check  func(*testing.T, error)
	}{
		{
			name: "issued by an authority the client does not know",
			client: func(tb testing.TB) *resilient.Client {
				tb.Helper()

				return over(tb, retrying, &tls.Config{RootCAs: x509.NewCertPool()})
			},
			url: "https://" + srv.TLS + "/whoami",
			check: func(t *testing.T, err error) {
				t.Helper()

				var unknown x509.UnknownAuthorityError

				require.ErrorAs(t, err, &unknown)
			},
		},
		{
			name:   "issued for another name",
			client: trustingTheSuiteCA(retrying),
			url:    "https://" + strings.Replace(srv.TLS, "localhost", "127.0.0.1", 1) + "/whoami",
			check: func(t *testing.T, err error) {
				t.Helper()

				var hostname x509.HostnameError

				require.ErrorAs(t, err, &hostname)
				assert.Equal(t, "127.0.0.1", hostname.Host)
			},
		},
		{
			name:   "expired",
			client: trustingTheSuiteCA(retrying),
			url:    "https://" + srv.Expired + "/whoami",
			check: func(t *testing.T, err error) {
				t.Helper()

				var invalid x509.CertificateInvalidError

				require.ErrorAs(t, err, &invalid)
				assert.Equal(t, x509.Expired, invalid.Reason)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := tc.client(t)

			_, err := client.SendWithRetry(api, get(t, tc.url)) //nolint:bodyclose // error
			require.ErrorIs(t, err, resilient.KindConnection)
			tc.check(t, err)
			assert.False(t, resilient.Retryable(err))
			assert.InDelta(t, 1, requests(t, client, "connection"), 0, "one handshake, no retry")
			assert.LessOrEqual(t, metric(t, client, "http_client_retries_total", nil), 0.0)
		})
	}
}

// The server refuses the client: a TLS version it will not speak, a client
// certificate it requires. These arrive as alerts from the peer — the client
// cannot tell a misconfiguration from a server having a bad moment, so they
// are ordinary connection errors and are retried.
func TestTLS_TheServerRefusesTheClient(t *testing.T) {
	t.Parallel()

	srv := web(t)
	retrying := policy(func(tc *resilient.TargetConfig) { tc.RetryMaxAttempts = 3 })

	t.Run("TLS 1.2 client, TLS 1.3 server", func(t *testing.T) {
		t.Parallel()

		old := trusting()
		old.MaxVersion = tls.VersionTLS12
		client := over(t, retrying, old)

		_, err := client.SendWithRetry(api, get(t, "https://"+srv.TLS13+"/whoami")) //nolint:bodyclose // error
		require.ErrorIs(t, err, resilient.KindConnection)
		require.ErrorContains(t, err, "protocol version")
		assert.InDelta(t, 3, requests(t, client, "connection"), 0, "retried: an alert may be transient")

		// The same listener accepts a current client.
		assert.Equal(t, "tls1.3", whoami(t, newClient(t, policy(nil), nil), "https://"+srv.TLS13).TLS)
	})

	t.Run("client certificate required", func(t *testing.T) {
		t.Parallel()

		anonymous := newClient(t, retrying, nil)

		_, err := anonymous.SendWithRetry(api, get(t, "https://"+srv.MTLS+"/whoami")) //nolint:bodyclose // error
		// With TLS 1.3 the server's verdict on the client certificate comes
		// after the client's own handshake is done, so the refusal lands on
		// whatever the client does first on the connection, and the message
		// varies with it. What must not vary is the kind.
		require.ErrorIs(t, err, resilient.KindConnection)
		assert.InDelta(t, 3, requests(t, anonymous, "connection"), 0)

		identified := trusting()
		identified.Certificates = []tls.Certificate{certs.client}
		assert.Equal(t, "HTTP/2.0", whoami(t, over(t, policy(nil), identified), "https://"+srv.MTLS).Proto,
			"the suite's client certificate is accepted")
	})
}

// The right host, the wrong protocol for the port.
func TestTLS_SchemeAndPortDisagree(t *testing.T) {
	t.Parallel()

	srv := web(t)
	client := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }), nil)

	// HTTP to the TLS port: the server answers, in cleartext, that this is
	// the wrong door. An answer is an answer — a 400, and a live dependency.
	_, err := client.Send(api, get(t, "http://"+srv.TLS+"/whoami")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindStatus)

	var outbound *resilient.OutboundError

	require.ErrorAs(t, err, &outbound)
	assert.Equal(t, http.StatusBadRequest, outbound.StatusCode)
	assert.Contains(t, string(outbound.Body), "HTTPS server")
	assert.Equal(t, resilient.StateClosed, breakerState(t, client))

	// HTTPS to the cleartext port: there is no handshake to be had.
	_, err = client.Send(api, get(t, "https://"+srv.Plain+"/whoami")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindConnection)
	require.ErrorContains(t, err, "HTTP response to HTTPS client")
	assert.Equal(t, resilient.StateOpen, breakerState(t, client), "with breaker_min_requests 1 this failure opens it")
}

// The redirect policy against a real server's Location headers, over TLS:
// what the client follows and what it refuses to be sent to.
func TestTLS_Redirects(t *testing.T) {
	t.Parallel()

	srv := web(t)
	base := "https://" + srv.TLS
	client := newClient(t, policy(func(tc *resilient.TargetConfig) { tc.BreakerMinRequests = 1 }),
		func(c *resilient.Config) { c.MaxRedirects = 3 })

	resp, err := client.Send(api, get(t, base+"/redirect/same"))
	require.NoError(t, err)
	assert.Contains(t, read(t, resp), "proto=HTTP/2.0", "a same-host redirect is followed to its target")

	start := time.Now()
	_, err = client.Send(api, get(t, base+"/redirect/loop")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindRedirect)
	require.ErrorContains(t, err, "stopped after 3 redirects")
	assert.Less(t, time.Since(start), 2*time.Second)

	_, err = client.Send(api, get(t, base+"/redirect/cross")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindRedirect)
	require.ErrorContains(t, err, "cross-host redirect to elsewhere.invalid")
	require.NotErrorIs(t, err, resilient.KindConnection, "refused before resolving the other host")

	_, err = client.Send(api, get(t, base+"/redirect/downgrade")) //nolint:bodyclose // error
	require.ErrorIs(t, err, resilient.KindRedirect)
	require.ErrorContains(t, err, "https→http")

	assert.Equal(t, resilient.StateClosed, breakerState(t, client), "the server answered every time; a refused redirect is not its failure")
	assert.False(t, resilient.Retryable(err))
}

// HTTP/3 has no TCP to fall back on: a UDP port nobody listens on is a
// handshake that never completes, and the attempt timeout is what ends it.
func TestH3_NobodyListening(t *testing.T) {
	t.Parallel()

	srv := web(t)
	client := newClient(t, policy(func(tc *resilient.TargetConfig) {
		tc.Timeout = 500 * time.Millisecond
		tc.BreakerMinRequests = 1
	}), nil, h3Options(nil)(t)...)

	// The TLS 1.3 listener is TCP only: its port number is unused on UDP.
	start := time.Now()
	_, err := client.Send(api, get(t, "https://"+srv.TLS13+"/whoami")) //nolint:bodyclose // error

	var outbound *resilient.OutboundError

	require.ErrorAs(t, err, &outbound)
	assert.Contains(t, []resilient.Kind{resilient.KindTimeout, resilient.KindConnection}, outbound.Kind,
		"a timeout, or a connection error where the host answers with ICMP unreachable")
	assert.Less(t, time.Since(start), 2*time.Second, "bounded by the attempt timeout")
	assert.Equal(t, resilient.StateOpen, breakerState(t, client))
}
