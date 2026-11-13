package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resilient "github.com/tracehubmmp/golang-basics/libs/resilient-http-client"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// api is the one target every test client declares.
const api = "api"

// policy returns the target's defaults with short retry delays, changed by mut.
func policy(mut func(*resilient.TargetConfig)) resilient.TargetConfig {
	tc := resilient.DefaultTarget(api)
	tc.RetryBaseDelay = 5 * time.Millisecond
	tc.RetryMaxDelay = 50 * time.Millisecond

	if mut != nil {
		mut(&tc)
	}

	return tc
}

// newClient builds a client over tc, shut down when the test ends. Without
// options it is the client a service gets: its own transport, built from
// [resilient.Config].
func newClient(tb testing.TB, tc resilient.TargetConfig, mut func(*resilient.Config), opts ...resilient.Option) *resilient.Client {
	tb.Helper()

	cfg := resilient.DefaultConfig()
	cfg.Targets = []resilient.TargetConfig{tc}

	if mut != nil {
		mut(&cfg)
	}

	client, err := resilient.New(cfg, opts...)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		assert.NoError(tb, client.Shutdown(ctx), "a test must leave no request in flight")
	})

	return client
}

// wire is one way of talking to the edge: a protocol, with or without TLS.
type wire struct {
	name  string
	proto string // what the server must report having spoken
	tls   bool
	// base is the URL prefix for an address of the edge.
	base func(e edge) string
	// options make the client speak exactly this protocol; nil is the
	// client's own transport.
	options func(tb testing.TB) []resilient.Option
}

// transportFor returns a WithHTTPClient option over a stdlib transport
// restricted to the given protocols.
func transportFor(set func(*http.Protocols), tlsConfig *tls.Config) func(testing.TB) []resilient.Option {
	return func(tb testing.TB) []resilient.Option {
		tb.Helper()

		protocols := new(http.Protocols)
		set(protocols)

		transport := &http.Transport{Protocols: protocols, TLSClientConfig: tlsConfig}
		tb.Cleanup(transport.CloseIdleConnections)

		return []resilient.Option{resilient.WithHTTPClient(&http.Client{Transport: transport})}
	}
}

// h3Options make the client speak HTTP/3: the policy layer over a transport
// that is not net/http's at all.
func h3Options(tlsConfig *tls.Config) func(testing.TB) []resilient.Option {
	return func(tb testing.TB) []resilient.Option {
		tb.Helper()

		transport := &http3.Transport{TLSClientConfig: tlsConfig}

		tb.Cleanup(func() { _ = transport.Close() })

		return []resilient.Option{resilient.WithHTTPClient(&http.Client{Transport: transport})}
	}
}

// wires is every protocol the edge serves. "h2 (own transport)" and "h1
// cleartext" go through the transport resilient.New builds; the others pin a
// protocol the default negotiation would not pick.
func wires() []wire {
	plain := func(e edge) string { return "http://" + e.Plain }
	secure := func(e edge) string { return "https://" + e.TLS }

	return []wire{
		{name: "h1 cleartext (own transport)", proto: "HTTP/1.1", base: plain},
		{
			name: "h2c prior knowledge", proto: "HTTP/2.0", base: plain,
			options: transportFor(func(p *http.Protocols) { p.SetUnencryptedHTTP2(true) }, nil),
		},
		{
			name: "h1 over TLS", proto: "HTTP/1.1", tls: true, base: secure,
			options: transportFor(func(p *http.Protocols) { p.SetHTTP1(true) }, nil),
		},
		{name: "h2 over TLS (own transport)", proto: "HTTP/2.0", tls: true, base: secure},
		{
			name: "h3", proto: "HTTP/3.0", tls: true, base: func(e edge) string { return "https://" + e.QUIC },
			options: h3Options(nil),
		},
	}
}

// client builds the wire's client over tc.
func (w wire) client(tb testing.TB, tc resilient.TargetConfig) *resilient.Client {
	tb.Helper()

	var opts []resilient.Option
	if w.options != nil {
		opts = w.options(tb)
	}

	return newClient(tb, tc, nil, opts...)
}

func request(tb testing.TB, ctx context.Context, method, url string, body io.Reader) *http.Request {
	tb.Helper()

	if body == nil {
		body = http.NoBody
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	require.NoError(tb, err)

	return req
}

func get(tb testing.TB, url string) *http.Request {
	tb.Helper()

	return request(tb, tb.Context(), http.MethodGet, url, nil)
}

// read drains and closes a response the test expects to be whole.
func read(tb testing.TB, resp *http.Response) string {
	tb.Helper()

	body, err := io.ReadAll(resp.Body)
	require.NoError(tb, err)
	require.NoError(tb, resp.Body.Close())

	return string(body)
}

// seen is what /whoami says the server saw of a request.
type seen struct {
	Proto, TLS, Port string
	Resumed          bool
}

var whoamiLine = regexp.MustCompile(`^proto=(\S+) tls=(\S+) resumed=(\S+) port=(\d+)$`)

// whoami asks the server what it saw: the protocol it spoke and the client
// port, which identifies the connection the request arrived on.
func whoami(tb testing.TB, client *resilient.Client, base string) seen {
	tb.Helper()

	resp, err := client.Send(api, get(tb, base+"/whoami"))
	require.NoError(tb, err)

	line := read(tb, resp)
	m := whoamiLine.FindStringSubmatch(line)
	require.NotNilf(tb, m, "unexpected /whoami answer %q", line)

	return seen{Proto: m[1], TLS: m[2], Resumed: m[3] == "true", Port: m[4]}
}

func metric(tb testing.TB, client *resilient.Client, name string, labels map[string]string) float64 {
	tb.Helper()

	return testx.Metric(tb, client.Registry(), name, labels)
}

func requests(tb testing.TB, client *resilient.Client, outcome string) float64 {
	tb.Helper()

	return metric(tb, client, "http_client_requests_total", map[string]string{"outcome": outcome})
}

func breakerState(tb testing.TB, client *resilient.Client) resilient.BreakerState {
	tb.Helper()

	state, err := client.BreakerState(api)
	require.NoError(tb, err)

	return state
}

// trusting returns a TLS config that trusts the suite's CA, for transports
// the tests build themselves.
func trusting() *tls.Config {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certs.caPEM)

	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}
