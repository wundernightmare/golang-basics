package integration_test

// The TLS layer: a real server's certificates and a real server's refusals,
// through grpc-go's handshake.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/tracehubmmp/golang-basics/libs/grpcx"
	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// A handshake that cannot succeed — the client will not trust the server, or
// the server will not have the client. gRPC reports all of them as
// Unavailable; the cause is in the message. The call fails promptly, the
// server never sees a request, and the breaker takes the target out.
func TestTLS_HandshakeThatCannotSucceed(t *testing.T) {
	t.Parallel()

	srv := jaeger(t)

	cases := []struct {
		name, addr, cause string
		opts              []grpcx.Option
	}{
		{
			name: "server certificate from an unknown authority", addr: srv.TLS,
			cause: "unknown authority",
			opts: []grpcx.Option{grpcx.WithDialOptions(grpc.WithTransportCredentials(
				credentials.NewTLS(&tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12})))},
		},
		{
			name: "server certificate for another name",
			addr: strings.Replace(srv.TLS, "localhost", "127.0.0.1", 1), cause: "doesn't contain any IP SANs",
		},
		{name: "server certificate expired", addr: srv.Expired, cause: "expired"},
		{
			name: "client too old for the server", addr: srv.TLS13, cause: "protocol version",
			opts: []grpcx.Option{trusting(func(c *tls.Config) { c.MaxVersion = tls.VersionTLS12 })},
		},
		{name: "client certificate required", addr: srv.MTLS, cause: "certificate required"},
		{name: "TLS to the cleartext port", addr: srv.Plain, cause: "first record does not look like a TLS handshake"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, conn := dial(t, policy(tc.addr, touchyBreaker), tc.opts...)
			service := testx.Unique("refused")

			start := time.Now()
			err := export(t.Context(), t, conn, 3, service)
			require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			require.ErrorContains(t, err, tc.cause)
			assert.Less(t, time.Since(start), 3*time.Second, "refused at the handshake, not at the deadline")
			assert.Equal(t, resilience.StateOpen, breaker(t, client))
			assert.InDelta(t, 1, calls(t, client, "unavailable"), 0, "one call, however many attempts gRPC made below")
			assert.Zero(t, stored(t, service), "no request reached the server")
		})
	}
}

// Cleartext to a TLS port: the server is waiting for a ClientHello and gets
// an HTTP/2 preface. No silent downgrade in either direction.
func TestTLS_CleartextToTheTLSPort(t *testing.T) {
	t.Parallel()

	client, conn := dial(t, policy(jaeger(t).TLS, cleartext, touchyBreaker))

	start := time.Now()
	err := export(t.Context(), t, conn, 3, "tls")
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	assert.Less(t, time.Since(start), 3*time.Second)
	assert.Equal(t, resilience.StateOpen, breaker(t, client))
}

// wait_for_ready turns "the connection is down" from an immediate failure
// into waiting. With a handshake that can never succeed the wait can only end
// at the deadline — so a deadline is what such a target must have — and the
// error still names the real cause, not just "deadline exceeded".
func TestTLS_WaitForReadyOnAHandshakeThatCannotSucceed(t *testing.T) {
	t.Parallel()

	client, conn := dial(t, policy(jaeger(t).Expired, touchyBreaker, timeout(700*time.Millisecond),
		func(tc *grpcx.TargetConfig) { tc.WaitForReady = true }))

	start := time.Now()
	err := export(context.Background(), t, conn, 3, "tls")
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
	assert.InDelta(t, 700, time.Since(start).Milliseconds(), 400, "it waited for the target's timeout, no longer")
	require.ErrorContains(t, err, "expired", "the last connection error is in the message")
	assert.Equal(t, resilience.StateOpen, breaker(t, client))
}
