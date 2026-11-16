package valkey_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// What the counters call each outcome: a tombstone is a miss, a conditional
// write is a "set", a publish is its own operation.
func TestLookupsAndCommandsAreCountedByKind(t *testing.T) {
	t.Parallel()

	cache, _, _ := fakeCache(t, valkey.Config{})
	ctx := t.Context()
	reg := prometheus.NewRegistry()
	reg.MustRegister(cache.Collectors()...)

	require.NoError(t, cache.Tombstone(ctx, "gone", time.Minute))
	_, ok, err := cache.Get(ctx, "gone")
	require.NoError(t, err)
	require.False(t, ok)

	_, err = cache.SetNX(ctx, "k", "v", time.Minute)
	require.NoError(t, err)
	_, err = cache.Replace(ctx, "k", "v", "w", time.Minute)
	require.NoError(t, err)
	require.NoError(t, cache.Publish(ctx, "events", "m"))

	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_lookups_total", map[string]string{"result": "miss"}))
	testx.NoMetric(t, reg, "cache_lookups_total", map[string]string{"result": "hit"})
	assert.Equal(t, 3.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"operation": "set"}),
		"the tombstone, the fill and the replacement")
	assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"operation": "publish"}))
	assert.Equal(t, -1.0, testx.Metric(t, reg, "cache_command_errors_total", nil))
}

// Two caches in one process: the series carry no instance label of their
// own, so each cache's collectors go through a registerer that adds one.
func TestTwoCachesRegisterOnOneRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	for _, name := range []string{"sessions", "tasks"} {
		cache, _, _ := fakeCache(t, valkey.Config{})
		require.NoError(t, cache.Set(t.Context(), "k", "v", time.Minute))

		for _, collector := range cache.Collectors() {
			require.NoError(t, prometheus.WrapRegistererWith(prometheus.Labels{"cache": name}, reg).Register(collector))
		}
	}

	for _, name := range []string{"sessions", "tasks"} {
		assert.Equal(t, 1.0, testx.Metric(t, reg, "cache_command_duration_seconds", map[string]string{"cache": name, "operation": "set"}))
	}

	testx.LintMetrics(t, reg)
}

// The readiness PING is bounded by the check itself: the caller's context
// has no deadline here, and the server never answers.
func TestReadyCheckHasATimeoutOfItsOwn(t *testing.T) {
	t.Parallel()

	cache, server, _ := fakeCache(t, valkey.Config{})
	require.NoError(t, cache.ReadyCheck()(context.Background()))

	server.goSilent(t)

	start := time.Now()
	err := cache.ReadyCheck()(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "valkey unreachable")
	assert.InDelta(t, 2*time.Second, time.Since(start), float64(500*time.Millisecond))
}

func TestClosedCacheSaysSo(t *testing.T) {
	t.Parallel()

	cache, _, _ := fakeCache(t, valkey.Config{})
	ctx := t.Context()

	// A live subscription is ended by Close, for good.
	subscribed := make(chan struct{}, 1)
	live := make(chan error, 1)

	go func() {
		live <- cache.Subscribe(ctx, "events", func(string) {}, func() { subscribed <- struct{}{} })
	}()

	<-subscribed
	cache.Close()

	select {
	case err := <-live:
		require.ErrorIs(t, err, valkey.ErrClosed)
	case <-time.After(5 * time.Second):
		require.Fail(t, "the subscription outlived Close")
	}

	_, _, err := cache.Get(ctx, "k")
	require.ErrorIs(t, err, valkey.ErrClosed)
	require.ErrorIs(t, err, valkeygo.ErrClosing, "the library's own error stays in the chain")
	require.ErrorIs(t, cache.Set(ctx, "k", "v", time.Minute), valkey.ErrClosed)
	require.ErrorIs(t, cache.Publish(ctx, "events", "m"), valkey.ErrClosed)
	require.ErrorIs(t, cache.ReadyCheck()(ctx), valkey.ErrClosed)
	require.ErrorIs(t, cache.Subscribe(ctx, "events", func(string) {}, nil), valkey.ErrClosed)

	w, hit, err := valkey.Aside(ctx, cache, "k", time.Minute, loadWidget(widget{ID: 5}))
	require.NoError(t, err, "Aside still answers from the source")
	assert.False(t, hit)
	assert.Equal(t, 5, w.ID)
}

// A subscription the server refuses (an ACL) is not retried: asking again
// changes nothing.
func TestSubscribeRefusedByTheServerEndsForGood(t *testing.T) {
	t.Parallel()

	cache, server, _ := fakeCache(t, valkey.Config{})
	server.refuseSubscribe.Store(true)

	done := make(chan error, 1)

	go func() { done <- cache.Subscribe(t.Context(), "events", func(string) {}, nil) }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "NOPERM")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Subscribe keeps retrying a refused subscription")
	}
}

// TLS from the discrete keys, against a server whose certificate only the
// configured CA file vouches for.
func TestTLSWithACAFile(t *testing.T) {
	t.Parallel()

	certificate, caFile := selfSigned(t)
	server := newFakeServer(t, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})

	cache, err := valkey.New(valkey.Config{Addr: server.addr, TLS: true, CAFile: caFile}, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	require.NoError(t, cache.Set(t.Context(), "k", "v", time.Minute))

	// The URL form takes the same CA file.
	viaURL, err := valkey.New(valkey.Config{URL: secret.New("valkeys://" + server.addr), CAFile: caFile}, nil)
	require.NoError(t, err)
	viaURL.Close()

	// Without the CA file the system roots decide, and they do not know it.
	_, err = valkey.New(valkey.Config{Addr: server.addr, TLS: true, DialTimeout: 2 * time.Second}, nil)
	require.ErrorContains(t, err, "certificate")
}

// selfSigned issues a certificate for 127.0.0.1 and writes it out as the CA
// file that verifies it.
func selfSigned(t *testing.T) (tls.Certificate, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "valkey test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, caFile
}
