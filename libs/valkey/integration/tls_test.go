package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/wundernightmare/golang-basics/libs/testx/containers"
	"github.com/wundernightmare/golang-basics/libs/valkey"
)

type tlsValkey struct {
	Addr string
	PKI  *containers.PKI
}

func valkeyWithMTLS(tb testing.TB) tlsValkey {
	tb.Helper()

	return containers.Fixture(tb, "valkey-mtls", func(ctx context.Context) (tlsValkey, testcontainers.Container, error) {
		pki, err := containers.NewPKI()
		if err != nil {
			return tlsValkey{}, nil, err
		}

		file := func(path string, content []byte) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader(content), ContainerFilePath: path, FileMode: 0o600}
		}

		ctr, err := testcontainers.Run(ctx, containers.ValkeyImage,
			testcontainers.WithExposedPorts("6379/tcp"),
			testcontainers.WithEntrypoint("valkey-server"),
			testcontainers.WithCmdArgs(
				"--port", "0", "--tls-port", "6379",
				"--tls-cert-file", "/tls/server.crt", "--tls-key-file", "/tls/server.key",
				"--tls-ca-cert-file", "/tls/ca.crt", "--tls-auth-clients", "yes",
			),
			testcontainers.WithFiles(
				file("/tls/ca.crt", pki.CA), file("/tls/server.crt", pki.ServerCert), file("/tls/server.key", pki.ServerKey),
			),
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForLog("Ready to accept connections"),
				wait.ForListeningPort("6379/tcp").SkipInternalCheck(),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return tlsValkey{}, ctr, err
		}

		addr, err := ctr.PortEndpoint(ctx, "6379/tcp", "")

		return tlsValkey{Addr: addr, PKI: pki}, ctr, err
	})
}

func TestMTLSWithARealValkey(t *testing.T) {
	t.Parallel()

	srv := valkeyWithMTLS(t)
	caFile, certFile, keyFile := srv.PKI.ClientFiles(t)
	cfg := valkey.Config{Addr: srv.Addr, TLS: true, CAFile: caFile, CertFile: certFile, KeyFile: keyFile}

	cache, err := valkey.New(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	require.NoError(t, cache.Set(t.Context(), "mtls", "works", time.Minute))
	got, ok, err := cache.Get(t.Context(), "mtls")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "works", got)

	cfg.CertFile, cfg.KeyFile, cfg.DialTimeout = "", "", 2*time.Second
	_, err = valkey.New(cfg, nil)
	require.Error(t, err, "the server requires a client certificate")
}
