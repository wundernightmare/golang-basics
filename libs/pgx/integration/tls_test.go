package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

type tlsPostgres struct {
	Port int
	PKI  *containers.PKI
}

func postgresWithTLS(tb testing.TB) tlsPostgres {
	tb.Helper()

	return containers.Fixture(tb, "postgres-tls", func(ctx context.Context) (tlsPostgres, testcontainers.Container, error) {
		pki, err := containers.NewPKI()
		if err != nil {
			return tlsPostgres{}, nil, err
		}

		file := func(path string, content []byte, mode int64) testcontainers.ContainerFile {
			return testcontainers.ContainerFile{Reader: bytes.NewReader(content), ContainerFilePath: path, FileMode: mode}
		}
		start := []byte(`#!/bin/sh
set -eu
chown postgres:postgres /certs/server.key
exec docker-entrypoint.sh "$@"
`)

		ctr, err := tcpostgres.Run(ctx, containers.PostgresImage,
			tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
			testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql": "rw"}),
			testcontainers.WithEntrypoint("sh", "/certs/start.sh"),
			testcontainers.WithCmdArgs("-c", "ssl=on", "-c", "ssl_cert_file=/certs/server.crt", "-c", "ssl_key_file=/certs/server.key"),
			testcontainers.WithFiles(
				file("/certs/start.sh", start, 0o700),
				file("/certs/server.crt", pki.ServerCert, 0o644),
				file("/certs/server.key", pki.ServerKey, 0o600),
			),
			testcontainers.WithWaitStrategy(wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeoutDefault(60*time.Second)))
		if err != nil {
			return tlsPostgres{}, ctr, err
		}

		port, err := ctr.MappedPort(ctx, "5432/tcp")

		return tlsPostgres{Port: int(port.Num()), PKI: pki}, ctr, err
	})
}

func TestCAFileVerifiesAPostgresTLSCertificate(t *testing.T) {
	t.Parallel()

	srv := postgresWithTLS(t)
	caFile, _, _ := srv.PKI.ClientFiles(t)
	cfg := pgx.Config{
		Host: "localhost", Port: srv.Port, User: "app", Password: secret.New("app"), Name: "app",
		SSLMode: "verify-full", CAFile: caFile, DialTimeout: 5 * time.Second, MaxConns: 1,
	}

	db := newDB(t, cfg)

	var one int
	require.NoError(t, db.Pool().QueryRow(t.Context(), "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)

	cfg.CAFile = ""
	_, err := pgx.New(context.Background(), cfg, nil)
	require.ErrorContains(t, err, "certificate")
}
