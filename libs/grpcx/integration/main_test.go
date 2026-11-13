package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// certs is the suite's PKI, created before any test (and any TLS handshake).
var certs *pki

// TestMain issues the certificates and makes the suite's CA the process's
// system trust store. SSL_CERT_FILE is read once, at the first certificate
// verification, so it is set before anything dials: that way the client's own
// transport — built by resilient.New from Config, with no test hook in it —
// trusts the test servers, and the suite exercises the transport the client
// really ships instead of one handed in through WithHTTPClient.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	var err error
	if certs, err = newPKI(); err != nil {
		fmt.Fprintln(os.Stderr, "integration: pki:", err)

		return 1
	}

	dir, err := os.MkdirTemp("", "grpcx-integration-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration: temp dir:", err)

		return 1
	}

	defer func() { _ = os.RemoveAll(dir) }()

	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, certs.caPEM, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "integration: write ca:", err)

		return 1
	}

	if err := os.Setenv("SSL_CERT_FILE", caFile); err != nil {
		fmt.Fprintln(os.Stderr, "integration: SSL_CERT_FILE:", err)

		return 1
	}

	// No system directory of certificates either: only the suite's CA.
	if err := os.Setenv("SSL_CERT_DIR", dir); err != nil {
		fmt.Fprintln(os.Stderr, "integration: SSL_CERT_DIR:", err)

		return 1
	}

	return containers.Main(m)
}
