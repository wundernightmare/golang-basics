package containers

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// PKI is a short-lived certificate authority and the server, expired-server
// and client certificates it issued. Certificates are generated for each test
// run; nothing is committed or reused between processes.
type PKI struct {
	CA                      []byte
	ServerCert, ServerKey   []byte
	ExpiredCert, ExpiredKey []byte
	ClientCert, ClientKey   []byte
	Client                  tls.Certificate
}

// NewPKI issues certificates for localhost and host.docker.internal, covering
// both host-to-container and container-to-host connections in the integration
// fixtures.
func NewPKI() (*PKI, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ca key: %w", err)
	}

	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "integration CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("ca certificate: %w", err)
	}

	out := &PKI{CA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}
	issue := func(serial int64, client bool, expired bool) (certPEM, keyPEM []byte, err error) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("leaf key: %w", err)
		}

		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "localhost"},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
		}
		if client {
			template.Subject.CommonName = "integration-client"
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		} else {
			template.DNSNames = []string{"localhost", "host.docker.internal"}
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}

		if expired {
			template.NotBefore, template.NotAfter = now.Add(-48*time.Hour), now.Add(-24*time.Hour)
		}

		der, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
		if err != nil {
			return nil, nil, fmt.Errorf("leaf certificate: %w", err)
		}

		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, nil, fmt.Errorf("leaf key encoding: %w", err)
		}

		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
	}

	if out.ServerCert, out.ServerKey, err = issue(2, false, false); err != nil {
		return nil, err
	}

	if out.ExpiredCert, out.ExpiredKey, err = issue(3, false, true); err != nil {
		return nil, err
	}

	if out.ClientCert, out.ClientKey, err = issue(4, true, false); err != nil {
		return nil, err
	}

	if out.Client, err = tls.X509KeyPair(out.ClientCert, out.ClientKey); err != nil {
		return nil, fmt.Errorf("client key pair: %w", err)
	}

	return out, nil
}

// ClientFiles writes the CA and client key pair under a test-owned temporary
// directory and returns their paths in that order.
func (p *PKI) ClientFiles(tb testing.TB) (caFile, certFile, keyFile string) {
	tb.Helper()

	dir := tb.TempDir()
	for _, file := range []struct {
		name string
		data []byte
	}{
		{"ca.pem", p.CA},
		{"client.pem", p.ClientCert},
		{"client.key", p.ClientKey},
	} {
		require.NoError(tb, os.WriteFile(filepath.Join(dir, file.name), file.data, 0o600))
	}

	return filepath.Join(dir, "ca.pem"), filepath.Join(dir, "client.pem"), filepath.Join(dir, "client.key")
}
