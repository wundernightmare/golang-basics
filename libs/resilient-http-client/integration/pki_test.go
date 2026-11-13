package integration_test

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
	"time"
)

// pki is the suite's certificate authority and what it issued. Generated per
// run: nothing here is a secret, and nothing is committed.
type pki struct {
	caPEM []byte // the CA certificate: what a client must trust

	serverCert, serverKey   []byte // CN localhost; SAN: localhost, host.docker.internal — no IP on purpose
	expiredCert, expiredKey []byte // the same names, expired yesterday
	clientCert, clientKey   []byte // for the mTLS listener

	client tls.Certificate
}

func newPKI() (*pki, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ca key: %w", err)
	}

	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "resilient-http-client integration CA"},
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

	out := &pki{caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}

	issue := func(serial int64, mut func(*x509.Certificate)) (certPEM, keyPEM []byte, err error) {
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
		mut(template)

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

	server := func(c *x509.Certificate) {
		c.DNSNames = []string{"localhost", "host.docker.internal"}
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	if out.serverCert, out.serverKey, err = issue(2, server); err != nil {
		return nil, err
	}

	if out.expiredCert, out.expiredKey, err = issue(3, func(c *x509.Certificate) {
		server(c)

		c.NotBefore, c.NotAfter = now.Add(-48*time.Hour), now.Add(-24*time.Hour)
	}); err != nil {
		return nil, err
	}

	if out.clientCert, out.clientKey, err = issue(4, func(c *x509.Certificate) {
		c.Subject = pkix.Name{CommonName: "integration-client"}
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}); err != nil {
		return nil, err
	}

	if out.client, err = tls.X509KeyPair(out.clientCert, out.clientKey); err != nil {
		return nil, fmt.Errorf("client key pair: %w", err)
	}

	return out, nil
}
