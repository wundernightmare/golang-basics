package secret

import (
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrEmptyFile is returned by [ReadFile] for a file with no value in it.
var ErrEmptyFile = errors.New("is empty")

// ErrNoCertificate is returned by [CertPool] for a file with no PEM
// certificate in it.
var ErrNoCertificate = errors.New("holds no PEM certificate")

// ReadFile reads a mounted secret: the whole file, without the trailing line
// break an editor or `echo` leaves. A file that cannot be read or holds
// nothing is an error — a secret that silently became "" would surface later
// as a confusing authentication failure. The error names the path, never the
// content.
func ReadFile(path string) (String, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: the path is operator-supplied configuration, not user input
	if err != nil {
		return String{}, err // unwrapped: an *os.PathError already says "open <path>: <cause>"; the caller adds the key
	}

	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" {
		return String{}, fmt.Errorf("%s %w", path, ErrEmptyFile)
	}

	return New(value), nil
}

// CertPool reads the other file a pod mounts next to its secrets: a PEM
// bundle of the certificate authorities that sign a dependency's certificate.
//
// The pool holds those certificates and nothing else. A CA file is a
// statement of whom to trust for that dependency — the system roots are what
// a client uses when there is no such file, not something the file adds to:
// that is what librdkafka's ssl.ca.location and the OpenTelemetry SDK's
// OTEL_EXPORTER_OTLP_CERTIFICATE mean, so every client configured from this
// repository means it too.
func CertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) //nolint:gosec // G304: the path is operator-supplied configuration, not user input
	if err != nil {
		return nil, err // unwrapped, as in ReadFile: the *os.PathError names the path, the caller adds the key
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s %w", path, ErrNoCertificate)
	}

	return pool, nil
}
