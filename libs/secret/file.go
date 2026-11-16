package secret

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
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

// ErrMalformedBundle is returned by [CertPool] for a file in which a PEM
// block is damaged: cut short, not base64, or a CERTIFICATE that does not
// parse.
var ErrMalformedBundle = errors.New("holds a malformed PEM block")

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
//
// The file is read whole or not at all. A damaged block is
// [ErrMalformedBundle] even when other certificates in the file are sound: a
// bundle that holds the outgoing and the incoming authority would otherwise
// load with one of them, and fail when the dependency switches to the other.
// Text outside the blocks and blocks of another type (a private key) are
// passed over; a file with no certificate at all is [ErrNoCertificate]. A
// certificate is not required to be an authority, so a self-signed leaf can
// be pinned.
func CertPool(path string) (*x509.CertPool, error) {
	rest, err := os.ReadFile(path) //nolint:gosec // G304: the path is operator-supplied configuration, not user input
	if err != nil {
		return nil, err // unwrapped, as in ReadFile: the *os.PathError names the path, the caller adds the key
	}

	// pem.Decode skips a block it cannot decode and goes on to the next
	// (TestCertPool_RefusesABundleWithADamagedBlock), so a damaged one shows
	// only as a BEGIN line that produced no block.
	opened := bytes.Count(append([]byte("\n"), rest...), []byte("\n-----BEGIN "))
	pool, decoded, found := x509.NewCertPool(), 0, false

	for {
		var block *pem.Block

		if block, rest = pem.Decode(rest); block == nil {
			break
		}

		decoded++

		if block.Type != "CERTIFICATE" {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%s %w: %w", path, ErrMalformedBundle, err)
		}

		pool.AddCert(cert)

		found = true
	}

	if decoded != opened {
		return nil, fmt.Errorf("%s %w", path, ErrMalformedBundle)
	}

	if !found {
		return nil, fmt.Errorf("%s %w", path, ErrNoCertificate)
	}

	return pool, nil
}
