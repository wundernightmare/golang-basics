package secret_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/secret"
)

func write(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "mounted")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

// authority makes a self-signed CA and returns it with its PEM encoding.
func authority(t *testing.T, name string) (*x509.Certificate, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestReadFile(t *testing.T) {
	t.Parallel()

	for name, content := range map[string]string{
		"as written":           hunter,
		"a trailing newline":   hunter + "\n",
		"a Windows line break": hunter + "\r\n",
		"several line breaks":  hunter + "\n\n",
	} {
		got, err := secret.ReadFile(write(t, content))
		require.NoError(t, err, name)
		assert.Equal(t, hunter, got.Expose(), name)
	}

	inner, err := secret.ReadFile(write(t, " a b\nc \n"))
	require.NoError(t, err)
	assert.Equal(t, " a b\nc ", inner.Expose(), "only the line breaks at the end go: a space may be part of a password")
}

func TestReadFile_RefusesAFileWithNoValue(t *testing.T) {
	t.Parallel()

	for name, content := range map[string]string{"empty": "", "only line breaks": "\r\n\n"} {
		path := write(t, content)

		got, err := secret.ReadFile(path)
		require.ErrorIs(t, err, secret.ErrEmptyFile, name)
		require.EqualError(t, err, path+" is empty", name)
		assert.True(t, got.IsZero(), name)
	}

	missing := filepath.Join(t.TempDir(), "nope")

	got, err := secret.ReadFile(missing)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, "open "+missing)
	assert.True(t, got.IsZero())
}

// The pool is the file and only the file: a certificate signed by anyone
// else — a public authority included — is not trusted through it.
func TestCertPool_TrustsWhatTheFileNamesAndNothingElse(t *testing.T) {
	t.Parallel()

	mine, minePEM := authority(t, "mine")
	other, otherPEM := authority(t, "other")
	stranger, _ := authority(t, "stranger")

	pool, err := secret.CertPool(write(t, minePEM+otherPEM))
	require.NoError(t, err)

	for _, trusted := range []*x509.Certificate{mine, other} {
		_, err := trusted.Verify(x509.VerifyOptions{Roots: pool})
		require.NoError(t, err, trusted.Subject.CommonName)
	}

	_, err = stranger.Verify(x509.VerifyOptions{Roots: pool})
	require.Error(t, err)

	system, err := x509.SystemCertPool()
	if err == nil && system != nil {
		assert.False(t, pool.Equal(system), "the system roots are not in it")
	}

	only, err := secret.CertPool(write(t, minePEM))
	require.NoError(t, err)

	want := x509.NewCertPool()
	want.AddCert(mine)
	assert.True(t, only.Equal(want), "exactly the certificates of the file")

	key := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("not a certificate")}))

	annotated, err := secret.CertPool(write(t, "# issued by mine\r\n"+minePEM+key+"trailing note\n"))
	require.NoError(t, err, "text around the blocks and a block of another type are passed over")
	assert.True(t, annotated.Equal(want))
}

// A bundle is read whole or not at all: with one of two authorities damaged
// the client would start, and fail only when the dependency moves to the
// certificate signed by the damaged one.
func TestCertPool_RefusesABundleWithADamagedBlock(t *testing.T) {
	t.Parallel()

	_, good := authority(t, "good")
	cert, _ := authority(t, "damaged")

	notDER := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: bytes.Repeat([]byte{0xff}, len(cert.Raw))}))
	cut := strings.TrimSuffix(good, "-----END CERTIFICATE-----\n")
	require.NotEqual(t, good, cut)

	for name, content := range map[string]string{
		"a block that is not a certificate": good + notDER,
		"a block that is not base64":        good + strings.Replace(good, "\n", "\n!", 1),
		"a block cut before its end":        good + cut,
		"a cut block before a whole one":    cut + good,
	} {
		path := write(t, content)

		pool, err := secret.CertPool(path)
		require.ErrorIs(t, err, secret.ErrMalformedBundle, name)
		require.ErrorContains(t, err, path+" holds a malformed PEM block", name)
		assert.Nil(t, pool, name)
	}
}

func TestCertPool_RefusesAFileWithNoCertificate(t *testing.T) {
	t.Parallel()

	for name, content := range map[string]string{"empty": "", "not PEM": "certificate goes here"} {
		path := write(t, content)

		pool, err := secret.CertPool(path)
		require.ErrorIs(t, err, secret.ErrNoCertificate, name)
		require.EqualError(t, err, path+" holds no PEM certificate", name)
		assert.Nil(t, pool, name)
	}

	missing := filepath.Join(t.TempDir(), "nope")

	pool, err := secret.CertPool(missing)
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, pool)
}
