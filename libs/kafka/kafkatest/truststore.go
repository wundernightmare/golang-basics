package kafkatest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
)

// RedpandaImage is the broker [TrustStore] runs: Redpanda, because one
// container gives a TLS listener with a certificate of the test's own and
// SASL/SCRAM accounts, which the Kafka fixture of libs/testx/containers
// (plaintext) does not.
const RedpandaImage = "redpandadata/redpanda:v26.2.3"

// Credentials of the account [TrustStore] connects with.
const (
	trustUser     = "app"
	trustPassword = "app-secret-for-tests"
)

// secured is how a test reaches the TLS broker.
type secured struct {
	broker string
	caPEM  []byte // the authority that signed the broker's certificate
}

type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newAuthority(name string) (authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return authority{}, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return authority{}, err
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return authority{}, err
	}

	return authority{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// serverPair issues the broker's certificate: valid for the names a test on
// the host dials the mapped port by.
func (a authority) serverPair() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "redpanda"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost", "redpanda"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, nil, err
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

// securedBroker starts the package's TLS broker once: a private CA, a
// certificate it signed, SASL/SCRAM-SHA-256 with one account.
func securedBroker(tb testing.TB) secured {
	tb.Helper()

	return containers.Fixture(tb, "redpanda-tls", func(ctx context.Context) (secured, testcontainers.Container, error) {
		issuer, err := newAuthority("kafkatest private CA")
		if err != nil {
			return secured{}, nil, err
		}

		cert, key, err := issuer.serverPair()
		if err != nil {
			return secured{}, nil, err
		}

		ctr, err := redpanda.Run(ctx, RedpandaImage,
			redpanda.WithTLS(cert, key),
			redpanda.WithEnableSASL(),
			redpanda.WithNewServiceAccount(trustUser, trustPassword),
			redpanda.WithSuperusers(trustUser),
			redpanda.WithAutoCreateTopics(),
		)
		if err != nil {
			return secured{}, ctr, err
		}

		broker, err := ctr.KafkaSeedBroker(ctx)

		return secured{broker: broker, caPEM: issuer.pem}, ctr, err
	})
}

// TrustStore pins what KAFKA_SSL_CA_LOCATION means, against a broker whose
// certificate is signed by a private CA (Redpanda, SASL_SSL):
//
//   - with that CA's file, a record goes out and comes back;
//   - without a file the client has the system roots, which do not know the
//     broker: no connection;
//   - with the file of another CA: no connection — the file is the trust
//     store, a certificate it does not vouch for is refused.
//
// The password comes from a mounted file, as in a pod.
func TrustStore(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("needs a broker container")
	}

	if addr := os.Getenv(trustChildEnv); addr != "" {
		trustChild(t, addr)

		return
	}

	broker := securedBroker(t)
	dir := t.TempDir()

	write := func(name string, content []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, content, 0o600))

		return path
	}

	stranger, err := newAuthority("somebody else's CA")
	require.NoError(t, err)

	password := write("app.password", []byte(trustPassword+"\n"))
	base := func() kafka.Config { return trustConfig(broker.broker, password) }

	t.Run("the CA file is trusted", func(t *testing.T) {
		t.Parallel()

		cfg := base()
		cfg.SSLCALocation = write("ca.crt", broker.caPEM)

		producer, err := kafka.NewProducer(t.Context(), cfg, nil)
		require.NoError(t, err, "connect over SASL_SSL with the private CA")

		defer producer.Close()

		want := testx.Unique("trusted-record")
		require.NoError(t, producer.Send(t.Context(), kafka.Message{Value: []byte(want)}))

		consumer, err := kafka.NewConsumer(t.Context(), cfg, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()

		var got string

		require.NoError(t, consumer.Run(ctx, func(_ context.Context, m kafka.Message) error {
			got = string(m.Value)

			cancel()

			return nil
		}))
		assert.Equal(t, want, got)
	})

	for name, caFile := range map[string]string{
		"the system roots do not know the broker": "",
		"another CA's file does not vouch for it": write("stranger.crt", stranger.pem),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := base()
			cfg.SSLCALocation = caFile

			err := delivers(t, cfg)
			require.Error(t, err, "a broker certificate the trust store does not cover is refused")
			t.Logf("refused with: %v", err)
		})
	}

	// The case that tells "the file is the trust store" from "the file is
	// added to the system roots": the broker's CA is a system root, and the
	// file names somebody else. The system roots are read once per process
	// from the environment, hence a child process.
	t.Run("a CA file replaces the system roots", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()

		top, _, _ := strings.Cut(t.Name(), "/")
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+top+"$", "-test.count=1", "-test.v") //nolint:gosec // G204: the test binary re-runs itself

		child.Env = append(os.Environ(),
			trustChildEnv+"="+broker.broker,
			trustChildDirEnv+"="+dir,
			"SSL_CERT_FILE="+write("system-root.crt", broker.caPEM),
			"SSL_CERT_DIR="+t.TempDir(),
		)

		out, err := child.CombinedOutput()
		require.NoError(t, err, "the child run:\n%s", out)

		if !strings.Contains(string(out), trustMarkSystem+"delivered") {
			t.Skipf("this provider does not take its system roots from SSL_CERT_FILE, so the case cannot be built:\n%s", out)
		}

		assert.Contains(t, string(out), trustMarkFile+"refused",
			"with a CA file the system roots no longer vouch for the broker:\n%s", out)
	})
}

// Environment of the child run of [TrustStore], and the lines it reports
// with.
const (
	trustChildEnv    = "KAFKATEST_TRUST_BROKER"
	trustChildDirEnv = "KAFKATEST_TRUST_DIR"
	trustMarkSystem  = "kafkatest-trust: system roots only: "
	trustMarkFile    = "kafkatest-trust: another CA's file: "
)

// trustChild is [TrustStore] in the child process, whose system roots are
// the broker's CA: it reports whether a record is delivered without a CA
// file, and with the file of another CA.
func trustChild(t *testing.T, addr string) {
	t.Helper()

	dir := os.Getenv(trustChildDirEnv)
	verdict := func(err error) string {
		if err != nil {
			return "refused (" + err.Error() + ")"
		}

		return "delivered"
	}

	cfg := trustConfig(addr, filepath.Join(dir, "app.password"))
	t.Log(trustMarkSystem + verdict(delivers(t, cfg)))

	cfg = trustConfig(addr, filepath.Join(dir, "app.password"))
	cfg.SSLCALocation = filepath.Join(dir, "stranger.crt")
	t.Log(trustMarkFile + verdict(delivers(t, cfg)))
}

// trustConfig is the client configuration of [TrustStore], without a CA.
func trustConfig(broker, passwordFile string) kafka.Config {
	return kafka.Config{
		Brokers:          []string{broker},
		Topic:            testx.Unique("trust"),
		Group:            testx.Unique("trust"),
		ClientID:         testx.Unique("trust"),
		SecurityProtocol: "sasl_ssl",
		SASLMechanism:    "SCRAM-SHA-256",
		SASLUsername:     trustUser,
		SASLPasswordFile: passwordFile,
		AutoOffsetReset:  "earliest",
		DialTimeout:      3 * time.Second, PublishTimeout: 5 * time.Second,
		AllowAutoTopicCreation: true,
		RetryBackoff:           10 * time.Millisecond, RetryBackoffMax: 50 * time.Millisecond,
	}
}

// delivers reports whether one record can be sent with cfg: the error of
// building the producer (it connects) or of the send.
func delivers(t *testing.T, cfg kafka.Config) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	producer, err := kafka.NewProducer(ctx, cfg, nil)
	if err != nil {
		return err
	}

	defer producer.Close()

	return producer.Send(ctx, kafka.Message{Value: []byte("must not arrive")})
}
