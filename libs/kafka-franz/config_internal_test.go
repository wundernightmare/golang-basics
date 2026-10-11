package franz

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// Every named buffer key reaches its franz-go option: nothing is left to
// the client's defaults (unbounded bytes, unbounded concurrent fetches).
func TestOpts_MapEveryBufferKey(t *testing.T) {
	cfg := kafka.Config{Brokers: []string{"localhost:1"}, Topics: []string{"t"}, Group: "g",
		ProducerQueueMaxKBytes: 2048, ProducerQueueMaxMessages: 500, MessageMaxBytes: 600_000,
		ConsumerQueueMaxKBytes: 8192, ConsumerFetchMaxBytes: 2 << 20, ConsumerPartitionFetchMaxBytes: 512 << 10,
		CompressionType: "zstd", AssignmentStrategy: "range"}
	require.NoError(t, cfg.Validate())
	common, err := commonOpts(cfg, newStats(cfg))
	require.NoError(t, err)
	cl, err := kgo.NewClient(append(append(common, producerOpts(cfg)...), consumerOpts(cfg)...)...)
	require.NoError(t, err)
	defer cl.Close()

	opts := cl.OptValues
	assert.Equal(t, []any{int64(2048 * 1024)}, opts(kgo.MaxBufferedBytes))
	assert.Equal(t, []any{int64(500)}, opts(kgo.MaxBufferedRecords))
	assert.Equal(t, []any{int32(2 << 20)}, opts(kgo.FetchMaxBytes))
	assert.Equal(t, []any{int32(512 << 10)}, opts(kgo.FetchMaxPartitionBytes))
	assert.Equal(t, []any{4}, opts(kgo.MaxConcurrentFetches), "8 MiB of prefetch = 4 fetches of 2 MiB")
	assert.Equal(t, []any{int32(minBrokerReadBytes)}, opts(kgo.BrokerMaxReadBytes))
}

func TestTLSConfig(t *testing.T) {
	tc, err := tlsConfig(kafka.Config{SecurityProtocol: "sasl_plaintext"})
	require.NoError(t, err)
	assert.Nil(t, tc, "no TLS unless the protocol says so")

	tc, err = tlsConfig(kafka.Config{SecurityProtocol: "ssl", SSLEndpointIdentificationAlgorithm: "none"})
	require.NoError(t, err)
	require.NotNil(t, tc.VerifyConnection, "the chain is still verified without the host name check")

	_, err = tlsConfig(kafka.Config{SecurityProtocol: "ssl", SSLCALocation: "/nonexistent/ca.pem"})
	require.Error(t, err)
}

func TestSASLMechanism(t *testing.T) {
	for _, m := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		mech, err := saslMechanism(kafka.Config{SecurityProtocol: "sasl_ssl", SASLMechanism: m, SASLUsername: "u", SASLPassword: "p"})
		require.NoError(t, err)
		assert.Equal(t, m, mech.Name())
	}
	_, err := saslMechanism(kafka.Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPasswordFile: "/nonexistent"})
	require.Error(t, err)
}

// writeCA writes a self-signed CA and a leaf it signed (cert + key) into a
// temp dir: what the SASL_SSL example's gen-certs.sh produces.
func writeCA(t *testing.T) (dir string, ca *x509.Certificate, leaf tls.Certificate) {
	t.Helper()
	dir = t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err = x509.ParseCertificate(caDER)
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "broker"},
		DNSNames: []string{"broker.internal"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	write := func(name, typ string, b []byte) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}), 0o600))
	}
	write("ca.crt", "CERTIFICATE", caDER)
	write("client.crt", "CERTIFICATE", der)
	write("client.key", "EC PRIVATE KEY", keyDER)
	leaf, err = tls.LoadX509KeyPair(filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))
	require.NoError(t, err)
	return dir, ca, leaf
}

// A private CA, a client key pair (mTLS) and endpoint identification "none":
// the chain is still checked against the CA, only the host name is not.
func TestTLSConfig_PrivateCAAndClientCert(t *testing.T) {
	dir, _, leaf := writeCA(t)
	cfg := kafka.Config{SecurityProtocol: "ssl", SSLCALocation: filepath.Join(dir, "ca.crt"),
		SSLCertificateLocation: filepath.Join(dir, "client.crt"), SSLKeyLocation: filepath.Join(dir, "client.key"),
		SSLEndpointIdentificationAlgorithm: "none"}
	tc, err := tlsConfig(cfg)
	require.NoError(t, err)
	require.Len(t, tc.Certificates, 1, "the client presents its certificate")

	peer, err := x509.ParseCertificate(leaf.Certificate[0])
	require.NoError(t, err)
	require.NoError(t, tc.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{peer}}),
		"a broker certificate signed by the CA passes whatever its host name")
	require.Error(t, tc.VerifyConnection(tls.ConnectionState{}), "no certificate at all")

	_, _, otherLeaf := writeCA(t)
	other, err := x509.ParseCertificate(otherLeaf.Certificate[0])
	require.NoError(t, err)
	require.Error(t, tc.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{other}}),
		"a certificate from another CA is refused")
}

func TestTLSConfig_BadFiles(t *testing.T) {
	dir, _, _ := writeCA(t)
	notPEM := filepath.Join(dir, "garbage.crt")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	_, err := tlsConfig(kafka.Config{SecurityProtocol: "sasl_ssl", SSLCALocation: notPEM})
	require.ErrorContains(t, err, "holds no PEM certificate")

	_, err = tlsConfig(kafka.Config{SecurityProtocol: "ssl", SSLCertificateLocation: filepath.Join(dir, "client.crt"),
		SSLKeyLocation: filepath.Join(dir, "missing.key")})
	require.ErrorContains(t, err, "load client key pair")
}

func TestSASLMechanism_PasswordFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(file, []byte("s3cret\n"), 0o600))
	mech, err := saslMechanism(kafka.Config{SecurityProtocol: "sasl_plaintext", SASLMechanism: "SCRAM-SHA-512", SASLUsername: "u", SASLPasswordFile: file})
	require.NoError(t, err)
	assert.Equal(t, "SCRAM-SHA-512", mech.Name())

	empty := filepath.Join(dir, "empty")
	require.NoError(t, os.WriteFile(empty, []byte("\n"), 0o600))
	_, err = saslMechanism(kafka.Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLPasswordFile: empty})
	require.ErrorContains(t, err, "is empty")

	_, err = saslMechanism(kafka.Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "GSSAPI"})
	require.ErrorContains(t, err, "unknown SASL mechanism")

	mech, err = saslMechanism(kafka.Config{SecurityProtocol: "ssl"})
	require.NoError(t, err)
	assert.Nil(t, mech, "no SASL unless the protocol says so")
}

// commonOpts carries TLS, SASL and auto topic creation into the client, and
// fails on what it cannot build.
func TestCommonOpts_SecurityAndErrors(t *testing.T) {
	dir, _, _ := writeCA(t)
	cfg := kafka.Config{Brokers: []string{"localhost:1"}, SecurityProtocol: "sasl_ssl", SASLMechanism: "SCRAM-SHA-256",
		SASLUsername: "u", SASLPassword: "p", SSLCALocation: filepath.Join(dir, "ca.crt"), AllowAutoTopicCreation: true}
	require.NoError(t, cfg.Validate())
	opts, err := commonOpts(cfg, newStats(cfg))
	require.NoError(t, err)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	defer cl.Close()
	assert.NotEmpty(t, cl.OptValues(kgo.DialTLSConfig)[0])
	assert.NotEmpty(t, cl.OptValues(kgo.SASL)[0])
	assert.Equal(t, []any{true}, cl.OptValues(kgo.AllowAutoTopicCreation))

	bad := cfg
	bad.SSLCALocation = filepath.Join(dir, "missing.crt")
	_, err = commonOpts(bad, newStats(bad))
	require.Error(t, err)

	bad = cfg
	bad.SASLMechanism = "GSSAPI"
	_, err = commonOpts(bad, newStats(bad))
	require.Error(t, err)
}

func TestCodecAndBalancer_EveryName(t *testing.T) {
	for name, want := range map[string]kgo.CompressionCodec{"none": kgo.NoCompression(), "gzip": kgo.GzipCompression(),
		"snappy": kgo.SnappyCompression(), "lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression()} {
		assert.Equal(t, want, codec(name), name)
	}
	for name, want := range map[string]string{"range": "range", "roundrobin": "roundrobin", "cooperative-sticky": "cooperative-sticky"} {
		assert.Equal(t, want, balancer(name).ProtocolName(), name)
	}
}

// A full buffer is waited out within the caller's budget, never blocking
// inside the client: with room for one record and no broker, the second
// Send spins on ErrMaxBuffered until ctx ends, and fails retryable.
func TestProducer_FullBufferWaitsWithinBudget(t *testing.T) {
	cfg := kafka.Config{Brokers: []string{"127.0.0.1:1"}, ProducerQueueMaxMessages: 1, PublishTimeout: 5 * time.Second}
	require.NoError(t, cfg.Validate())
	pc, err := Driver{}.NewProducer(cfg, nil)
	require.NoError(t, err)
	defer pc.Close()

	first, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	go func() { _ = pc.Send(first, kafka.Message{Topic: "t", Value: []byte("1")}) }()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = pc.Send(ctx, kafka.Message{Topic: "t", Value: []byte("2"), Headers: []kafka.Header{{Key: "h", Value: []byte("v")}}})
	var se *kafka.SendError
	require.ErrorAs(t, err, &se)
	assert.True(t, se.Retryable)
}

func TestDriver_ClientsFailOnBadSecurity(t *testing.T) {
	cfg := kafka.Config{Brokers: []string{"127.0.0.1:1"}, SecurityProtocol: "ssl", SSLCALocation: "/nonexistent/ca.crt"}
	require.ErrorContains(t, cfg.Validate(), "KAFKA_SSL_CA_LOCATION", "the boot catches it first…")
	// …and a client built without Validate still refuses it.
	_, err := Driver{}.NewProducer(cfg, nil)
	require.Error(t, err)
	_, err = Driver{}.NewConsumer(cfg, nil, kafka.Rebalance{})
	require.Error(t, err)
}
