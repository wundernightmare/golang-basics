package kafka

import (
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

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, "tasks.events", cfg.Topic)
	assert.Equal(t, []string{"tasks.events"}, cfg.Topics, "Topics falls back to the single Topic")
	assert.False(t, cfg.AllowAutoTopicCreation)
	assert.False(t, cfg.TLSEnabled)
	assert.Equal(t, 5, cfg.MaxRetries)
	assert.Equal(t, 200*time.Millisecond, cfg.RetryBackoff)
	assert.Equal(t, 30*time.Second, cfg.RetryBackoffMax)
	assert.Equal(t, 10*time.Second, cfg.DrainTimeout)
	assert.Equal(t, 30*time.Second, cfg.HandlerTimeout)
	assert.Equal(t, 100, cfg.MaxPollRecords)
	assert.Equal(t, "tasks.events.dlq", cfg.deadLetterTopic("tasks.events"))

	t.Setenv("TASKS_KAFKA_BROKERS", "a:9092,b:9092")
	t.Setenv("TASKS_KAFKA_TOPICS", "x,y")
	t.Setenv("TASKS_KAFKA_DEAD_LETTER_TOPIC", "none")
	t.Setenv("TASKS_KAFKA_CONSUMER_MAX_RETRIES", "-1")
	cfg, err = LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)
	assert.Equal(t, []string{"x", "y"}, cfg.Topics)
	assert.Empty(t, cfg.deadLetterTopic("x"), `"none" disables the DLQ`)
	assert.Equal(t, 0, cfg.maxRetries(), "negative means no retries")
}

// The service config pattern: kafka.Config nested in a service struct and
// loaded through httpx.LoadYAML, with env over YAML over the tag defaults.
func TestConfig_LoadYAMLNested(t *testing.T) {
	type svc struct {
		Kafka Config `yaml:"kafka"`
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("kafka:\n  topic: from-yaml\n  sasl_mechanism: scram-sha-512\n  sasl_username: u\n  consumer_max_retries: 2\n"), 0o600))
	t.Setenv("SVC_KAFKA_SASL_PASSWORD", "from-env")

	var cfg svc
	require.NoError(t, httpx.LoadYAML(path, "SVC_", &cfg))
	require.NoError(t, cfg.Kafka.Validate())
	assert.Equal(t, "from-yaml", cfg.Kafka.Topic)
	assert.Equal(t, []string{"from-yaml"}, cfg.Kafka.Topics)
	assert.Equal(t, "from-env", cfg.Kafka.SASLPassword)
	assert.Equal(t, 2, cfg.Kafka.MaxRetries)
	assert.Equal(t, "{topic}.dlq", cfg.Kafka.DeadLetterTopic, "tag default fills what neither source set")

	redacted, ok := httpx.Redact(cfg).(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "[redacted]", redacted["kafka"].(map[string]any)["sasl_password"], "the password never reaches /admin/config")
}

func TestConfig_Validate(t *testing.T) {
	cases := map[string]struct {
		cfg  Config
		want string
	}{
		"defaults are valid":           {cfg: Config{}},
		"unknown sasl":                 {cfg: Config{SASLMechanism: "gssapi"}, want: "unknown mechanism"},
		"sasl without credentials":     {cfg: Config{SASLMechanism: "plain"}, want: "needs KAFKA_SASL_USERNAME"},
		"credentials without sasl":     {cfg: Config{SASLUsername: "u"}, want: "without KAFKA_SASL_MECHANISM"},
		"half a key pair":              {cfg: Config{TLSEnabled: true, TLSClientCert: "c.pem"}, want: "must be set together"},
		"tls files without tls":        {cfg: Config{TLSCACert: "ca.pem"}, want: "KAFKA_TLS_ENABLED is false"},
		"backoff cap below the start":  {cfg: Config{RetryBackoff: time.Second, RetryBackoffMax: time.Millisecond}, want: "RETRY_BACKOFF_MAX"},
		"negative handler timeout":     {cfg: Config{HandlerTimeout: -time.Second}, want: "KAFKA_CONSUMER_HANDLER_TIMEOUT"},
		"sasl mechanism is normalised": {cfg: Config{SASLMechanism: " SCRAM-SHA-256 ", SASLUsername: "u", SASLPassword: "p"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestConfig_SASLMapping(t *testing.T) {
	for mech, want := range map[string]string{
		"plain":         "PLAIN",
		"scram-sha-256": "SCRAM-SHA-256",
		"scram-sha-512": "SCRAM-SHA-512",
	} {
		t.Run(mech, func(t *testing.T) {
			m, err := Config{SASLMechanism: mech, SASLUsername: "u", SASLPassword: "p"}.saslMechanism()
			require.NoError(t, err)
			require.NotNil(t, m)
			assert.Equal(t, want, m.Name())
		})
	}
	m, err := Config{}.saslMechanism()
	require.NoError(t, err)
	assert.Nil(t, m, "no SASL by default")

	opts, err := Config{SASLMechanism: "plain", SASLUsername: "u", SASLPassword: "p", TLSEnabled: true}.commonOpts()
	require.NoError(t, err)
	assert.Len(t, opts, 5, "seeds, client id, dial timeout, TLS, SASL")
}

func TestConfig_TLSMapping(t *testing.T) {
	cfg, err := Config{}.tlsConfig()
	require.NoError(t, err)
	assert.Nil(t, cfg, "plaintext by default")

	dir := t.TempDir()
	caPEM, certPEM, keyPEM := selfSigned(t)
	ca, cert, key := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "client.pem"), filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(ca, caPEM, 0o600))
	require.NoError(t, os.WriteFile(cert, certPEM, 0o600))
	require.NoError(t, os.WriteFile(key, keyPEM, 0o600))

	cfg, err = Config{TLSEnabled: true, TLSCACert: ca, TLSClientCert: cert, TLSClientKey: key}.tlsConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.False(t, cfg.InsecureSkipVerify, "verification is on unless asked")
	require.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1, "the client pair is loaded for mTLS")

	cfg, err = Config{TLSEnabled: true, TLSInsecureSkipVerify: true}.tlsConfig()
	require.NoError(t, err)
	assert.True(t, cfg.InsecureSkipVerify)

	_, err = Config{TLSEnabled: true, TLSCACert: key}.tlsConfig()
	require.ErrorContains(t, err, "no PEM certificate", "a key is not a CA bundle")
	_, err = Config{TLSEnabled: true, TLSCACert: filepath.Join(dir, "missing.pem")}.tlsConfig()
	require.ErrorContains(t, err, "KAFKA_TLS_CA_CERT")
}

func TestDeadLetterTopic(t *testing.T) {
	assert.Equal(t, "orders.dlq", Config{DeadLetterTopic: "{topic}.dlq"}.deadLetterTopic("orders"))
	assert.Equal(t, "all-dead", Config{DeadLetterTopic: "all-dead"}.deadLetterTopic("orders"))
	assert.Empty(t, Config{DeadLetterTopic: "none"}.deadLetterTopic("orders"))
	assert.Empty(t, Config{DeadLetterTopic: "-"}.deadLetterTopic("orders"))
}

// selfSigned returns a CA certificate (also usable as the leaf) and its key, PEM-encoded.
func selfSigned(t testing.TB) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafka-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return certPEM, certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
