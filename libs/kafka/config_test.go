package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

func TestLoadConfig_Defaults(t *testing.T) {
	_, err := LoadConfig("TASKS_")
	require.ErrorContains(t, err, "KAFKA_BROKERS must be set", "the address has no default")

	t.Setenv("TASKS_KAFKA_BROKERS", "localhost:9092")
	cfg, err := LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, "tasks.events", cfg.Topic)
	assert.Equal(t, []string{"tasks.events"}, cfg.Topics, "Topics falls back to the single Topic")
	assert.False(t, cfg.AllowAutoTopicCreation)
	assert.Empty(t, cfg.SecurityProtocol, "plaintext")
	// The memory bounds: the same defaults as nodejs-basics.
	assert.Equal(t, 65536, cfg.ProducerQueueMaxKBytes)
	assert.Equal(t, 100000, cfg.ProducerQueueMaxMessages)
	assert.Equal(t, 1000000, cfg.MessageMaxBytes)
	assert.Equal(t, 4096, cfg.ConsumerQueueMaxKBytes)
	assert.Equal(t, 4<<20, cfg.ConsumerFetchMaxBytes)
	assert.Equal(t, 1<<20, cfg.ConsumerPartitionFetchMaxBytes)
	assert.Equal(t, "lz4", cfg.CompressionType)
	assert.Equal(t, 5, cfg.LingerMs)
	assert.Equal(t, "cooperative-sticky", cfg.AssignmentStrategy)
	assert.Equal(t, "earliest", cfg.AutoOffsetReset)
	assert.Equal(t, 15000, cfg.StatisticsInterval)
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
	t.Setenv("TASKS_KAFKA_EXTRA_PROPERTIES", `{"debug":"broker","socket.timeout.ms":30000,"socket.nagle.disable":true}`)
	cfg, err = LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, []string{"a:9092", "b:9092"}, cfg.Brokers)
	assert.Equal(t, []string{"x", "y"}, cfg.Topics)
	assert.Empty(t, cfg.deadLetterTopic("x"), `"none" disables the DLQ`)
	assert.Equal(t, 0, cfg.maxRetries(), "negative means no retries")
	assert.Equal(t, Properties{"debug": "broker", "socket.timeout.ms": "30000", "socket.nagle.disable": "true"}, cfg.ExtraProperties,
		"JSON values of any scalar type become librdkafka strings")

	t.Setenv("TASKS_KAFKA_EXTRA_PROPERTIES", `["not","an","object"]`)
	_, err = LoadConfig("TASKS_")
	require.ErrorContains(t, err, "want a JSON object")
}

// The service config pattern: kafka.Config nested in a service struct and
// loaded through httpx.LoadYAML, with env over YAML over the tag defaults.
func TestConfig_LoadYAMLNested(t *testing.T) {
	type svc struct {
		Kafka Config `yaml:"kafka"`
	}
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(ca, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600))
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`kafka:
  brokers: [b:9092]
  topic: from-yaml
  security_protocol: sasl_ssl
  sasl_mechanism: SCRAM-SHA-512
  sasl_username: u
  ssl_ca_location: `+ca+`
  consumer_max_retries: 2
  producer_queue_max_kbytes: 32768
  extra_properties:
    socket.timeout.ms: 30000
    sasl.oauthbearer.client.secret: hush
`), 0o600))
	t.Setenv("SVC_KAFKA_SASL_PASSWORD", "from-env")

	var cfg svc
	require.NoError(t, httpx.LoadYAML(path, "SVC_", &cfg))
	require.NoError(t, cfg.Kafka.Validate())
	assert.Equal(t, "from-yaml", cfg.Kafka.Topic)
	assert.Equal(t, []string{"from-yaml"}, cfg.Kafka.Topics)
	assert.Equal(t, "from-env", cfg.Kafka.SASLPassword)
	assert.Equal(t, 2, cfg.Kafka.MaxRetries)
	assert.Equal(t, 32768, cfg.Kafka.ProducerQueueMaxKBytes)
	assert.Equal(t, "30000", cfg.Kafka.ExtraProperties["socket.timeout.ms"], "a YAML number is a property string")
	assert.Equal(t, "{topic}.dlq", cfg.Kafka.DeadLetterTopic, "tag default fills what neither source set")

	redacted, ok := httpx.Redact(cfg).(map[string]any)
	require.True(t, ok)
	k := redacted["kafka"].(map[string]any)
	assert.Equal(t, "[redacted]", k["sasl_password"], "the password never reaches /admin/config")
	assert.Equal(t, "[redacted]", k["extra_properties"].(map[string]any)["sasl.oauthbearer.client.secret"], "nor a secret property")
	assert.Equal(t, "30000", k["extra_properties"].(map[string]any)["socket.timeout.ms"])
	assert.Equal(t, ca, k["ssl_ca_location"])
}

func TestConfig_Validate(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(pw, []byte("s3cret\n"), 0o600))
	cases := map[string]struct {
		cfg  Config
		want string
	}{
		"defaults are valid":           {cfg: Config{}},
		"no brokers":                   {cfg: Config{Brokers: []string{}}, want: "KAFKA_BROKERS must be set"},
		"unknown protocol":             {cfg: Config{SecurityProtocol: "tls"}, want: "unknown protocol"},
		"sasl password and its file":   {cfg: Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPassword: "p", SASLPasswordFile: pw}, want: "pick one"},
		"sasl password from a file":    {cfg: Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPasswordFile: pw}},
		"unknown sasl":                 {cfg: Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "gssapi"}, want: "unknown mechanism"},
		"sasl without a mechanism":     {cfg: Config{SecurityProtocol: "sasl_plaintext"}, want: "needs KAFKA_SASL_MECHANISM"},
		"sasl without credentials":     {cfg: Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN"}, want: "needs KAFKA_SASL_USERNAME"},
		"sasl keys without sasl":       {cfg: Config{SASLUsername: "u"}, want: "is not sasl_plaintext or sasl_ssl"},
		"ssl keys without ssl":         {cfg: Config{SSLCALocation: "ca.pem"}, want: "is not ssl or sasl_ssl"},
		"half a key pair":              {cfg: Config{SecurityProtocol: "ssl", SSLCertificateLocation: "c.pem"}, want: "must be set together"},
		"bad endpoint algorithm":       {cfg: Config{SecurityProtocol: "ssl", SSLEndpointIdentificationAlgorithm: "dns"}, want: "want https|none"},
		"protocol is normalised":       {cfg: Config{SecurityProtocol: " SASL_SSL ", SASLMechanism: "scram-sha-256", SASLUsername: "u", SASLPassword: "p"}},
		"unknown codec":                {cfg: Config{CompressionType: "brotli"}, want: "unknown codec"},
		"unknown strategy":             {cfg: Config{AssignmentStrategy: "sticky"}, want: "cooperative-sticky|range|roundrobin"},
		"poll interval below session":  {cfg: Config{SessionTimeoutMs: 60_000, MaxPollIntervalMs: 30_000}, want: "is below KAFKA_CONSUMER_SESSION_TIMEOUT_MS"},
		"record larger than the queue": {cfg: Config{ProducerQueueMaxKBytes: 512}, want: "does not fit in KAFKA_PRODUCER_QUEUE_MAX_KBYTES"},
		"fetch below the record size":  {cfg: Config{ConsumerFetchMaxBytes: 1024}, want: "the largest record could not be fetched"},
		"partition fetch above fetch":  {cfg: Config{ConsumerPartitionFetchMaxBytes: 8 << 20}, want: "is above KAFKA_CONSUMER_FETCH_MAX_BYTES"},
		"negative buffer":              {cfg: Config{ConsumerQueueMaxKBytes: -1}, want: "KAFKA_CONSUMER_QUEUE_MAX_KBYTES must be positive"},
		"backoff cap below the start":  {cfg: Config{RetryBackoff: time.Second, RetryBackoffMax: time.Millisecond}, want: "RETRY_BACKOFF_MAX"},
		"negative handler timeout":     {cfg: Config{HandlerTimeout: -time.Second}, want: "KAFKA_CONSUMER_HANDLER_TIMEOUT"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.cfg.Brokers == nil { // every case but "no brokers" has the one required key
				tc.cfg.Brokers = []string{"b:9092"}
			}
			err := tc.cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// Every file a security key names is checked at Validate — what boot and
// --check-config run — so a missing secret or certificate mount is a config
// error with the key's name, not a TLS/SASL failure once the client dials.
func TestConfig_ValidateChecksTheFilesItNames(t *testing.T) {
	dir := t.TempDir()
	file := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		return p
	}
	ca, cert, key, pw, empty := file("ca.pem", "x"), file("c.pem", "x"), file("k.pem", "x"), file("pw", "s3cret\n"), file("empty", "\n")
	missing := filepath.Join(dir, "nope")
	tls := func(c Config) Config {
		c.Brokers, c.SecurityProtocol = []string{"b:9092"}, "ssl"
		return c
	}
	sasl := func(file string) Config {
		return Config{Brokers: []string{"b:9092"}, SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPasswordFile: file}
	}
	for name, tc := range map[string]struct {
		cfg  Config
		want string
	}{
		"all present":           {cfg: tls(Config{SSLCALocation: ca, SSLCertificateLocation: cert, SSLKeyLocation: key})},
		"CA directory":          {cfg: tls(Config{SSLCALocation: dir})},
		"CA probe":              {cfg: tls(Config{SSLCALocation: "probe"})},
		"missing CA":            {cfg: tls(Config{SSLCALocation: missing}), want: "KAFKA_SSL_CA_LOCATION: open " + missing},
		"missing client key":    {cfg: tls(Config{SSLCertificateLocation: cert, SSLKeyLocation: missing}), want: "KAFKA_SSL_KEY_LOCATION"},
		"client cert directory": {cfg: tls(Config{SSLCertificateLocation: dir, SSLKeyLocation: key}), want: "KAFKA_SSL_CERTIFICATE_LOCATION: " + dir + " is a directory"},
		"password file":         {cfg: sasl(pw)},
		"missing password file": {cfg: sasl(missing), want: "KAFKA_SASL_PASSWORD_FILE: open " + missing},
		"empty password file":   {cfg: sasl(empty), want: "KAFKA_SASL_PASSWORD_FILE: " + empty + " is empty"},
	} {
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

func TestDeadLetterTopic(t *testing.T) {
	assert.Equal(t, "orders.dlq", Config{DeadLetterTopic: "{topic}.dlq"}.deadLetterTopic("orders"))
	assert.Equal(t, "parked", Config{DeadLetterTopic: "parked"}.deadLetterTopic("orders"))
	assert.Empty(t, Config{DeadLetterTopic: DeadLetterDisabled}.deadLetterTopic("orders"))
}

func TestSendError(t *testing.T) {
	se := NewSendError(KindRejected, 10, errors.New("too large"))
	assert.False(t, se.Retryable)
	assert.True(t, Rejected(fmt.Errorf("kafka: send: %w", se)), "survives wrapping")
	assert.Equal(t, "kafka send failed (rejected): too large", se.Error())
	for _, kind := range []string{KindQueueFull, KindUnavailable, KindNotProvisioned, KindFatal, KindUnknown} {
		assert.True(t, NewSendError(kind, 0, errors.New("x")).Retryable, kind)
	}
	assert.Equal(t, KindUnavailable, NewSendError(KindRejected, 0, context.DeadlineExceeded).Kind,
		"a spent deadline is never the record's fault")
	assert.False(t, Rejected(errors.New("plain")))
	assert.False(t, Rejected(nil))
}

// Pass-through properties reach GET /admin/config through httpx.Redact: every
// librdkafka property that carries a secret inline must come out redacted,
// paths and plain settings must not.
func TestConfig_RedactsSecretPassThroughProperties(t *testing.T) {
	t.Parallel()
	secrets := []string{
		"sasl.password",
		"sasl.oauthbearer.config",
		"sasl.oauthbearer.client.secret",
		"sasl.oauthbearer.assertion.private.key.pem",
		"sasl.oauthbearer.assertion.private.key.passphrase",
		"ssl.key.pem",
		"ssl.key.password",
		"ssl.keystore.password",
	}
	plain := map[string]string{"ssl.key.location": "/etc/kafka/client.key", "linger.ms": "5", "debug": "broker"}
	props := Properties{}
	for _, k := range secrets {
		props[k] = "s3cret-" + k
	}
	for k, v := range plain {
		props[k] = v
	}
	out, err := json.Marshal(httpx.Redact(Config{ExtraProperties: props}))
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	extra, ok := got["extra_properties"].(map[string]any)
	require.True(t, ok, "%s", out)
	for _, k := range secrets {
		assert.Equal(t, "[redacted]", extra[k], k)
	}
	for k, v := range plain {
		assert.Equal(t, v, extra[k], k)
	}
	assert.NotContains(t, string(out), "s3cret")
}
