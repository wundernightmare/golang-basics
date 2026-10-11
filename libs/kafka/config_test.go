package kafka

import (
	"context"
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
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`kafka:
  brokers: [b:9092]
  topic: from-yaml
  security_protocol: sasl_ssl
  sasl_mechanism: SCRAM-SHA-512
  sasl_username: u
  ssl_ca_location: /etc/kafka/ca.crt
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
	assert.Equal(t, "/etc/kafka/ca.crt", k["ssl_ca_location"])
}

func TestConfig_Validate(t *testing.T) {
	cases := map[string]struct {
		cfg  Config
		want string
	}{
		"defaults are valid":           {cfg: Config{}},
		"no brokers":                   {cfg: Config{Brokers: []string{}}, want: "KAFKA_BROKERS must be set"},
		"unknown protocol":             {cfg: Config{SecurityProtocol: "tls"}, want: "unknown protocol"},
		"sasl password and its file":   {cfg: Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPassword: "p", SASLPasswordFile: "/run/pw"}, want: "pick one"},
		"sasl password from a file":    {cfg: Config{SecurityProtocol: "sasl_ssl", SASLMechanism: "PLAIN", SASLUsername: "u", SASLPasswordFile: "/run/pw"}},
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
