// Package integration runs the ingest service as the binary assembles it
// (internal/app) against a real Kafka and a real S3-compatible store
// (RustFS) from libs/testx/containers, and breaks them on purpose. Each test
// owns a topic and an archive prefix. Skipped under -short.
//
// The invariant every test here comes back to: an event the service answered
// 200 for is in Kafka or in the archive — whatever was down, and whenever the
// service was stopped.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/app"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/config"
	_ "github.com/tracehubmmp/golang-basics/services/ingest/internal/kafkaprovider" // the Kafka client the binary links
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/testsupport"
)

func TestMain(m *testing.M) {
	code := containers.Main(m)

	if builtBin != "" {
		_ = os.RemoveAll(filepath.Dir(builtBin)) // the child binary of kill_test.go
	}

	os.Exit(code)
}

const bucket = "ingest-integration"

// backends are the addresses the service is wired to: the shared containers,
// or (chaos) Toxiproxy in front of the store.
type backends struct {
	brokers    []string // nil: no Kafka
	s3Endpoint string
	store      containers.S3Store
}

func direct(t testing.TB) backends {
	t.Helper()

	store := containers.S3(t)
	ensureBucket(t, store)

	return backends{brokers: containers.Kafka(t), s3Endpoint: store.Endpoint, store: store}
}

// ensureBucket creates the suite's bucket and waits until the store really
// serves it. The service creates it too, but with one attempt at boot — and
// RustFS reports /health before its S3 API is ready: for the first seconds a
// signed call can still answer 503 (seen on CI runners).
func ensureBucket(t testing.TB, backend containers.S3Store) {
	t.Helper()

	store, err := s3.New(context.Background(), s3.Config{
		Bucket: bucket, Endpoint: backend.Endpoint, Region: backend.Region,
		AccessKeyID: backend.AccessKey, SecretAccessKey: backend.SecretKey,
		UsePathStyle: true, Insecure: true, AutoCreateBucket: true,
	}, nil)
	require.NoError(t, err)

	defer store.Close()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, store.Ensure(context.Background()))
	}, time.Minute, 250*time.Millisecond, "the bucket becomes reachable")
}

// service is one running instance.
type service struct {
	t      testing.TB
	base   string // http://127.0.0.1:port
	admin  string
	srv    *httpx.Server
	log    *testx.LogBuffer
	topic  string
	prefix string
	back   backends

	cancel context.CancelFunc
	done   chan error
}

// environment is the configuration of a test instance: the real keys, with
// every wait shortened so an outage plays out in seconds.
func environment(back backends, topic, prefix string, extra map[string]string) map[string]string {
	env := map[string]string{
		"INGEST_HTTP_ADDR":             "127.0.0.1:0",
		"INGEST_ADMIN_ADDR":            "127.0.0.1:0",
		"INGEST_HTTP_SHUTDOWN_DELAY":   "0s",
		"INGEST_HTTP_SHUTDOWN_TIMEOUT": "5s",
		"INGEST_HEALTH_CHECK_INTERVAL": "1s",
		"INGEST_HEALTH_CHECK_TIMEOUT":  "900ms",

		"INGEST_S3_BUCKET":             bucket,
		"INGEST_S3_ENDPOINT":           back.s3Endpoint,
		"INGEST_S3_INSECURE":           "true",
		"INGEST_S3_ACCESS_KEY_ID":      back.store.AccessKey,
		"INGEST_S3_SECRET_ACCESS_KEY":  back.store.SecretKey,
		"INGEST_S3_AUTO_CREATE_BUCKET": "true",
		"INGEST_S3_OP_TIMEOUT":         "1s",
		"INGEST_S3_MAX_ATTEMPTS":       "1",
		"INGEST_S3_DIAL_TIMEOUT":       "500ms",

		"INGEST_ARCHIVE_PREFIX":        prefix,
		"INGEST_ARCHIVE_LINGER":        "300ms",
		"INGEST_ARCHIVE_URGENT_LINGER": "100ms",

		"INGEST_KAFKA_ACK_GRACE": "200ms",
		"INGEST_DRAIN_TIMEOUT":   "10s",

		"INGEST_KAFKA_BREAKER_MIN_REQUESTS":   "3",
		"INGEST_KAFKA_BREAKER_OPEN_TIMEOUT":   "1s",
		"INGEST_ARCHIVE_BREAKER_MIN_REQUESTS": "2",
		"INGEST_ARCHIVE_BREAKER_OPEN_TIMEOUT": "1s",
	}

	if len(back.brokers) > 0 {
		env["INGEST_KAFKA_BROKERS"] = strings.Join(back.brokers, ",")
		env["INGEST_KAFKA_TOPIC"] = topic
		env["INGEST_KAFKA_CLIENT_ID"] = testx.Unique("ingest")
		env["INGEST_KAFKA_ALLOW_AUTO_TOPIC_CREATION"] = "true"
		env["INGEST_KAFKA_PUBLISH_TIMEOUT"] = "2s"
		env["INGEST_KAFKA_DIAL_TIMEOUT"] = "2s"
	}

	maps.Copy(env, extra)

	return env
}

// run starts the service through app.Run — the binary's own assembly — and
// waits until it listens. It is stopped (gracefully) when the test ends, or
// earlier by stop.
func run(t testing.TB, back backends, extra map[string]string) *service {
	t.Helper()

	svc := &service{
		t: t, log: &testx.LogBuffer{}, back: back,
		topic: testx.Unique("ingest.events"), prefix: testx.Unique("it") + "/",
		done: make(chan error, 1),
	}

	cfg, err := config.LoadFrom("", environment(back, svc.topic, svc.prefix, extra))
	require.NoError(t, err)

	logger := httpx.NewLogger(httpx.LogConfig{Service: "ingest", Level: "info", Format: "json", Writer: svc.log})

	ctx, cancel := context.WithCancel(context.Background())
	svc.cancel = cancel

	servers := make(chan *httpx.Server, 1)

	go func() { svc.done <- app.Run(ctx, cfg, logger, func(srv *httpx.Server) { servers <- srv }) }()

	select {
	case svc.srv = <-servers:
	case err := <-svc.done:
		require.FailNow(t, "the service did not start", "%v\n%s", err, svc.log.String())
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the service did not start in time", svc.log.String())
	}

	require.Eventually(t, func() bool { return svc.srv.AdminListenAddr() != "" }, 30*time.Second, 5*time.Millisecond,
		"the listeners bind:\n%s", svc.log.String())

	svc.base = "http://" + svc.srv.ListenAddr()
	svc.admin = "http://" + svc.srv.AdminListenAddr()

	t.Cleanup(func() { _ = svc.stop() })

	return svc
}

// stop shuts the service down as SIGTERM would and returns what Run returned.
func (s *service) stop() error {
	s.cancel()

	select {
	case err := <-s.done:
		s.done <- err // a second stop (the cleanup) sees the same answer
		return err
	case <-time.After(60 * time.Second):
		require.FailNow(s.t, "the service did not stop", s.log.String())
		return nil
	}
}

type answer struct {
	status int
	header http.Header
	body   string
	took   time.Duration
	err    error // a transport error: no answer at all
}

// post sends one batch of events with the given ids.
func (s *service) post(ids ...string) answer {
	events := make([]string, len(ids))
	for i, id := range ids {
		events[i] = fmt.Sprintf(`{"id":%q,"type":"it.test","payload":{"n":%d}}`, id, i)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.base+"/events",
		strings.NewReader(`{"events":[`+strings.Join(events, ",")+`]}`))
	if err != nil {
		return answer{err: err}
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return answer{err: err, took: time.Since(start)}
	}

	defer func() { _ = res.Body.Close() }()

	body, _ := io.ReadAll(res.Body)

	return answer{status: res.StatusCode, header: res.Header, body: string(body), took: time.Since(start)}
}

// ids makes n unique event ids.
func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = testx.Unique("ev")
	}

	return out
}

func (s *service) metric(name string, labels map[string]string) float64 {
	return testx.Metric(s.t, s.srv.Metrics.Registry, name, labels)
}

// waitReadyz polls /readyz until it reports status, and returns the body.
func (s *service) waitReadyz(status string) string {
	s.t.Helper()

	var body string

	require.Eventually(s.t, func() bool {
		res, err := http.Get(s.admin + "/readyz")
		if err != nil {
			return false
		}

		defer func() { _ = res.Body.Close() }()

		raw, _ := io.ReadAll(res.Body)
		body = string(raw)

		var out struct {
			Status string `json:"status"`
		}

		return json.Unmarshal(raw, &out) == nil && out.Status == status
	}, 30*time.Second, 100*time.Millisecond, "readyz never reported %q, last: %s", status, body)

	return body
}

// archived returns the ids of every event in the service's raw archive,
// read straight from the store (never through a proxy a test took down).
func (s *service) archived() map[string]int {
	s.t.Helper()

	store, err := s3.New(context.Background(), s3.Config{
		Bucket: bucket, Endpoint: s.back.store.Endpoint, Region: s.back.store.Region,
		AccessKeyID: s.back.store.AccessKey, SecretAccessKey: s.back.store.SecretKey,
		UsePathStyle: true, Insecure: true,
	}, nil)
	require.NoError(s.t, err)

	defer store.Close()

	out := map[string]int{}

	for obj, err := range store.List(context.Background(), archive.NewLayout(s.prefix).RawRoot()) {
		require.NoError(s.t, err)

		object, err := store.Get(context.Background(), obj.Key, 64<<20)
		require.NoError(s.t, err)

		records, err := testsupport.DecodeObject(object)
		require.NoError(s.t, err, obj.Key)

		for _, r := range records {
			out[r.ID]++
		}
	}

	return out
}

// waitArchived waits until every id is in the archive.
func (s *service) waitArchived(want []string) {
	s.t.Helper()

	var missing []string

	require.Eventually(s.t, func() bool {
		got := s.archived()
		missing = missing[:0]

		for _, id := range want {
			if got[id] == 0 {
				missing = append(missing, id)
			}
		}

		return len(missing) == 0
	}, 30*time.Second, 200*time.Millisecond, "not archived: %v", &missing)
}

// published reads the service's topic from the start until it has seen every
// wanted id or the timeout passes, and returns the ids it saw with their
// event_type header.
func (s *service) published(want []string, timeout time.Duration) map[string]string {
	s.t.Helper()

	consumer, err := kafka.NewConsumer(context.Background(), kafka.Config{
		Brokers: s.back.brokers, Topics: []string{s.topic}, Group: testx.Unique("read"), ClientID: testx.Unique("read"),
		AllowAutoTopicCreation: true, StatisticsInterval: -1,
	}, nil)
	require.NoError(s.t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var (
		mu   sync.Mutex
		seen = map[string]string{}
	)

	_ = consumer.Run(ctx, func(_ context.Context, m kafka.Message) error {
		eventType, _ := m.Header(kafka.HeaderEventType)

		mu.Lock()
		defer mu.Unlock()

		seen[string(m.Key)] = string(eventType)

		for _, id := range want {
			if _, ok := seen[id]; !ok {
				return nil
			}
		}

		cancel()

		return nil
	})

	mu.Lock()
	defer mu.Unlock()

	return maps.Clone(seen)
}
