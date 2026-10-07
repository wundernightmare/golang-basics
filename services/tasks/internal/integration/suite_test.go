// Package integration exercises the whole tasks vertical — HTTP → Postgres
// (+ outbox) → relay → Kafka, with the Valkey read cache — against real
// containers (one of each per test binary, from libs/testx/containers),
// wired exactly as main.go wires it. Each test owns a fresh Postgres schema,
// a Kafka topic and (through the task ids) its cache keys, so tests never see
// each other's rows or events. Skipped under -short.
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/api"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/cache"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/outbox"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/store"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/testsupport"
)

func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

// deps are the addresses the service is wired to — the shared containers, or
// (chaos) Toxiproxy in front of them.
type deps struct {
	postgres string // base DSN; the test's schema is added to it
	valkey   string
	brokers  []string
}

func directDeps(t testing.TB) deps {
	t.Helper()
	return deps{postgres: containers.Postgres(t), valkey: containers.Valkey(t), brokers: containers.Kafka(t)}
}

// wired is the running service.
type wired struct {
	base  string // http://127.0.0.1:port
	srv   *httpx.Server
	db    *pgx.DB
	store *store.Store
	relay *outbox.Relay
	log   *testx.LogBuffer
	topic string
}

// wire assembles the service as main.go does — migrations, store, cache,
// relay, optional checks for cache and broker, every lib's collectors — and
// runs it (server + relay) until the test ends. The logger writes into a
// buffer the test can search.
func wire(t testing.TB, d deps) *wired {
	t.Helper()
	ctx := context.Background()
	buf := &testx.LogBuffer{}
	log := httpx.NewLogger(httpx.LogConfig{Service: "tasks", Level: "info", Format: "json", Writer: buf})
	topic := testx.Unique("tasks.events")

	db := testsupport.MigratedDB(t, testsupport.SchemaDSN(t, containers.Postgres(t), d.postgres))

	vk, err := valkey.New(valkey.Config{URL: d.valkey, DialTimeout: 5 * time.Second, OpTimeout: 300 * time.Millisecond}, log)
	require.NoError(t, err)
	t.Cleanup(vk.Close)

	producer, err := kafka.NewProducer(ctx, kafka.Config{
		Brokers: d.brokers, Topic: topic, ClientID: testx.Unique("tasks-it"),
		DialTimeout: 10 * time.Second, PublishTimeout: 2 * time.Second, AllowAutoTopicCreation: true,
	}, log)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	// Health re-evaluated every second so the chaos tests see it move; the
	// check timeout below the interval, as Validate demands.
	srv, err := httpx.NewServer(httpx.Config{
		Service: "tasks", Addr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0",
		HealthInterval: time.Second, HealthTimeout: 900 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
	}, log)
	require.NoError(t, err)
	st := store.New(db.Pool())
	relay := outbox.NewRelay(db.Pool(), producer, outbox.Config{
		Topic: topic, PollInterval: 200 * time.Millisecond, MaxBackoff: time.Second,
	}, log)
	srv.Health.Register("postgres", db.ReadyCheck())
	srv.Health.Register("valkey", vk.ReadyCheck(), httpx.Optional())
	srv.Health.Register("kafka", producer.ReadyCheck(), httpx.Optional())
	srv.Metrics.Registry.MustRegister(db.Collectors()...)
	srv.Metrics.Registry.MustRegister(vk.Collectors()...)
	srv.Metrics.Registry.MustRegister(producer.Collectors()...)
	srv.Metrics.Registry.MustRegister(relay.Collectors()...)
	api.Register(srv, api.Deps{
		Store: st, Cache: cache.New(vk, time.Minute, 2*time.Second, log), Logger: log, Committed: relay.Wake,
	})

	runCtx, stop := context.WithCancel(ctx)
	serverDone, relayDone := make(chan error, 1), make(chan error, 1)
	go func() { serverDone <- srv.Run(runCtx) }()
	go func() { relayDone <- relay.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		<-serverDone
		<-relayDone
	})
	require.Eventually(t, func() bool { return srv.ListenAddr() != "" }, 10*time.Second, 10*time.Millisecond)
	return &wired{base: "http://" + srv.ListenAddr(), srv: srv, db: db, store: st, relay: relay, log: buf, topic: topic}
}

// --- HTTP helpers ------------------------------------------------------------

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) decode(t testing.TB, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(r.body, v), string(r.body))
}

var client = &http.Client{Timeout: 30 * time.Second}

func (w *wired) do(t testing.TB, method, path, body string, headers ...string) response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, w.base+path, rd)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return response{status: resp.StatusCode, header: resp.Header, body: bytes.TrimSpace(raw)}
}

// readyz returns the admin listener's readiness status and body.
func (w *wired) readyz(t testing.TB) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	w.srv.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec.Code, rec.Body.String()
}

func (w *wired) waitReadyz(t testing.TB, want string) string {
	t.Helper()
	var body string
	require.Eventually(t, func() bool {
		_, body = w.readyz(t)
		return strings.Contains(body, `"status":"`+want+`"`)
	}, 40*time.Second, 100*time.Millisecond, "readyz never reported %q (last: %s)", want, body)
	return body
}

func (w *wired) pending(t testing.TB) int64 {
	t.Helper()
	n, _, err := w.relay.Stats(context.Background())
	require.NoError(t, err)
	return n
}

// --- Kafka -------------------------------------------------------------------

// consumed is one record read back from the topic.
type consumed struct {
	key     string
	value   []byte
	headers map[string]string
}

// readEvents reads the topic from the start until it has n records or the
// timeout passes, and returns what it got.
func readEvents(t testing.TB, brokers []string, topic string, n int, timeout time.Duration) []consumed {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []consumed
	for len(out) < n && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			h := map[string]string{}
			for _, x := range r.Headers {
				h[x.Key] = string(x.Value)
			}
			out = append(out, consumed{key: string(r.Key), value: r.Value, headers: h})
		})
	}
	return out
}
