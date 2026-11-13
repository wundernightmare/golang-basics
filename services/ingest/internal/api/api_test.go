package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/contracts/ingestapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/contract"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/api"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/pipeline"
)

// fakeIngester records what the handler passed on and answers what the test
// sets.
type fakeIngester struct {
	mu     sync.Mutex
	got    [][]pipeline.Event
	err    error
	onCall func(ctx context.Context) error
}

func (f *fakeIngester) Ingest(ctx context.Context, events []pipeline.Event) error {
	f.mu.Lock()
	f.got = append(f.got, events)
	err, onCall := f.err, f.onCall
	f.mu.Unlock()

	if onCall != nil {
		return onCall(ctx)
	}

	return err
}

func (f *fakeIngester) fail(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *fakeIngester) last(t testing.TB) []pipeline.Event {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	require.NotEmpty(t, f.got)

	return f.got[len(f.got)-1]
}

func (f *fakeIngester) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.got)
}

type harness struct {
	ts       *httptest.Server
	ingester *fakeIngester
	log      *testx.LogBuffer
	oapi     *contract.OpenAPI
}

func newHarness(t testing.TB, mutate ...func(*httpx.Config)) *harness {
	t.Helper()

	hrn := &harness{ingester: &fakeIngester{}, log: &testx.LogBuffer{}}
	logger := httpx.NewLogger(httpx.LogConfig{Level: "info", Format: "json", Writer: hrn.log})

	cfg := httpx.Config{Service: "ingest", Addr: ":0", AdminAddr: ""}
	for _, m := range mutate {
		m(&cfg)
	}

	srv, err := httpx.NewServer(cfg, logger)
	require.NoError(t, err)
	api.Register(srv, api.Deps{
		Ingester: hrn.ingester, Logger: logger,
		MaxEventBytes: 256, MaxBatchEvents: 3, RetryAfter: 2 * time.Second,
	})
	hrn.ts = httptest.NewServer(srv.Handler())
	t.Cleanup(hrn.ts.Close)
	hrn.oapi = contract.LoadOpenAPI(t, "openapi3/ingest.openapi.yaml")

	return hrn
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) problem(t testing.TB) map[string]any {
	t.Helper()
	assert.Equal(t, httpx.ProblemContentType, r.header.Get("Content-Type"))

	var p map[string]any
	require.NoError(t, json.Unmarshal(r.body, &p), string(r.body))

	return p
}

// post sends a batch and checks the exchange against the OpenAPI document.
func (hrn *harness) post(t testing.TB, body string) response {
	t.Helper()

	resp := hrn.send(t, body)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, hrn.ts.URL+"/events", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	hrn.oapi.Validate(t, req, []byte(body), resp.status, resp.header, resp.body)

	return resp
}

// send is post without the contract check — for bodies the schema itself
// rejects (the validator would refuse the request before the response) and
// for the undocumented 504.
func (hrn *harness) send(t testing.TB, body string) response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, hrn.ts.URL+"/events", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = res.Body.Close() }()

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return response{status: res.StatusCode, header: res.Header, body: raw}
}

func TestIngest_AcceptsABatchAndKeepsEveryEventByteForByte(t *testing.T) {
	t.Parallel()

	hrn := newHarness(t)

	// Odd spacing and key order on purpose: what is stored is what was sent.
	first := `{ "payload": {"b":1,  "a":[1,2,{"z":null}]}, "id":"0193a1f0-7c1e-7000-8000-000000000001", "type":"page.viewed" }`
	second := `{"id":"e_2","payload":{}}`

	resp := hrn.post(t, `{"events":[`+first+`,`+second+`]}`)
	require.Equal(t, http.StatusOK, resp.status, string(resp.body))

	var out ingestapi.IngestResponse
	require.NoError(t, json.Unmarshal(resp.body, &out))
	assert.Equal(t, int32(2), out.Accepted)

	got := hrn.ingester.last(t)
	require.Len(t, got, 2)
	assert.Equal(t, "0193a1f0-7c1e-7000-8000-000000000001", got[0].ID)
	assert.Equal(t, "page.viewed", got[0].Type)
	assert.Equal(t, first, string(got[0].Raw)) // byte for byte: a re-encoding would not pass
	assert.Equal(t, "e_2", got[1].ID)
	assert.Empty(t, got[1].Type)
	assert.Equal(t, second, string(got[1].Raw))
}

// What the schema calls invalid is a 400 with a stable code, and never
// reaches the pipeline. (Schemathesis generates the long tail; these are the
// rules the handler implements by hand because it does not decode into the
// generated types.)
func TestIngest_RejectsWhatTheContractRejects(t *testing.T) {
	t.Parallel()

	hrn := newHarness(t)

	for name, tc := range map[string]struct{ body, code string }{
		"not json":            {`{"events":`, ""},
		"not an object":       {`[]`, ""},
		"null":                {`null`, "invalid_body"},
		"no events":           {`{}`, "invalid_body"},
		"extra top field":     {`{"events":[{"id":"a","payload":{}}],"x":1}`, "invalid_body"},
		"events not an array": {`{"events":{}}`, "invalid_body"},
		"events null":         {`{"events":null}`, "invalid_body"},
		"empty batch":         {`{"events":[]}`, "invalid_body"},
		"too many events":     {`{"events":[{"id":"a","payload":{}},{"id":"b","payload":{}},{"id":"c","payload":{}},{"id":"d","payload":{}}]}`, "invalid_body"},
		"event not an object": {`{"events":["a"]}`, "invalid_event"},
		"event null":          {`{"events":[null]}`, "invalid_event"},
		"no id":               {`{"events":[{"payload":{}}]}`, "invalid_event"},
		"id not a string":     {`{"events":[{"id":7,"payload":{}}]}`, "invalid_event"},
		"empty id":            {`{"events":[{"id":"","payload":{}}]}`, "invalid_event"},
		"id with a space":     {`{"events":[{"id":"a b","payload":{}}]}`, "invalid_event"},
		"id with a slash":     {`{"events":[{"id":"a/b","payload":{}}]}`, "invalid_event"},
		"id too long":         {`{"events":[{"id":"` + strings.Repeat("a", 65) + `","payload":{}}]}`, "invalid_event"},
		"no payload":          {`{"events":[{"id":"a"}]}`, "invalid_event"},
		"payload not object":  {`{"events":[{"id":"a","payload":[1]}]}`, "invalid_event"},
		"payload null":        {`{"events":[{"id":"a","payload":null}]}`, "invalid_event"},
		"type upper case":     {`{"events":[{"id":"a","type":"Page","payload":{}}]}`, "invalid_event"},
		"type null":           {`{"events":[{"id":"a","type":null,"payload":{}}]}`, "invalid_event"},
		"type empty":          {`{"events":[{"id":"a","type":"","payload":{}}]}`, "invalid_event"},
		"unknown event field": {`{"events":[{"id":"a","payload":{},"ts":1}]}`, "invalid_event"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			resp := hrn.send(t, tc.body)
			require.Equal(t, http.StatusBadRequest, resp.status, string(resp.body))

			if p := resp.problem(t); tc.code != "" {
				assert.Equal(t, tc.code, p["code"])
			}
		})
	}

	resp := hrn.send(t, `{"events":[{"id":"ok","payload":{}},{"id":"second bad","payload":{}}]}`)
	assert.Contains(t, resp.problem(t)["detail"], "events[1]", "the problem names the offending event")
}

func TestIngest_InvalidBatchesNeverReachThePipeline(t *testing.T) {
	t.Parallel()

	hrn := newHarness(t)

	for _, body := range []string{`{}`, `{"events":[]}`, `{"events":[{"id":"a"}]}`, `{"events":[{"id":"ok","payload":{}},{"id":"","payload":{}}]}`} {
		require.Equal(t, http.StatusBadRequest, hrn.send(t, body).status)
	}

	assert.Zero(t, hrn.ingester.calls(), "a batch is accepted whole or not at all")
}

func TestIngest_SizeLimits(t *testing.T) {
	t.Parallel()

	hrn := newHarness(t, func(c *httpx.Config) { c.MaxBodyBytes = 1024 })

	big := fmt.Sprintf(`{"events":[{"id":"a","payload":{"x":"%s"}}]}`, strings.Repeat("x", 300))
	resp := hrn.post(t, big)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.status)
	assert.Equal(t, "event_too_large", resp.problem(t)["code"])

	huge := fmt.Sprintf(`{"events":[{"id":"a","payload":{"x":"%s"}}]}`, strings.Repeat("x", 2000))
	resp = hrn.post(t, huge)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.status, "the body limit of httpx")

	assert.Zero(t, hrn.ingester.calls())
}

func TestIngest_PipelineOutcomesBecomeRetryableProblems(t *testing.T) {
	t.Parallel()

	hrn := newHarness(t)
	body := `{"events":[{"id":"a","payload":{}}]}`

	hrn.ingester.fail(pipeline.ErrOverloaded)

	resp := hrn.post(t, body)
	require.Equal(t, http.StatusTooManyRequests, resp.status)
	assert.Equal(t, "2", resp.header.Get("Retry-After"))
	assert.Equal(t, "overloaded", resp.problem(t)["code"])

	hrn.ingester.fail(fmt.Errorf("%w: kafka: broker down; archive: put failed", pipeline.ErrNoSink))

	resp = hrn.post(t, body)
	require.Equal(t, http.StatusServiceUnavailable, resp.status)
	assert.Equal(t, "2", resp.header.Get("Retry-After"))
	assert.Equal(t, "no_sink_available", resp.problem(t)["code"])
	assert.NotContains(t, string(resp.body), "broker down", "the cause is for the log, not the client")
	assert.Contains(t, hrn.log.String(), "broker down", "a 5xx is logged with its cause")
}

func TestIngest_RequestDeadlineIsA504(t *testing.T) {
	t.Parallel()

	hrn := newHarness(t, func(c *httpx.Config) {
		c.RequestTimeout = 50 * time.Millisecond
		c.RequestTimeoutMin = 0
	})
	hrn.ingester.onCall = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	resp := hrn.send(t, `{"events":[{"id":"a","payload":{}}]}`)
	assert.Equal(t, http.StatusGatewayTimeout, resp.status, string(resp.body))
}
