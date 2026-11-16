package s3_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/s3"
	"github.com/wundernightmare/golang-basics/libs/secret"
)

const (
	completed    = `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><ETag>"e"</ETag></CompleteMultipartUploadResult>`
	noSuchKey    = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>no key</Message></Error>`
	noSuchBucket = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>no bucket</Message></Error>`

	// short is a deadline a test waits out; long is one it must never reach.
	short = 200 * time.Millisecond
	long  = 30 * time.Second
)

// fakeStore is an HTTP server that answers every S3 call the way a healthy
// store does, unless the test replaced the answer of that operation. It
// counts what it was asked, by the library's operation names.
type fakeStore struct {
	url  string
	stop chan struct{}

	mu   sync.Mutex
	seen map[string]int
	on   map[string]http.HandlerFunc
}

func newFakeStore(t *testing.T) *fakeStore {
	t.Helper()

	f := &fakeStore{stop: make(chan struct{}), seen: map[string]int{}, on: map[string]http.HandlerFunc{}}

	srv := httptest.NewServer(f)
	f.url = srv.URL

	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(f.stop) }) // first: Close waits for the handlers hang parked

	return f
}

// operation names the S3 call a request of a path-style client is.
func operation(r *http.Request) string {
	query := r.URL.Query()
	bucket := strings.Count(strings.Trim(r.URL.Path, "/"), "/") == 0

	switch {
	case r.Method == http.MethodPost && query.Has("uploads"):
		return "create_upload"
	case r.Method == http.MethodPut && query.Has("partNumber"):
		return "upload_part"
	case r.Method == http.MethodPost && query.Has("uploadId"):
		return "complete_upload"
	case r.Method == http.MethodDelete && query.Has("uploadId"):
		return "abort_upload"
	case r.Method == http.MethodPost && query.Has("delete"):
		return "delete"
	case r.Method == http.MethodHead && bucket:
		return "head_bucket"
	case r.Method == http.MethodHead:
		return "head"
	case r.Method == http.MethodGet:
		return "get"
	default:
		return "put"
	}
}

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op := operation(r)

	f.mu.Lock()
	f.seen[op]++
	handler := f.on[op]
	f.mu.Unlock()

	if handler != nil {
		handler(w, r)
		return
	}

	_, _ = io.Copy(io.Discard, r.Body)

	switch op {
	case "create_upload":
		xml(w, http.StatusOK, initiated)
	case "upload_part":
		w.Header().Set("ETag", `"etag"`)
	case "complete_upload":
		xml(w, http.StatusOK, completed)
	case "abort_upload":
		w.WriteHeader(http.StatusNoContent)
	case "delete":
		xml(w, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>`)
	case "get":
		_, _ = w.Write([]byte("hello"))
	case "head":
		w.Header().Set("Content-Length", "5")
	}
}

func xml(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// answer replaces what the store says to one operation.
func (f *fakeStore) answer(op string, handler http.HandlerFunc) *fakeStore {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.on[op] = handler

	return f
}

func (f *fakeStore) fails(op string) *fakeStore {
	return f.answer(op, func(w http.ResponseWriter, _ *http.Request) { fail(w) })
}

// hangs makes the store take the request of op and never answer it.
func (f *fakeStore) hangs(op string) *fakeStore {
	return f.answer(op, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // the server notices a caller that left only once the body is read

		select {
		case <-r.Context().Done():
		case <-f.stop:
		}
	})
}

func (f *fakeStore) calls(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.seen[op]
}

// client is a client of the store: one attempt per call and deadlines no
// test reaches, unless mutate says otherwise.
func (f *fakeStore) client(t *testing.T, mutate ...func(*s3.Config)) *s3.Client {
	t.Helper()

	return clientOf(t, f.url, nil, mutate...)
}

func clientOf(t *testing.T, endpoint string, opts []s3.Option, mutate ...func(*s3.Config)) *s3.Client {
	t.Helper()

	cfg := s3.Config{
		Bucket: "b", Endpoint: endpoint, Insecure: true,
		AccessKeyID: "k", SecretAccessKey: secret.New("s"), MaxAttempts: 1,
		OpTimeout: long, TransferTimeout: long, UploadPartBytes: s3.MinUploadPartBytes,
	}
	for _, m := range mutate {
		m(&cfg)
	}

	client, err := s3.New(context.Background(), cfg, nil, opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}
