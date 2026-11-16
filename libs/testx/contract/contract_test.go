package contract_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/tracehubmmp/golang-basics/libs/testx/contract"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// fails reports whether check failed the testing.TB it was given, without
// failing the test that asks.
func fails(t *testing.T, check func(tb testing.TB)) bool {
	t.Helper()

	tb := &recorder{TB: t}

	func() {
		defer func() {
			if r := recover(); r != nil && r != any(failedNow{}) {
				panic(r)
			}
		}()

		check(tb)
	}()

	return tb.failed
}

type recorder struct {
	testing.TB

	failed bool
}

type failedNow struct{}

func (r *recorder) Errorf(string, ...any) { r.failed = true }

func (r *recorder) FailNow() { r.failed = true; panic(failedNow{}) }

// The documents are the caller's: a path relative to the test's directory.
func TestJSONSchema(t *testing.T) {
	t.Parallel()

	schema := contract.LoadJSONSchema(t, "testdata/NoteWritten.json")

	for name, tc := range map[string]struct {
		doc   string
		fails bool
	}{
		"conforms":                 {doc: `{"id":"a","written_at":"2026-11-16T10:00:00Z"}`},
		"a required field missing": {doc: `{"id":"a"}`, fails: true},
		"a format not honoured":    {doc: `{"id":"a","written_at":"yesterday"}`, fails: true},
		"not JSON":                 {doc: `{"id":`, fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.fails, fails(t, func(tb testing.TB) {
				tb.Helper()
				schema.Validate(tb, []byte(tc.doc))
			}))
		})
	}
}

func TestOpenAPI(t *testing.T) {
	t.Parallel()

	oas := contract.LoadOpenAPI(t, "testdata/notes.openapi.yaml")
	json := http.Header{"Content-Type": {"application/json"}}

	for name, tc := range map[string]struct {
		target string
		status int
		header http.Header
		body   string
		fails  bool
	}{
		"conforms":                {target: "/notes/1", status: 200, header: json, body: `{"id":"1","written_at":"2026-11-16T10:00:00Z"}`},
		"an undocumented path":    {target: "/drafts/1", status: 200, header: json, body: `{}`, fails: true},
		"an undocumented status":  {target: "/notes/1", status: 418, header: json, body: `{}`, fails: true},
		"a body off the schema":   {target: "/notes/1", status: 200, header: json, body: `{"id":"1","written_at":"yesterday"}`, fails: true},
		"an undocumented content": {target: "/notes/1", status: 200, header: http.Header{"Content-Type": {"text/plain"}}, body: `1`, fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.target, http.NoBody)

			assert.Equal(t, tc.fails, fails(t, func(tb testing.TB) {
				tb.Helper()
				oas.Validate(tb, req, nil, tc.status, tc.header, []byte(tc.body))
			}))
		})
	}
}
