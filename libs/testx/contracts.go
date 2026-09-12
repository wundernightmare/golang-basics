package testx

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

// workspaceRoot locates the repository root from any module: contracts live
// under api/ at the root, and tests reference them by that path.
func workspaceRoot(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	root := filepath.Dir(file) // libs/testx
	for range 6 {
		if _, err := os.Stat(filepath.Join(root, "go.work")); err == nil {
			return root
		}
		root = filepath.Dir(root)
	}
	tb.Fatal("testx: go.work not found above " + file)
	return ""
}

// ContractPath returns the absolute path of a file under api/ — the
// TypeSpec-emitted contracts (api/openapi3/…, api/jsonschema/…).
func ContractPath(tb testing.TB, rel string) string {
	tb.Helper()
	return filepath.Join(workspaceRoot(tb), "api", rel)
}

// OpenAPI validates HTTP exchanges against an OpenAPI document — the way a
// handler test proves it honours the contract (api/tsp/tasks.tsp), not just
// its own expectations.
type OpenAPI struct {
	doc    *openapi3.T
	router routers.Router
}

// LoadOpenAPI loads and validates the document at api/<rel>.
func LoadOpenAPI(tb testing.TB, rel string) *OpenAPI {
	tb.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(ContractPath(tb, rel))
	require.NoError(tb, err)
	require.NoError(tb, doc.Validate(context.Background()))
	// Route by path only: the document's `servers` name the deployment
	// (localhost:8082), tests talk to an httptest listener on a random port.
	doc.Servers = nil
	router, err := gorillamux.NewRouter(doc)
	require.NoError(tb, err)
	return &OpenAPI{doc: doc, router: router}
}

// Validate checks that req matched an operation and that the recorded
// response (status, headers, body) conforms to that operation's schema.
// reqBody is the request body as sent (the request's Body has been consumed).
func (o *OpenAPI) Validate(tb testing.TB, req *http.Request, reqBody []byte, status int, header http.Header, body []byte) {
	tb.Helper()
	route, pathParams, err := o.router.FindRoute(req)
	require.NoErrorf(tb, err, "%s %s is not in the contract", req.Method, req.URL.Path)

	req.Body = io.NopCloser(bytes.NewReader(reqBody))
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: pathParams, Route: route}
	if err := openapi3filter.ValidateRequest(context.Background(), in); err != nil {
		// A request the contract rejects must be answered with a problem, which
		// the response validation below checks; the request itself is allowed
		// to be invalid (that is what 4xx tests send).
		require.GreaterOrEqualf(tb, status, 400, "contract rejects the request (%v) but the server answered %d", err, status)
	}
	out := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in,
		Status:                 status,
		Header:                 header,
		Body:                   io.NopCloser(bytes.NewReader(body)),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true},
	}
	require.NoErrorf(tb, openapi3filter.ValidateResponse(context.Background(), out),
		"%s %s → %d does not match the contract\n%s", req.Method, req.URL.Path, status, body)
}

// JSONSchema validates documents against a JSON Schema (draft 2020-12) — the
// way an event test proves the bytes on the wire match api/tsp/events.tsp.
type JSONSchema struct{ schema *jsonschema.Schema }

// LoadJSONSchema compiles the schema at api/<rel>.
func LoadJSONSchema(tb testing.TB, rel string) *JSONSchema {
	tb.Helper()
	c := jsonschema.NewCompiler()
	s, err := c.Compile(ContractPath(tb, rel))
	require.NoError(tb, err)
	return &JSONSchema{schema: s}
}

// Validate fails the test unless doc (raw JSON) conforms to the schema.
func (s *JSONSchema) Validate(tb testing.TB, doc []byte) {
	tb.Helper()
	var v any
	require.NoError(tb, json.Unmarshal(doc, &v), "not JSON: %s", doc)
	require.NoErrorf(tb, s.schema.Validate(v), "event does not match the schema\n%s", doc)
}
