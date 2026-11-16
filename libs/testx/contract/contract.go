// Package contract validates HTTP exchanges against an OpenAPI document and
// events against a JSON Schema — the way a handler or producer test proves it
// honours the contract, not just its own expectations. Its own module, so
// only contract tests carry kin-openapi.
//
// The documents are the caller's and are named by file path. `go test` runs a
// test in its package's directory, so a relative path is relative to the test
// file: "../../../../api/openapi3/tasks.openapi.yaml" from a service of this
// repository, whose contracts are emitted from api/tsp.
package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

// OpenAPI validates HTTP exchanges against an OpenAPI document.
type OpenAPI struct {
	doc    *openapi3.T
	router routers.Router
}

// LoadOpenAPI loads and validates the document at path.
func LoadOpenAPI(tb testing.TB, path string) *OpenAPI {
	tb.Helper()

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(path)
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

// JSONSchema validates documents against a JSON Schema (draft 2020-12).
type JSONSchema struct{ schema *jsonschema.Schema }

// LoadJSONSchema compiles the schema at path.
func LoadJSONSchema(tb testing.TB, path string) *JSONSchema {
	tb.Helper()

	c := jsonschema.NewCompiler()
	// In draft 2020-12 `format` is an annotation unless the validator is
	// told to assert it; a contract that says date-time means it.
	c.AssertFormat()

	s, err := c.Compile(path)
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
