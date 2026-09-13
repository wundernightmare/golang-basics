package httpx

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ProblemContentType is the media type for RFC 9457 problem details.
const ProblemContentType = "application/problem+json"

// Problem is an RFC 9457 (Problem Details for HTTP APIs) error body. It gives
// every service one machine-readable error shape instead of ad-hoc
// {"error": "..."} JSON. Type defaults to "about:blank" and Title to the HTTP
// status text when left empty. Extensions are merged in as top-level members,
// as the RFC allows (e.g. "code", "errors", "trace_id").
type Problem struct {
	Type       string         // URI identifying the problem type (default "about:blank")
	Title      string         // short, human-readable summary (default: status text)
	Status     int            // HTTP status code
	Detail     string         // human-readable explanation specific to this occurrence
	Instance   string         // URI identifying this specific occurrence
	Extensions map[string]any // additional top-level members
}

// status returns the HTTP status the problem will be written with: the one
// set, or 500 when it is not a valid HTTP status code. Without this a stray
// value (0 from a zero Problem, an errno, a typo) reaches WriteHeader, which
// panics on anything outside 100–999 — found by FuzzProblemJSON.
func (p Problem) status() int {
	if p.Status < 100 || p.Status > 999 {
		return http.StatusInternalServerError
	}
	return p.Status
}

// MarshalJSON renders the problem as a flat JSON object with the standard
// members plus any extensions, per RFC 9457 §3. The status member is the one
// the response carries (see status).
func (p Problem) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(p.Extensions)+5)
	for k, v := range p.Extensions {
		m[k] = v
	}
	typ := p.Type
	if typ == "" {
		typ = "about:blank"
	}
	title := p.Title
	if title == "" {
		title = http.StatusText(p.status())
	}
	m["type"] = typ
	m["title"] = title
	m["status"] = p.status()
	if p.Detail != "" {
		m["detail"] = p.Detail
	}
	if p.Instance != "" {
		m["instance"] = p.Instance
	}
	return json.Marshal(m)
}

// NewProblem builds a [Problem] for status with an occurrence-specific detail,
// defaulting Type to "about:blank" and Title to the status text.
func NewProblem(status int, detail string) Problem {
	return Problem{Status: status, Detail: detail}
}

// AbortProblem writes p as application/problem+json and aborts the gin handler
// chain. Use it from handlers and from the error-mapping layer so failures are
// always returned in the RFC 9457 shape.
//
// Two members are filled in from the request when the caller left them out:
// Instance becomes the request path, and the request_id extension carries the
// id the server echoed in X-Request-Id — so an error body alone is enough to
// find its log lines.
func AbortProblem(c *gin.Context, p Problem) {
	if c.Request != nil {
		if p.Instance == "" && c.Request.URL != nil {
			p.Instance = c.Request.URL.Path
		}
		p = withRequestIDExtension(p, RequestIDFromContext(c.Request.Context()))
	}
	body, err := json.Marshal(p)
	if err != nil {
		// Marshalling a Problem cannot realistically fail; degrade safely.
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	c.Abort()
	c.Data(p.status(), ProblemContentType, body)
}

// withRequestIDExtension adds request_id to p's extensions unless the caller
// set one or id is empty. The caller's map is never mutated.
func withRequestIDExtension(p Problem, id string) Problem {
	if id == "" {
		return p
	}
	if _, set := p.Extensions["request_id"]; set {
		return p
	}
	ext := make(map[string]any, len(p.Extensions)+1)
	for k, v := range p.Extensions {
		ext[k] = v
	}
	ext["request_id"] = id
	p.Extensions = ext
	return p
}

// writeProblem is AbortProblem for the plain net/http admin mux.
func writeProblem(w http.ResponseWriter, p Problem) {
	body, err := json.Marshal(p)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", ProblemContentType)
	w.WriteHeader(p.status())
	_, _ = w.Write(body)
}
