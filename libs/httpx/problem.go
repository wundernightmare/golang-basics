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

// MarshalJSON renders the problem as a flat JSON object with the standard
// members plus any extensions, per RFC 9457 §3.
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
		title = http.StatusText(p.Status)
	}
	m["type"] = typ
	m["title"] = title
	m["status"] = p.Status
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
func AbortProblem(c *gin.Context, p Problem) {
	body, err := json.Marshal(p)
	if err != nil {
		// Marshalling a Problem cannot realistically fail; degrade safely.
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	c.Abort()
	c.Data(p.Status, ProblemContentType, body)
}
