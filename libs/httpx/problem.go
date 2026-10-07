package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ProblemContentType is the media type for RFC 9457 problem details.
const ProblemContentType = "application/problem+json"

// Problem is an RFC 9457 (Problem Details for HTTP APIs) error body. It gives
// every service one machine-readable error shape instead of ad-hoc
// {"error": "..."} JSON. Type defaults to "about:blank" and Title to the HTTP
// status text when left empty. Extensions are merged in as top-level members,
// as the RFC allows (e.g. "code", "errors", "trace_id").
//
// Problem is also an error, so a store or service layer can return one and
// the handler can pass whatever it got to [WriteError]. A 5xx problem should
// carry the underlying error through [Problem.WithCause] (or [Internal]): the
// cause is what gets logged and recorded on the span, while the client only
// sees Detail.
type Problem struct {
	Type       string         // URI identifying the problem type (default "about:blank")
	Title      string         // short, human-readable summary (default: status text)
	Status     int            // HTTP status code
	Detail     string         // human-readable explanation specific to this occurrence
	Instance   string         // URI identifying this specific occurrence
	Extensions map[string]any // additional top-level members

	cause error // logged with a 5xx, never sent
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

// Error implements error: the status text and detail, plus the cause when
// there is one, so a Problem reads naturally in a log line or a test failure.
func (p Problem) Error() string {
	var b strings.Builder
	b.WriteString(http.StatusText(p.status()))
	if p.Detail != "" {
		b.WriteString(": ")
		b.WriteString(p.Detail)
	}
	if p.cause != nil {
		b.WriteString(" (")
		b.WriteString(p.cause.Error())
		b.WriteString(")")
	}
	return b.String()
}

// Unwrap returns the cause, so errors.Is / errors.As see through a Problem.
func (p Problem) Unwrap() error { return p.cause }

// WithCause attaches the underlying error. [WriteProblem] logs it (and
// records it on the active span) when the status is 5xx; the client never
// sees it.
func (p Problem) WithCause(err error) Problem {
	p.cause = err
	return p
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

// Internal is the 500 problem: detail for the client, cause for the log.
func Internal(detail string, cause error) Problem {
	return NewProblem(http.StatusInternalServerError, detail).WithCause(cause)
}

// WriteProblem writes p as application/problem+json. Use it from handlers and
// from the error-mapping layer so failures are always returned in the RFC
// 9457 shape.
//
// Two members are filled in from the request when the caller left them out:
// Instance becomes the request path, and the request_id extension carries the
// id the server echoed in X-Request-Id — so an error body alone is enough to
// find its log lines. A 5xx is also logged at error level with its cause and
// recorded on the request's span: the client gets "could not persist task",
// the operator gets the pgconn error.
func WriteProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	if r != nil {
		if p.Instance == "" && r.URL != nil {
			p.Instance = r.URL.Path
		}
		p = withRequestIDExtension(p, RequestIDFromContext(r.Context()))
		if p.status() >= http.StatusInternalServerError {
			reportFailure(r.Context(), p)
		}
	}
	writeProblem(w, r, p)
}

// WriteError answers with err in problem shape: a [Problem] (possibly
// wrapped) is written as is; a context deadline is a 504; anything else is a
// 500 with err as the cause, so no error is ever swallowed on the way to a
// generic response.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var p Problem
	switch {
	case errors.As(err, &p):
	case errors.Is(err, context.DeadlineExceeded):
		p = NewProblem(http.StatusGatewayTimeout, "the request took too long").WithCause(err)
	default:
		p = Internal("internal error", err)
	}
	WriteProblem(w, r, p)
}

// reportFailure logs a 5xx with its cause and marks the span, with the
// request's logger and context so trace_id / request_id are attached.
func reportFailure(ctx context.Context, p Problem) {
	log := slog.Default()
	if info := infoFrom(ctx); info != nil && info.log != nil {
		log = info.log
	}
	attrs := []slog.Attr{slog.Int("status", p.status()), slog.String("detail", p.Detail)}
	if p.cause != nil {
		attrs = append(attrs, slog.Any("err", p.cause))
	}
	log.LogAttrs(ctx, slog.LevelError, "request failed", attrs...)

	span := trace.SpanFromContext(ctx)
	if p.cause != nil {
		span.RecordError(p.cause)
	}
	span.SetStatus(codes.Error, p.Detail)
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

// writeProblem encodes and writes p; the request is only used to attach the
// request_id extension when it was not already filled in.
func writeProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	if r != nil && p.Instance == "" && r.URL != nil {
		p.Instance = r.URL.Path
		p = withRequestIDExtension(p, RequestIDFromContext(r.Context()))
	}
	body, err := json.Marshal(p)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", ProblemContentType)
	w.WriteHeader(p.status())
	_, _ = w.Write(body)
}

// WriteJSON writes v as application/json with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// DecodeJSON decodes the request body into v. It returns a [Problem]: 413
// when the body exceeded Config.MaxBodyBytes, 400 when it is not the JSON the
// caller expects (a syntax error, a wrong type, trailing data), so a handler
// can pass the error straight to [WriteError].
func DecodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return NewProblem(http.StatusRequestEntityTooLarge, "request body is too large").WithCause(err)
		}
		return NewProblem(http.StatusBadRequest, "invalid JSON body").WithCause(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return NewProblem(http.StatusBadRequest, "request body must contain a single JSON value")
	}
	return nil
}
