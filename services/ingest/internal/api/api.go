// Package api holds the ingest service's one route. The handler validates
// the batch against the contract (api/tsp/ingest.tsp) and hands it to the
// pipeline; whether, where and how fast it becomes durable is the pipeline's
// business (internal/pipeline).
//
// The request is not decoded into the generated ingestapi.Event: that would
// parse every payload into a map and re-encode it, and the service's promise
// is to store an event byte for byte as it was sent. The body is split into
// raw JSON values instead, and only the envelope fields are looked at.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/contracts/ingestapi"
	"github.com/tracehubmmp/golang-basics/libs/httpx"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/pipeline"
)

// Ingester is the write path (satisfied by pipeline.Pipeline).
type Ingester interface {
	Ingest(ctx context.Context, events []pipeline.Event) error
}

// Deps bundles everything the handler needs.
type Deps struct {
	Ingester Ingester
	Logger   *slog.Logger
	// MaxEventBytes is the largest single event accepted (413 above it).
	MaxEventBytes int
	// MaxBatchEvents is the most events in one request (400 above it).
	MaxBatchEvents int
	// RetryAfter is the Retry-After of a 429 / 503.
	RetryAfter time.Duration
}

type handlers struct {
	Deps
	retryAfter string
}

// Register attaches the ingest route to the server's mux.
func Register(srv *httpx.Server, deps Deps) {
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}

	h := &handlers{Deps: deps, retryAfter: strconv.Itoa(max(int(deps.RetryAfter.Round(time.Second)/time.Second), 1))}
	srv.Mux().HandleFunc("POST /events", h.ingest)
}

// Stable problem codes.
const (
	codeInvalidBody   = "invalid_body"
	codeInvalidEvent  = "invalid_event"
	codeEventTooLarge = "event_too_large"
	codeOverloaded    = "overloaded"
	codeNoSink        = "no_sink_available"
)

func (h *handlers) ingest(w http.ResponseWriter, r *http.Request) {
	events, err := h.decode(r)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	err = h.Ingester.Ingest(r.Context(), events)

	switch {
	case err == nil:
		httpx.WriteJSON(w, http.StatusOK, ingestapi.IngestResponse{Accepted: int32(len(events))}) //nolint:gosec // bounded by MaxBatchEvents
	case errors.Is(err, pipeline.ErrOverloaded):
		w.Header().Set("Retry-After", h.retryAfter)
		httpx.WriteProblem(w, r, problem(http.StatusTooManyRequests, codeOverloaded,
			"the service is holding as much unconfirmed data as it may; send the batch again shortly"))
	case errors.Is(err, pipeline.ErrNoSink):
		w.Header().Set("Retry-After", h.retryAfter)
		httpx.WriteProblem(w, r, problem(http.StatusServiceUnavailable, codeNoSink,
			"the batch could not be stored; send it again").WithCause(err))
	default:
		// The request's deadline (504) or the client going away (499). The
		// sinks may still store the batch: a retry is safe, by event id.
		httpx.WriteError(w, r, err)
	}
}

func problem(status int, code, detail string) httpx.Problem {
	return httpx.Problem{Status: status, Detail: detail, Extensions: map[string]any{"code": code}}
}

func invalid(code, format string, args ...any) httpx.Problem {
	return problem(http.StatusBadRequest, code, fmt.Sprintf(format, args...))
}

// decode reads and validates the body: exactly the documented shape, nothing
// more (the contract closes both objects), so that what the schema calls
// invalid is a 400 here too.
func (h *handlers) decode(r *http.Request) ([]pipeline.Event, error) {
	var body map[string]json.RawMessage
	if err := httpx.DecodeJSON(r, &body); err != nil {
		return nil, err
	}

	if body == nil {
		return nil, invalid(codeInvalidBody, "the body must be a JSON object")
	}

	rawEvents, ok := body["events"]
	if !ok || len(body) != 1 {
		return nil, invalid(codeInvalidBody, `the body must be {"events": [...]} and nothing else`)
	}

	var items []json.RawMessage
	if err := json.Unmarshal(rawEvents, &items); err != nil || items == nil {
		return nil, invalid(codeInvalidBody, `"events" must be an array`)
	}

	if len(items) == 0 || len(items) > h.MaxBatchEvents {
		return nil, invalid(codeInvalidBody, `"events" must hold 1..%d events, got %d`, h.MaxBatchEvents, len(items))
	}

	events := make([]pipeline.Event, len(items))

	for i, item := range items {
		event, err := h.event(item)
		if err != nil {
			if p, ok := errors.AsType[httpx.Problem](err); ok {
				p.Detail = fmt.Sprintf("events[%d]: %s", i, p.Detail)
				return nil, p
			}

			return nil, err
		}

		events[i] = event
	}

	return events, nil
}

func (h *handlers) event(raw json.RawMessage) (pipeline.Event, error) {
	if len(raw) > h.MaxEventBytes {
		return pipeline.Event{}, problem(http.StatusRequestEntityTooLarge, codeEventTooLarge,
			fmt.Sprintf("the event is %d bytes, the limit is %d", len(raw), h.MaxEventBytes))
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return pipeline.Event{}, invalid(codeInvalidEvent, "an event must be a JSON object")
	}

	event := pipeline.Event{Raw: raw}

	for name, value := range fields {
		switch name {
		case "id":
			if err := json.Unmarshal(value, &event.ID); err != nil || !validID(event.ID) {
				return pipeline.Event{}, invalid(codeInvalidEvent, `"id" must be a string of 1..64 characters from A-Z a-z 0-9 _ -`)
			}
		case "type":
			if err := json.Unmarshal(value, &event.Type); err != nil || !validType(event.Type) || isNull(value) {
				return pipeline.Event{}, invalid(codeInvalidEvent, `"type" must be a string of 1..64 characters from a-z 0-9 . _ -`)
			}
		case "payload":
			if trimmed := bytes.TrimLeft(value, " \t\r\n"); len(trimmed) == 0 || trimmed[0] != '{' {
				return pipeline.Event{}, invalid(codeInvalidEvent, `"payload" must be a JSON object`)
			}
		default:
			return pipeline.Event{}, invalid(codeInvalidEvent, "unknown field %q: an event is id, type and payload", name)
		}
	}

	if _, ok := fields["payload"]; !ok || event.ID == "" {
		return pipeline.Event{}, invalid(codeInvalidEvent, `"id" and "payload" are required`)
	}

	return event, nil
}

func isNull(value json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(value), []byte("null")) }

const maxFieldLength = 64

// validID is the contract's ^[A-Za-z0-9_-]{1,64}$: every UUID, ULID and
// base64url id, and nothing that needs escaping in a key, a header or a log.
func validID(id string) bool {
	if id == "" || len(id) > maxFieldLength {
		return false
	}

	for i := range len(id) {
		c := id[i]

		alnum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !alnum && c != '_' && c != '-' {
			return false
		}
	}

	return true
}

// validType is the contract's ^[a-z0-9._-]{1,64}$.
func validType(name string) bool {
	if name == "" || len(name) > maxFieldLength {
		return false
	}

	for i := range len(name) {
		c := name[i]

		lower := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !lower && c != '.' && c != '_' && c != '-' {
			return false
		}
	}

	return true
}
