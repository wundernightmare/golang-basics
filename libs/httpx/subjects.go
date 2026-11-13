package httpx

import (
	"context"
	"maps"
	"net/http"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DebugSubjectAttribute is the span attribute a request of a subject under
// investigation carries. A tail-sampling collector keeps every trace that has
// it (docker/otel-collector.yaml).
const DebugSubjectAttribute = "debug.subject"

// maxDebugSubjects bounds the list: it is a debugging switch for a handful of
// tenants or users at a time, not a feature flag store.
const maxDebugSubjects = 100

// debugSubjects is the set of subjects — tenant ids, user ids, whatever the
// service identifies its callers by — whose requests are logged at debug
// level and marked for tracing, for a limited time. It is the "what is wrong
// with this one customer" switch: no restart, no debug logs for everyone.
//
// The operator adds a subject on the admin port:
//
//	PUT    /admin/debug-subjects/{subject}?ttl=15m
//	DELETE /admin/debug-subjects/{subject}
//	GET    /admin/debug-subjects
//
// and the service, once it knows who is calling (in its authenticator),
// says so:
//
//	ctx = srv.DebugSubject(ctx, tenantID)
//
// The set lives in this process: with several replicas, add the subject on
// each (or through whatever fans an admin call out to all pods).
type debugSubjects struct {
	mu      sync.Mutex
	expires map[string]time.Time
}

func (d *debugSubjects) add(subject string, until time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.expires == nil {
		d.expires = map[string]time.Time{}
	}

	if _, known := d.expires[subject]; !known && len(d.expires) >= maxDebugSubjects {
		d.sweep(time.Now())

		if len(d.expires) >= maxDebugSubjects {
			return false
		}
	}

	d.expires[subject] = until

	return true
}

func (d *debugSubjects) remove(subject string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.expires, subject)
}

func (d *debugSubjects) sweep(now time.Time) {
	for subject, until := range d.expires {
		if !now.Before(until) {
			delete(d.expires, subject)
		}
	}
}

func (d *debugSubjects) active(subject string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	until, ok := d.expires[subject]
	if !ok {
		return false
	}

	if !time.Now().Before(until) {
		delete(d.expires, subject)

		return false
	}

	return true
}

func (d *debugSubjects) list() map[string]time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.sweep(time.Now())

	out := make(map[string]time.Time, len(d.expires))
	maps.Copy(out, d.expires)

	return out
}

// DebugSubject tells the server who a unit of work is for. When subject is
// under investigation (see PUT /admin/debug-subjects) the returned context
// logs at debug level, unsampled ([WithDebugLogging]), and the current span
// gets the [DebugSubjectAttribute] attribute so a tail-sampling collector
// keeps the trace; otherwise ctx is returned as it is. The check is a map
// lookup: call it for every request.
//
// A trace can only be kept if it was recorded: with head sampling below 1.0
// (OTEL_TRACES_SAMPLER_RATIO) the requests the head sampler dropped are not
// brought back by this.
func (s *Server) DebugSubject(ctx context.Context, subject string) context.Context {
	if subject == "" || !s.subjects.active(subject) {
		return ctx
	}

	trace.SpanFromContext(ctx).SetAttributes(attribute.String(DebugSubjectAttribute, subject))

	return WithDebugLogging(ctx)
}

type debugSubjectsResponse struct {
	Subjects []debugSubjectEntry `json:"subjects"`
	MaxTTL   string              `json:"max_ttl"`
}

type debugSubjectEntry struct {
	Subject   string    `json:"subject"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Server) debugSubjectsResponse() debugSubjectsResponse {
	resp := debugSubjectsResponse{Subjects: []debugSubjectEntry{}, MaxTTL: s.cfg.LogLevelMaxTTL.String()}
	for subject, until := range s.subjects.list() {
		resp.Subjects = append(resp.Subjects, debugSubjectEntry{Subject: subject, ExpiresAt: until})
	}

	sort.Slice(resp.Subjects, func(i, j int) bool { return resp.Subjects[i].Subject < resp.Subjects[j].Subject })

	return resp
}

func (s *Server) getDebugSubjects(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, s.debugSubjectsResponse())
}

// putDebugSubject adds (or extends) a subject. ttl is required and capped at
// LOG_LEVEL_MAX_TTL: a debugging switch that nobody remembers to turn off
// turns itself off.
func (s *Server) putDebugSubject(w http.ResponseWriter, r *http.Request) {
	subject := r.PathValue("subject")

	ttl, err := time.ParseDuration(r.URL.Query().Get("ttl"))
	if err != nil || ttl <= 0 {
		writeProblem(w, r, NewProblem(http.StatusBadRequest, "ttl is required: a positive duration such as 15m"))

		return
	}

	ttl = min(ttl, s.cfg.LogLevelMaxTTL)

	if !s.subjects.add(subject, time.Now().Add(ttl)) {
		writeProblem(w, r, NewProblem(http.StatusConflict, "too many debug subjects; remove one first"))

		return
	}

	s.log.WarnContext(r.Context(), "admin: debug subject added", "subject", subject, "ttl", ttl.String(), "client", r.RemoteAddr)
	WriteJSON(w, http.StatusOK, s.debugSubjectsResponse())
}

func (s *Server) deleteDebugSubject(w http.ResponseWriter, r *http.Request) {
	s.subjects.remove(r.PathValue("subject"))
	s.log.WarnContext(r.Context(), "admin: debug subject removed", "subject", r.PathValue("subject"), "client", r.RemoteAddr)
	WriteJSON(w, http.StatusOK, s.debugSubjectsResponse())
}
