package httpx

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"net/http/pprof" //nolint:gosec // registered on the admin mux only, never on the API listener
	"strings"
	"time"
)

// newAdminMux builds the operational surface served on Config.AdminAddr:
//
//	GET    /healthz           liveness
//	GET    /readyz            readiness (gate + registered checks)
//	GET    /metrics           Prometheus exposition
//	GET    /version           build identity + start time / uptime (see [Build])
//	GET    /admin/config      effective configuration, secrets redacted (see [Redact], [WithConfig])
//	GET    /admin/log-level   current log level, base level, expiry of a runtime change
//	PUT    /admin/log-level   change the level for a while: level=debug|info|warn|error, ttl=30m (capped)
//	DELETE /admin/log-level   revert to the base level now
//	GET    /debug/pprof/…     Go runtime profiles (cpu, heap, goroutine, block, mutex, trace)
//
// Everything is read-only except the two log-level mutations, which require
// `Authorization: Bearer <Config.AdminToken>` when a token is configured; an
// empty token leaves them open (the local / compose default) and Run says so
// in its "admin server listening" line.
//
// It is a plain [http.ServeMux] rather than gin: none of the API middleware
// (access log, request metrics, tracing) applies here, so probes and scrapes
// never show up as traffic and a runtime agent can poll pprof for free.
// Keep this listener off the ingress; it is the one that exposes internals.
func newAdminMux(s *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.Health.LiveHandler())
	mux.HandleFunc("GET /readyz", s.Health.ReadyHandler())
	mux.Handle("GET /metrics", s.Metrics.Handler())
	mux.HandleFunc("GET /version", s.versionHandler)
	mux.HandleFunc("GET /admin/config", s.configHandler)
	mux.HandleFunc("GET /admin/log-level", s.getLogLevel)
	guard := requireBearer(s.cfg.AdminToken, s.log)
	mux.Handle("PUT /admin/log-level", guard(http.HandlerFunc(s.putLogLevel)))
	mux.Handle("DELETE /admin/log-level", guard(http.HandlerFunc(s.deleteLogLevel)))
	// Explicit registration so nothing depends on http.DefaultServeMux.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// versionResponse is what GET /version serves: the build identity plus when
// this process started. The API-port /version some services expose (ping)
// serves the bare [BuildInfo] its contract describes.
type versionResponse struct {
	BuildInfo
	StartedAt     time.Time `json:"started_at"`
	UptimeSeconds int64     `json:"uptime_seconds"`
}

func (s *Server) versionHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, versionResponse{
		BuildInfo:     s.Build,
		StartedAt:     s.startedAt,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
	})
}

func (s *Server) configHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, Redact(s.configView))
}

// logLevelResponse is the body of every /admin/log-level response.
type logLevelResponse struct {
	Level     string     `json:"level"`              // in effect now
	Base      string     `json:"base"`               // configured; what runtime changes revert to
	Previous  string     `json:"previous,omitempty"` // on a mutation: the level before it
	ExpiresAt *time.Time `json:"expires_at"`         // when the runtime change reverts; null when none
	MaxTTL    string     `json:"max_ttl"`            // cap applied to PUT's ttl
}

func (s *Server) logLevelResponse(previous string) logLevelResponse {
	level, expires := s.LogLevel.Level()
	resp := logLevelResponse{
		Level:    levelString(level),
		Base:     levelString(s.LogLevel.Base()),
		Previous: previous,
		MaxTTL:   s.cfg.LogLevelMaxTTL.String(),
	}
	if !expires.IsZero() {
		resp.ExpiresAt = &expires
	}
	return resp
}

func (s *Server) getLogLevel(w http.ResponseWriter, _ *http.Request) {
	if s.LogLevel == nil {
		writeProblem(w, NewProblem(http.StatusNotImplemented, "the logger was not built by httpx.NewLogger; its level cannot be changed at runtime"))
		return
	}
	writeJSON(w, http.StatusOK, s.logLevelResponse(""))
}

// putLogLevel reads level= and ttl= from the query string or a form body
// (`curl -X PUT 'host:9080/admin/log-level?level=debug&ttl=30m'`). ttl is
// optional and capped at Config.LogLevelMaxTTL: a runtime change always
// expires, so nobody has to remember to turn debug off.
func (s *Server) putLogLevel(w http.ResponseWriter, r *http.Request) {
	if s.LogLevel == nil {
		writeProblem(w, NewProblem(http.StatusNotImplemented, "the logger was not built by httpx.NewLogger; its level cannot be changed at runtime"))
		return
	}
	level, err := parseLevelStrict(r.FormValue("level"))
	if err != nil {
		writeProblem(w, NewProblem(http.StatusBadRequest, err.Error()))
		return
	}
	ttl := s.cfg.LogLevelMaxTTL
	if raw := r.FormValue("ttl"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			writeProblem(w, NewProblem(http.StatusBadRequest, "ttl must be a positive duration such as 30m or 2h"))
			return
		}
		ttl = min(d, s.cfg.LogLevelMaxTTL)
	}

	previous := s.LogLevel.Set(level, ttl)
	resp := s.logLevelResponse(levelString(previous))
	// Marked context: this record must land whatever the level was or is.
	// ("from"/"to", not "level": that key is the record's own severity.)
	s.log.LogAttrs(WithDebugLogging(r.Context()), slog.LevelWarn, "log level changed",
		slog.String("from", resp.Previous), slog.String("to", resp.Level),
		slog.String("ttl", ttl.String()), slog.String("client", r.RemoteAddr))
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) deleteLogLevel(w http.ResponseWriter, r *http.Request) {
	if s.LogLevel == nil {
		writeProblem(w, NewProblem(http.StatusNotImplemented, "the logger was not built by httpx.NewLogger; its level cannot be changed at runtime"))
		return
	}
	previous := s.LogLevel.Reset()
	resp := s.logLevelResponse(levelString(previous))
	s.log.LogAttrs(WithDebugLogging(r.Context()), slog.LevelWarn, "log level reset",
		slog.String("from", resp.Previous), slog.String("to", resp.Level),
		slog.String("client", r.RemoteAddr))
	writeJSON(w, http.StatusOK, resp)
}

// requireBearer guards a mutating admin handler with `Authorization: Bearer
// <token>` (constant-time compare). An empty token disables the guard.
// Rejections are logged at warn: on the admin listener they are either an
// operator with a stale token or something that should not be there at all.
func requireBearer(token string, log *slog.Logger) func(http.Handler) http.Handler {
	if token == "" {
		return func(next http.Handler) http.Handler { return next }
	}
	want := []byte(token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
				log.Warn("admin: rejected unauthenticated mutation",
					"method", r.Method, "path", r.URL.Path, "client", r.RemoteAddr)
				w.Header().Set("WWW-Authenticate", `Bearer realm="admin"`)
				writeProblem(w, NewProblem(http.StatusUnauthorized, "a valid Authorization: Bearer <ADMIN_TOKEN> header is required"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
