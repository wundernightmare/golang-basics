package httpx

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"runtime/debug"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// Middleware wraps an [http.Handler]. The server composes its built-in chain
// from these and accepts extra ones through [WithMiddleware].
type Middleware func(http.Handler) http.Handler

// chain applies mws to h, first middleware outermost.
func chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// reqInfo is the per-request state the middleware chain shares through the
// request context: the id, the response as observed by the outermost
// wrapper, the matched route once the mux has run. One allocation per
// request; every middleware reads it instead of wrapping the writer again.
type reqInfo struct {
	id       string
	clientIP string
	route    string // matched route template ("/tasks/{id}"), "" until routed or when unmatched
	rw       *responseWriter
	log      *slog.Logger
	aborted  bool // handler panicked with http.ErrAbortHandler: the connection was dropped
}

type reqInfoKey struct{}

func infoFrom(ctx context.Context) *reqInfo {
	i, _ := ctx.Value(reqInfoKey{}).(*reqInfo)
	return i
}

// status returns the response status as the client saw it: what was written,
// 200 when the handler returned without writing (net/http's behaviour), 499
// when the connection was aborted.
func (i *reqInfo) status() int {
	switch {
	case i == nil:
		return 0
	case i.aborted:
		return statusClientClosedRequest
	case i.rw.status == 0:
		return http.StatusOK
	default:
		return i.rw.status
	}
}

// statusClientClosedRequest is nginx's convention for "the client went away
// before we answered" — an access-log status, never one sent on the wire.
const statusClientClosedRequest = 499

// routeLabel is the route the metrics and logs are keyed on.
func (i *reqInfo) routeLabel() string {
	if i == nil || i.route == "" {
		return "unmatched"
	}
	return i.route
}

// RouteFromContext returns the route template ("/tasks/{id}") the request
// matched, or "" before routing / when no route matched. Available to
// middleware that runs after the handler and to the handler itself.
func RouteFromContext(ctx context.Context) string {
	if i := infoFrom(ctx); i != nil {
		return i.route
	}
	return ""
}

// ClientIPFromContext returns the client address the server attributed the
// request to: the peer address, or the first untrusted hop of X-Forwarded-For
// when the peer is one of Config.TrustedProxies.
func ClientIPFromContext(ctx context.Context) string {
	if i := infoFrom(ctx); i != nil {
		return i.clientIP
	}
	return ""
}

// responseWriter records the status and byte count written through it. It
// exposes Unwrap so [http.ResponseController] reaches the underlying writer
// for Flush / Hijack / deadlines, and implements Flusher directly for
// handlers that type-assert it.
type responseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *responseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *responseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// entry is the outermost middleware: it settles the request id, wraps the
// writer, attributes the client address, applies the per-request deadline
// and puts all of it on the context before tracing, logging or the handler
// run, so every one of them sees the same facts.
//
// Why a request id when there is a trace id: tracing is opt-in here (and
// sampled where it is on), so trace_id is absent exactly when someone needs
// to tie a client's "it failed" to a log line. The request id is 100% present,
// costs eight random bytes, and travels in the response header and in every
// log record and problem+json body of the request. An inbound X-Request-Id
// is honoured only from a trusted proxy: the public internet does not get to
// choose how its requests are correlated.
func (s *Server) entry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &responseWriter{ResponseWriter: w}
		peer := peerAddr(r.RemoteAddr)
		trusted := s.isTrusted(peer)

		id := ""
		if trusted {
			id = r.Header.Get(RequestIDHeader)
			if !validRequestID(id) {
				id = ""
			}
		}
		if id == "" {
			id = newRequestID()
		}

		info := &reqInfo{id: id, rw: rw, log: s.log, clientIP: s.clientIP(r, peer, trusted)}
		ctx := context.WithValue(r.Context(), reqInfoKey{}, info)
		ctx = WithRequestID(ctx, id)
		if s.cfg.RequestTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.cfg.RequestTimeout)
			defer cancel()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(rw, r.WithContext(ctx))
	})
}

func peerAddr(remote string) netip.Addr {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	a, _ := netip.ParseAddr(host)
	return a.Unmap()
}

func (s *Server) isTrusted(a netip.Addr) bool {
	if !a.IsValid() {
		return false
	}
	for _, p := range s.proxies {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// clientIP walks X-Forwarded-For from the right, skipping trusted proxies,
// and returns the first address that is not one — the client as the nearest
// trusted hop saw it. Without a trusted peer the header is ignored entirely.
func (s *Server) clientIP(r *http.Request, peer netip.Addr, trusted bool) string {
	if !trusted {
		return peer.String()
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			break // a malformed hop: trust nothing to its left
		}
		a = a.Unmap()
		if !s.isTrusted(a) {
			return a.String()
		}
	}
	return peer.String()
}

// debugToken is the middleware behind [Config].DebugToken: a request whose
// X-Debug-Token equals token (constant-time compare) runs with a context
// marked by [WithDebugLogging] and gets X-Debug-Logging: on in the response.
// A missing or wrong token is ignored silently — the API port is public, and
// answering "wrong token" would make it an oracle and a log-flood vector.
func debugToken(token string) Middleware {
	want := tokenDigest(token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get(DebugTokenHeader)
			if got == "" || subtle.ConstantTimeCompare(tokenDigest(got), want) != 1 {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set(DebugLoggingHeader, "on")
			next.ServeHTTP(w, r.WithContext(WithDebugLogging(r.Context())))
		})
	}
}

// tracerName is the instrumentation scope of the server spans.
const tracerName = "github.com/tracehubmmp/golang-basics/libs/httpx"

// tracing starts one server span per request from the global tracer
// provider, continuing the trace context the propagator finds in the headers,
// and names it by the matched route once the handler ran. With no provider
// installed (otelx.Init not called, or tracing disabled) the spans are
// non-recording and cost a few allocations; the propagator is still whatever
// was installed, so context is forwarded even when nothing is exported.
func tracing(next http.Handler) http.Handler {
	tracer := otel.Tracer(tracerName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		method := methodLabel(r.Method)
		attrs := []attribute.KeyValue{
			semconv.HTTPRequestMethodKey.String(method),
			semconv.URLPath(r.URL.Path),
			semconv.URLScheme(scheme(r)),
			semconv.ServerAddress(r.Host),
			semconv.NetworkProtocolVersion(strings.TrimPrefix(r.Proto, "HTTP/")),
		}
		if method == methodOther {
			attrs = append(attrs, semconv.HTTPRequestMethodOriginal(r.Method))
		}
		if q := r.URL.RawQuery; q != "" {
			attrs = append(attrs, semconv.URLQuery(q))
		}
		if ua := r.UserAgent(); ua != "" {
			attrs = append(attrs, semconv.UserAgentOriginal(ua))
		}
		if ip := ClientIPFromContext(ctx); ip != "" {
			attrs = append(attrs, semconv.ClientAddress(ip))
		}
		ctx, span := tracer.Start(ctx, method, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))

		info := infoFrom(ctx)
		if info != nil && info.route != "" {
			span.SetName(method + " " + info.route)
			span.SetAttributes(semconv.HTTPRoute(info.route))
		}
		status := info.status()
		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	})
}

func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// accessLog logs one structured line per API request: info for 2xx/3xx/4xx,
// warn for anything slower than slow (when > 0), error for 5xx. The record
// is emitted with the request context, so trace_id / span_id / request_id
// are attached, and it goes through the sampling handler — under load only a
// sample of ordinary requests is written while every 5xx/slow line survives.
// 4xx are info rather than warn on purpose: warn is never sampled, and a
// scanner walking the API would otherwise write a log line per probe. Exact
// counts live in the http_requests_total metric; the log is for the shape
// and the outliers.
func accessLog(log *slog.Logger, slow time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			defer func() {
				info := infoFrom(r.Context())
				status := info.status()
				latency := time.Since(start)
				attrs := []slog.Attr{
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("route", info.routeLabel()),
					slog.Int("status", status),
					slog.Float64("latency_ms", float64(latency.Microseconds())/1000.0),
					slog.Int64("bytes", info.rw.bytes),
					slog.String("client_ip", info.clientIP),
				}
				level, msg := slog.LevelInfo, "request"
				switch {
				case status >= http.StatusInternalServerError:
					level = slog.LevelError
				case slow > 0 && latency > slow:
					level, msg = slog.LevelWarn, "slow request"
				}
				log.LogAttrs(r.Context(), level, msg, attrs...)
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// recovery turns a panic in a handler into a 500 problem+json (when nothing
// was written yet), a structured error log with the stack, an error status on
// the span and one tick of http_panics_total. It sits inside the log and
// metrics middleware so the 500 is logged and counted like any other; both
// use defer and survive a panic that escapes anyway. http.ErrAbortHandler is
// re-raised as net/http expects: it means "drop the connection", not "bug".
func (s *Server) recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			p := recover()
			if p == nil {
				return
			}
			info := infoFrom(r.Context())
			if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				if info != nil {
					info.aborted = true
				}
				panic(p)
			}
			s.Metrics.panics.Inc()
			err := fmt.Errorf("panic: %v", p)
			s.log.LogAttrs(r.Context(), slog.LevelError, "panic in handler",
				slog.Any("panic", p), slog.String("method", r.Method), slog.String("path", r.URL.Path),
				slog.String("stack", string(debug.Stack())))
			span := trace.SpanFromContext(r.Context())
			span.RecordError(err)
			span.SetStatus(codes.Error, "panic")
			if info != nil && info.rw.wroteHeader {
				return // the response is already on the wire; nothing sensible to add
			}
			writeProblem(w, r, NewProblem(http.StatusInternalServerError, "internal error"))
		}()
		next.ServeHTTP(w, r)
	})
}

// bodyLimit caps the request body at limit bytes: reading past it fails with
// *http.MaxBytesError, which [DecodeJSON] turns into 413, and net/http closes
// the connection so the client cannot keep streaming.
func bodyLimit(limit int64) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil && r.Body != http.NoBody {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// route dispatches to the mux and records the matched pattern. Unknown route
// → 404, known route with the wrong method → 405 with Allow, both as
// problem+json so every error the API port emits has the same shape; the
// mux's own not-found / not-allowed handlers are run against a header probe
// so their status and Allow header are kept and only the text body replaced.
func route(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern == "" {
			probe := &headerProbe{header: http.Header{}}
			mux.ServeHTTP(probe, r)
			status := probe.status
			if status == 0 {
				status = http.StatusNotFound
			}
			detail := "no route for " + r.Method + " " + r.URL.Path
			if allow := probe.header.Get("Allow"); allow != "" {
				w.Header().Set("Allow", allow)
				detail = r.Method + " is not allowed on " + r.URL.Path
			}
			writeProblem(w, r, NewProblem(status, detail))
			return
		}
		// The mux sets r.Pattern before it calls the handler; record it in a
		// defer so a handler that panics is still attributed to its route.
		defer func() {
			if info := infoFrom(r.Context()); info != nil {
				info.route = routeOf(r.Pattern)
			}
		}()
		mux.ServeHTTP(w, r)
	})
}

// headerProbe is a ResponseWriter that keeps the status and headers and
// drops the body.
type headerProbe struct {
	header http.Header
	status int
}

func (p *headerProbe) Header() http.Header         { return p.header }
func (p *headerProbe) WriteHeader(code int)        { p.status = code }
func (p *headerProbe) Write(b []byte) (int, error) { return len(b), nil }

// routeOf strips the method and host from a ServeMux pattern
// ("GET example.com/tasks/{id}" → "/tasks/{id}").
func routeOf(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	if i := strings.IndexByte(pattern, '/'); i > 0 {
		pattern = pattern[i:]
	}
	return pattern
}

// methodOther is the label for a request method outside the standard set,
// per the OpenTelemetry semantic conventions: metrics and spans must not grow
// a series per arbitrary method a scanner sends.
const methodOther = "_OTHER"

func methodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return m
	}
	return methodOther
}
