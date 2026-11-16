package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Server wires an [http.ServeMux] for the service's API together with the
// cross-cutting concerns every service shares — request ids, tracing,
// structured, sampled request logging with trace correlation, Prometheus
// metrics, panic recovery, body limits, per-request deadlines, per-request
// debug logging — and a second, admin listener for the operational surface
// (probes, metrics, build identity, runtime switches, pprof). Services call
// [Server.Mux] to register their own routes, then [Server.Run] to serve both
// listeners with graceful shutdown.
type Server struct {
	cfg        Config
	log        *slog.Logger
	mux        *http.ServeMux
	handler    http.Handler // the API chain around mux
	admin      http.Handler
	proxies    []netip.Prefix
	apiAddr    atomic.Pointer[string] // bound API address, set by Run
	adminAddr  atomic.Pointer[string] // bound admin address, set by Run
	startedAt  time.Time
	configView any
	Build      BuildInfo
	Metrics    *Metrics
	Health     *Health
	diag       *diagnostics
	// LogLevel is the runtime level control of the logger passed to
	// [NewServer], nil when that logger did not come from [NewLogger].
	LogLevel *LogLevel

	subjects debugSubjects // tenants / users under investigation (subjects.go)

	inFlight inFlight // API requests a handler has not returned from (see shutdown)

	ran atomic.Bool // Run was called: the gate, the listeners and the timers are those of one run

	// listen is net.Listen, a field so that a test can hand Run a listener
	// that fails while serving. It takes no context: a bind is immediate,
	// and a Run whose ctx is already cancelled must still bind and then shut
	// down cleanly.
	listen func(network, address string) (net.Listener, error)
}

// inFlight counts the requests of the API listener whose handler is still
// running, so that a forced close can wait for them: http.Server.Close
// closes connections and returns, it does not wait for handlers.
type inFlight struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed when n reaches zero; nil until someone waits
}

func (f *inFlight) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.n++
		f.mu.Unlock()

		defer func() {
			f.mu.Lock()
			defer f.mu.Unlock()

			if f.n--; f.n == 0 && f.idle != nil {
				close(f.idle)
				f.idle = nil
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// wait reports whether every tracked handler returned within budget.
func (f *inFlight) wait(budget time.Duration) bool {
	f.mu.Lock()

	if f.n == 0 {
		f.mu.Unlock()

		return true
	}

	if f.idle == nil {
		f.idle = make(chan struct{})
	}

	idle := f.idle
	f.mu.Unlock()

	timer := time.NewTimer(budget)
	defer timer.Stop()

	select {
	case <-idle:
		return true
	case <-timer.C:
		return false
	}
}

// Option customises [NewServer].
type Option func(*serverOptions)

type serverOptions struct {
	outer  []Middleware
	config any
	tp     trace.TracerProvider
	prop   propagation.TextMapPropagator
}

// WithTracerProvider sets the provider the server spans come from. Without it
// the global provider (otel.GetTracerProvider, installed by otelx.Init) is
// used — the OpenTelemetry convention for instrumentation.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *serverOptions) { o.tp = tp }
}

// WithPropagator sets the propagator that reads the caller's trace context
// from the request headers. Without it the global one is used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *serverOptions) { o.prop = p }
}

// WithMiddleware installs middleware — an authenticator, a tenant resolver, a
// rate limiter — *inside* the built-in tracing, access log, metrics and body
// limit (what it reads of the body is capped at Config.MaxBodyBytes, like
// what the handler reads) and outside the handler's panic recovery. A request it
// rejects (401, 429) is therefore still traced, logged and counted like any
// other, and what it adds to the context is visible to the handler; the
// access-log line is written after it returns. A handler that panics has
// become a 500 response by the time control is back in the middleware; a
// panic of the middleware itself is recovered one level further out, with
// the same 500, log line and http_unhandled_errors_total tick. First
// middleware outermost.
func WithMiddleware(mw ...Middleware) Option {
	return func(o *serverOptions) { o.outer = append(o.outer, mw...) }
}

// WithConfig sets what GET /admin/config shows — normally the service's own
// config struct, which usually embeds [Config]. It is passed through
// [Redact] on every request, so secrets never leave the process as long as
// they are tagged `secret:"true"` or named like secrets. Without this option
// the endpoint shows the [Config] the server was built with.
func WithConfig(v any) Option {
	return func(o *serverOptions) { o.config = v }
}

// NewServer constructs a server from cfg and log; it validates cfg (see
// [Config.Validate]) and returns the first problem instead of a server.
//
// The API chain applies, in order: flight-recorder trigger (when
// Config.DiagFlightRecorder and DiagFlightSlowRequest are set) → request id +
// deadline → debug token (when Config.DebugToken is set) → tracing → access
// log → metrics → panic recovery → body limit → rate limit (when
// Config.RateLimit or RateLimitPerClient is set) → [WithMiddleware] extras →
// panic recovery → routing (404/405 as problem+json) → the service's mux. The admin handler serves the
// probes, /metrics, /version, /admin/* and /debug/pprof; its table of routes
// is on newAdminMux (admin.go).
func NewServer(cfg Config, log *slog.Logger, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("httpx: config: %w", err)
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	var applied serverOptions
	for _, opt := range opts {
		opt(&applied)
	}

	if applied.tp == nil {
		applied.tp = otel.GetTracerProvider() // a delegating proxy: picks up a later otelx.Init too
	}

	if applied.prop == nil {
		applied.prop = otel.GetTextMapPropagator()
	}

	if applied.config == nil {
		applied.config = cfg // the effective one, defaults applied
	}

	proxies, _ := parsePrefixes(cfg.TrustedProxies) // validated above
	build := Build(cfg.Service)

	metrics := NewMetrics(build, log)
	h := NewHealth(cfg.HealthInterval, cfg.HealthTimeout)
	metrics.Registry.MustRegister(h.Collectors()...)

	s := &Server{
		cfg: cfg, log: log, mux: http.NewServeMux(),
		proxies:    proxies,
		startedAt:  time.Now(),
		configView: applied.config,
		Build:      build,
		Metrics:    metrics, Health: h,
		LogLevel: LogLevelOf(log),
		listen:   net.Listen,
	}

	s.diag = newDiagnostics(cfg, build, log)
	metrics.Registry.MustRegister(s.diag.dumps)

	mws := []Middleware{}
	if cfg.DiagFlightRecorder && cfg.DiagFlightSlowRequest > 0 {
		mws = append(mws, s.flightTrigger)
	}

	mws = append(mws, s.entry)
	if !cfg.DebugToken.IsZero() {
		mws = append(mws, debugToken(cfg.DebugToken.Expose()))
	}

	mws = append(mws,
		tracing(applied.tp, applied.prop, cfg.DebugTraceTrustRemote),
		accessLog(log, cfg.SlowRequest),
		metrics.Middleware(),
		// For what the service installs below: without it a panic there
		// reaches net/http, the client sees a dropped connection, and the
		// log and the metrics above record a request that wrote nothing.
		s.recovery,
		// Before anything that may read the body: the service's middleware
		// (a signature check buffers it) as well as its handlers.
		bodyLimit(cfg.MaxBodyBytes),
	)

	// Inside tracing, the access log and the metrics — a 429 is a request
	// like any other to them — and outside everything the service installs:
	// what is shed costs no authentication, no handler.
	if limiter := newRateLimiter(cfg, metrics.Registry); limiter != nil {
		mws = append(mws, limiter.middleware)
	}

	mws = append(mws, applied.outer...)
	mws = append(mws, s.recovery)
	s.handler = chain(route(s.mux), mws...)
	s.admin = newAdminMux(s)

	return s, nil
}

// Mux exposes the API mux so services can add routes with the Go 1.22+
// method-and-pattern syntax:
//
//	srv.Mux().HandleFunc("GET /tasks/{id}", h.get)
func (s *Server) Mux() *http.ServeMux { return s.mux }

// Handler returns the complete API handler (middleware chain + mux), mainly
// so tests can drive it with httptest without a listener.
func (s *Server) Handler() http.Handler { return s.handler }

// Admin exposes the admin handler (the routes of newAdminMux), mainly so
// tests can drive it without a listener.
func (s *Server) Admin() http.Handler { return s.admin }

// Logger returns the server's structured logger.
func (s *Server) Logger() *slog.Logger { return s.log }

// ListenAddr returns the address the API listener is bound to once [Server.Run]
// has bound it (a Config.Addr of ":0" picks a free port), "" before that or
// when there is no API listener. Tests use it instead of fixed ports.
func (s *Server) ListenAddr() string { return derefString(s.apiAddr.Load()) }

// AdminListenAddr is [Server.ListenAddr] for the admin listener.
func (s *Server) AdminListenAddr() string { return derefString(s.adminAddr.Load()) }

func derefString(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

// Run binds the listeners, evaluates the readiness checks once, opens the
// readiness gate and serves until ctx is cancelled (typically by a
// SIGINT/SIGTERM context from [SignalContext]) or a listener fails. An empty
// Config.Addr skips the API listener (a pure worker); an empty AdminAddr
// skips the admin one.
//
// Shutdown, on cancellation:
//
//  1. readiness flips to 503 so load balancers stop sending new connections;
//  2. Config.ShutdownDelay passes — the time it takes for that to propagate
//     (endpoint controllers, ingress reloads) — while requests are still
//     served;
//  3. the API listener drains within Config.ShutdownTimeout; if requests are
//     still running when the budget is spent, their contexts are cancelled
//     and the connections closed, and Run waits up to unwindBudget for their
//     handlers to return, so that main's deferred cleanup does not close a
//     pool under a handler that is rolling back. A handler that ignores its
//     context for longer than that outlives Run;
//  4. the admin listener stops last, so probes answered throughout; its
//     requests are not cancelled by the forced close of step 3.
//
// A listener that fails ends Run with its error at once, during step 2 as
// well: there is nothing left to wait for.
//
// A clean shutdown returns nil. Binding happens before anything else, so a
// port in use is returned from Run directly rather than reported from a
// goroutine. A Server runs once: a second Run, after the first or next to
// it, is an error — build a new Server.
func (s *Server) Run(ctx context.Context) error {
	if !s.ran.CompareAndSwap(false, true) {
		return errors.New("httpx: Run was called more than once on one Server")
	}

	// Request contexts are deliberately NOT derived from ctx: cancelling Run
	// must drain in-flight requests within ShutdownTimeout, not abort them.
	// cancelBase (step 3 above) is what ends them.
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	// The admin listener has a context of its own: the forced close of the
	// API must not cut a profile or a scrape that is being served.
	adminCtx, cancelAdmin := context.WithCancel(context.Background())
	defer cancelAdmin()

	var (
		api, admin     *http.Server
		apiLn, adminLn net.Listener
	)

	if s.cfg.Addr != "" {
		api = &http.Server{
			Handler:           s.inFlight.track(s.handler),
			ReadHeaderTimeout: s.cfg.ReadHeaderTimeout,
			ReadTimeout:       s.cfg.ReadTimeout,
			WriteTimeout:      s.cfg.WriteTimeout,
			IdleTimeout:       s.cfg.IdleTimeout,
			MaxHeaderBytes:    s.cfg.MaxHeaderBytes,
			BaseContext:       func(net.Listener) context.Context { return baseCtx },
		}

		ln, err := s.listen("tcp", s.cfg.Addr)
		if err != nil {
			return fmt.Errorf("httpx: bind API listener %s: %w", s.cfg.Addr, err)
		}

		apiLn = ln
		addr := ln.Addr().String()
		s.apiAddr.Store(&addr)
	}

	if s.cfg.AdminAddr != "" {
		// No WriteTimeout: /debug/pprof/profile streams for its ?seconds= budget.
		admin = &http.Server{
			Handler:           s.admin,
			ReadHeaderTimeout: s.cfg.ReadHeaderTimeout,
			IdleTimeout:       s.cfg.IdleTimeout,
			MaxHeaderBytes:    s.cfg.MaxHeaderBytes,
			BaseContext:       func(net.Listener) context.Context { return adminCtx },
		}

		ln, err := s.listen("tcp", s.cfg.AdminAddr)
		if err != nil {
			if apiLn != nil {
				_ = apiLn.Close()
			}

			return fmt.Errorf("httpx: bind admin listener %s: %w", s.cfg.AdminAddr, err)
		}

		adminLn = ln
		addr := ln.Addr().String()
		s.adminAddr.Store(&addr)
	}

	if api == nil && admin == nil {
		return errors.New("httpx: neither HTTP_ADDR nor ADMIN_ADDR is set")
	}

	serveErr := make(chan error, 2)
	serve := func(name string, srv *http.Server, ln net.Listener, attrs ...any) {
		if srv == nil {
			return
		}

		go func() {
			s.log.InfoContext(ctx, name+" listening", append([]any{"addr", ln.Addr().String()}, attrs...)...)

			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("%s: %w", name, err)
				return
			}

			serveErr <- nil
		}()
	}
	// Say out loud whether the admin surface is guarded: "off" is fine on a
	// laptop and a finding in a cluster (Validate only lets it through on
	// loopback or with ADMIN_INSECURE).
	adminAuth := "bearer"
	if s.cfg.AdminToken.IsZero() {
		adminAuth = "off"
	}

	serve("admin server", admin, adminLn, "guard", adminAuth) // not "auth": the logger masks keys that name credentials
	serve("http server", api, apiLn, "debug_token", !s.cfg.DebugToken.IsZero())

	// Readiness: one synchronous pass so the first probe answers from real
	// results, then the background loop for the lifetime of the server.
	s.Health.refresh(ctx)

	healthCtx, stopHealth := context.WithCancel(ctx)
	defer stopHealth()

	go s.Health.loop(healthCtx)

	s.tuneRuntime(healthCtx)

	stopDiagnostics := s.diag.start(healthCtx)
	defer stopDiagnostics()

	s.Health.SetReady(true)

	select {
	case err := <-serveErr:
		s.Health.SetReady(false)
		_ = s.shutdown(api, admin, cancelBase) // own timeout context; the listener failure is the error worth returning

		return err
	case <-ctx.Done():
		s.Health.SetReady(false)
		s.log.InfoContext(ctx, "shutdown requested, draining",
			"delay", s.cfg.ShutdownDelay.String(), "timeout", s.cfg.ShutdownTimeout.String())

		if s.cfg.ShutdownDelay > 0 {
			delay := time.NewTimer(s.cfg.ShutdownDelay)
			defer delay.Stop()

			select {
			case <-delay.C:
			case err := <-serveErr:
				_ = s.shutdown(api, admin, cancelBase)

				return err
			}
		}

		if err := s.shutdown(api, admin, cancelBase); err != nil { // ctx is done: shutdown runs on its own ShutdownTimeout context
			return err
		}

		s.log.InfoContext(ctx, "servers stopped cleanly")

		return nil
	}
}

// unwindBudget is how long a forced close waits for the handlers it has
// cancelled: time to notice the cancellation and roll back, not to finish
// the work — the drain budget was for that, and what is left of the pod's
// termination grace period is unknown here.
const unwindBudget = 2 * time.Second

// adminShutdownBudget bounds the admin listener's own drain: probes and
// scrapes are short, and it must not eat into the API's budget.
const adminShutdownBudget = 2 * time.Second

// shutdown drains the API listener within the budget, forces it closed past
// it and waits for the cancelled handlers (see unwindBudget), then stops the
// admin listener.
func (s *Server) shutdown(api, admin *http.Server, cancelBase context.CancelFunc) error {
	var first error

	if api != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		err := api.Shutdown(shutCtx)

		cancel()

		if err != nil {
			s.log.Warn("drain budget exhausted, closing remaining connections", "err", err)
			cancelBase() // every in-flight request context is cancelled

			_ = api.Close() // and its connection closed
			first = fmt.Errorf("httpx: drain: %w", err)

			if !s.inFlight.wait(unwindBudget) {
				s.log.Warn("handlers still running after the forced close: they ignore their context", "waited", unwindBudget.String())
			}
		}
	}

	if admin != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), adminShutdownBudget)
		err := admin.Shutdown(shutCtx)

		cancel()

		if err != nil {
			// Not a failure of the service: nothing a caller waits for is
			// served here. What outlives the budget is a scrape cut short
			// or — the common case — a connection a client opened and
			// never used, which net/http counts as active for five
			// seconds. Close it and say so; the exit code stays clean.
			_ = admin.Close()

			s.log.Warn("admin listener: connections still open after its drain budget, closed", "budget", adminShutdownBudget.String(), "err", err)
		}
	}

	cancelBase()

	return first
}
