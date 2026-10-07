package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
	"time"
)

// Server wires an [http.ServeMux] for the service's API together with the
// cross-cutting concerns every service shares — request ids, tracing,
// structured, sampled request logging with trace correlation, Prometheus
// metrics, panic recovery, body limits, per-request deadlines, per-request
// debug logging — and a second, admin listener for the operational surface
// (health, metrics, version, config, runtime log level, pprof). Services call
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
	// LogLevel is the runtime level control of the logger passed to
	// [NewServer], nil when that logger did not come from [NewLogger].
	LogLevel *LogLevel
}

// Option customises [NewServer].
type Option func(*serverOptions)

type serverOptions struct {
	outer  []Middleware
	config any
}

// WithMiddleware installs middleware — an authenticator, a tenant resolver, a
// rate limiter — *inside* the built-in tracing, access log and metrics and
// outside panic recovery and the body limit. A request it rejects (401, 429)
// is therefore still traced, logged and counted like any other, and what it
// adds to the context is visible to the handler; the access-log line is
// written after it returns. First middleware outermost.
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
// The API chain applies, in order: request id + deadline → debug token (when
// Config.DebugToken is set) → tracing → access log → metrics → [WithMiddleware]
// extras → panic recovery → body limit → routing (404/405 as problem+json) →
// the service's mux. The admin handler serves /healthz, /livez, /readyz,
// /metrics, /version, /admin/config, /admin/log-level and /debug/pprof (see
// newAdminMux).
func NewServer(cfg Config, log *slog.Logger, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("httpx: config: %w", err)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	var o serverOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.config == nil {
		o.config = cfg // the effective one, defaults applied
	}
	proxies, _ := parsePrefixes(cfg.TrustedProxies) // validated above
	build := Build(cfg.Service)

	m := NewMetrics(build, log)
	h := NewHealth(cfg.HealthInterval, cfg.HealthTimeout)
	m.Registry.MustRegister(h.Collectors()...)

	s := &Server{
		cfg: cfg, log: log, mux: http.NewServeMux(),
		proxies:    proxies,
		startedAt:  time.Now(),
		configView: o.config,
		Build:      build,
		Metrics:    m, Health: h,
		LogLevel: LogLevelOf(log),
	}

	mws := []Middleware{s.entry}
	if cfg.DebugToken != "" {
		mws = append(mws, debugToken(cfg.DebugToken))
	}
	mws = append(mws,
		tracing,
		accessLog(log, cfg.SlowRequest),
		m.Middleware(),
	)
	mws = append(mws, o.outer...)
	mws = append(mws,
		s.recovery,
		bodyLimit(cfg.MaxBodyBytes),
	)
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

// Admin exposes the admin handler (health, metrics, version, config, log
// level, pprof), mainly so tests can drive it without a listener.
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
//     and the connections closed, so Run returns and main's deferred cleanup
//     never races a handler;
//  4. the admin listener stops last, so probes answered throughout.
//
// A clean shutdown returns nil. Binding happens before anything else, so a
// port in use is returned from Run directly rather than reported from a
// goroutine.
func (s *Server) Run(ctx context.Context) error {
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	var api, admin *http.Server
	var apiLn, adminLn net.Listener
	if s.cfg.Addr != "" {
		api = &http.Server{
			Handler:           s.handler,
			ReadHeaderTimeout: s.cfg.ReadHeaderTimeout,
			ReadTimeout:       s.cfg.ReadTimeout,
			WriteTimeout:      s.cfg.WriteTimeout,
			IdleTimeout:       s.cfg.IdleTimeout,
			MaxHeaderBytes:    s.cfg.MaxHeaderBytes,
			BaseContext:       func(net.Listener) context.Context { return baseCtx },
		}
		ln, err := net.Listen("tcp", s.cfg.Addr)
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
			BaseContext:       func(net.Listener) context.Context { return baseCtx },
		}
		ln, err := net.Listen("tcp", s.cfg.AdminAddr)
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
			s.log.Info(name+" listening", append([]any{"addr", ln.Addr().String()}, attrs...)...)
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
	if s.cfg.AdminToken == "" {
		adminAuth = "off"
	}
	serve("admin server", admin, adminLn, "auth", adminAuth)
	serve("http server", api, apiLn, "debug_token", s.cfg.DebugToken != "")

	// Readiness: one synchronous pass so the first probe answers from real
	// results, then the background loop for the lifetime of the server.
	s.Health.refresh(ctx)
	healthCtx, stopHealth := context.WithCancel(ctx)
	defer stopHealth()
	go s.Health.Run(healthCtx)
	s.Health.SetReady(true)

	select {
	case err := <-serveErr:
		s.Health.SetReady(false)
		_ = s.shutdown(api, admin, cancelBase) // the listener failure is the error worth returning
		return err
	case <-ctx.Done():
		s.Health.SetReady(false)
		s.log.Info("shutdown requested, draining",
			"delay", s.cfg.ShutdownDelay.String(), "timeout", s.cfg.ShutdownTimeout.String())
		if s.cfg.ShutdownDelay > 0 {
			time.Sleep(s.cfg.ShutdownDelay)
		}
		if err := s.shutdown(api, admin, cancelBase); err != nil {
			return err
		}
		s.log.Info("servers stopped cleanly")
		return nil
	}
}

// adminShutdownBudget bounds the admin listener's own drain: probes and
// scrapes are short, and it must not eat into the API's budget.
const adminShutdownBudget = 2 * time.Second

// shutdown drains the API listener within the budget, forces it closed past
// it, then stops the admin listener.
func (s *Server) shutdown(api, admin *http.Server, cancelBase context.CancelFunc) error {
	var first error
	if api != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		err := api.Shutdown(shutCtx)
		cancel()
		if err != nil {
			s.log.Warn("drain budget exhausted, closing remaining connections", "err", err)
			cancelBase()    // every in-flight request context is cancelled
			_ = api.Close() // and its connection closed
			first = fmt.Errorf("httpx: drain: %w", err)
		}
	}
	if admin != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), adminShutdownBudget)
		err := admin.Shutdown(shutCtx)
		cancel()
		if err != nil {
			_ = admin.Close()
			if first == nil {
				first = fmt.Errorf("httpx: admin drain: %w", err)
			}
		}
	}
	cancelBase()
	return first
}
