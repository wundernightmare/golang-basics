package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Server wires a [gin.Engine] for the service's API together with the
// cross-cutting concerns every service shares — structured, sampled request
// logging with trace correlation, panic recovery, Prometheus metrics — and a
// second, admin listener for the operational surface (health, metrics,
// version, pprof). Services call [Server.Engine] to register their own routes,
// then [Server.Run] to serve both listeners with graceful shutdown.
type Server struct {
	cfg     Config
	log     *slog.Logger
	engine  *gin.Engine
	admin   http.Handler
	Build   BuildInfo
	Metrics *Metrics
	Health  *Health
}

// Option customises [NewServer].
type Option func(*serverOptions)

type serverOptions struct {
	outer []gin.HandlerFunc
}

// WithMiddleware installs middleware *outside* the built-in access log and
// metrics — this is where tracing goes (otelx.GinMiddleware). The tracing
// middleware puts the span on the request context only for the duration of
// its own Next(), so anything that must see the span after the handler ran
// (the access-log line, which needs the status) has to be inside it, not
// before it. Registering tracing with Engine().Use() after NewServer would
// put it inside the logger and the access log would lose its trace_id.
func WithMiddleware(mw ...gin.HandlerFunc) Option {
	return func(o *serverOptions) { o.outer = append(o.outer, mw...) }
}

// NewServer constructs a server from cfg and log. The API engine applies, in
// order: [WithMiddleware] extras (tracing) → request logging → panic recovery
// → metrics. The admin handler serves /healthz, /readyz, /metrics, /version
// and /debug/pprof (see newAdminMux).
func NewServer(cfg Config, log *slog.Logger, opts ...Option) *Server {
	gin.SetMode(gin.ReleaseMode)
	var o serverOptions
	for _, opt := range opts {
		opt(&o)
	}

	if cfg.Service == "" {
		cfg.Service = defaultServiceName()
	}
	build := Build(cfg.Service)

	m := NewMetrics(build, log)
	h := NewHealth(cfg.HealthInterval, cfg.HealthTimeout)
	m.Registry.MustRegister(h.Collectors()...)

	e := gin.New()
	e.Use(o.outer...)
	e.Use(requestLogger(log, cfg.SlowRequest), gin.Recovery(), m.Middleware())

	return &Server{
		cfg: cfg, log: log, engine: e,
		admin:   newAdminMux(h, m, build),
		Build:   build,
		Metrics: m, Health: h,
	}
}

// Engine exposes the underlying gin engine so services can add routes.
func (s *Server) Engine() *gin.Engine { return s.engine }

// Admin exposes the admin handler (health, metrics, version, pprof), mainly so
// tests can drive it without a listener.
func (s *Server) Admin() http.Handler { return s.admin }

// Logger returns the server's structured logger.
func (s *Server) Logger() *slog.Logger { return s.log }

// Run starts the listeners and blocks until ctx is cancelled (typically by a
// SIGINT/SIGTERM context from [SignalContext]) or one of them fails. An empty
// Config.Addr skips the API listener (a pure worker); an empty AdminAddr skips
// the admin one. On cancellation it marks the service not-ready, drains the
// API listener within Config.ShutdownTimeout, then stops the admin listener —
// so probes keep answering (503) while in-flight requests finish. A clean
// shutdown returns nil.
func (s *Server) Run(ctx context.Context) error {
	var api, admin *http.Server
	if s.cfg.Addr != "" {
		api = &http.Server{Addr: s.cfg.Addr, Handler: s.engine, ReadHeaderTimeout: 5 * time.Second}
	}
	if s.cfg.AdminAddr != "" {
		// No WriteTimeout: /debug/pprof/profile streams for its ?seconds= budget.
		admin = &http.Server{Addr: s.cfg.AdminAddr, Handler: s.admin, ReadHeaderTimeout: 5 * time.Second}
	}
	if api == nil && admin == nil {
		return errors.New("httpx: neither HTTP_ADDR nor ADMIN_ADDR is set")
	}

	serveErr := make(chan error, 2)
	serve := func(name string, srv *http.Server) {
		if srv == nil {
			return
		}
		go func() {
			s.log.Info(name+" listening", "addr", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- err
				return
			}
			serveErr <- nil
		}()
	}
	serve("admin server", admin)
	serve("http server", api)

	// Background readiness evaluation for the lifetime of the server; the
	// first pass runs before the gate opens so /readyz is accurate at once.
	healthCtx, stopHealth := context.WithCancel(ctx)
	defer stopHealth()
	go s.Health.Run(healthCtx)

	s.Health.SetReady(true)

	select {
	case err := <-serveErr:
		_ = s.shutdown(api, admin) // the listener failure is the error worth returning
		return err
	case <-ctx.Done():
		s.Health.SetReady(false)
		s.log.Info("shutdown requested, draining", "timeout", s.cfg.ShutdownTimeout.String())
		if err := s.shutdown(api, admin); err != nil {
			return err
		}
		s.log.Info("servers stopped cleanly")
		return nil
	}
}

func (s *Server) shutdown(api, admin *http.Server) error {
	shutCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	var first error
	for _, srv := range []*http.Server{api, admin} { // API first: probes stay up while draining
		if srv == nil {
			continue
		}
		if err := srv.Shutdown(shutCtx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// requestLogger logs one structured line per API request: info for 2xx/3xx,
// warn for 4xx and for anything slower than slow (when > 0), error for 5xx.
// The record is emitted with the request context, so trace_id / span_id are
// attached when tracing middleware is installed, and it goes through the
// sampling handler — under load only a sample of successful requests is
// written while every 4xx/5xx/slow line survives. Exact counts live in the
// http_requests_total metric; the log is for the shape and the outliers.
func requestLogger(log *slog.Logger, slow time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		latency := time.Since(start)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		attrs := []slog.Attr{
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("route", route),
			slog.Int("status", status),
			slog.Float64("latency_ms", float64(latency.Microseconds())/1000.0),
			slog.Int("bytes", max(c.Writer.Size(), 0)), // -1 when nothing was written
			slog.String("client_ip", c.ClientIP()),
		}

		level, msg := slog.LevelInfo, "request"
		switch {
		case status >= http.StatusInternalServerError:
			level = slog.LevelError
		case status >= http.StatusBadRequest:
			level = slog.LevelWarn
		case slow > 0 && latency > slow:
			level, msg = slog.LevelWarn, "slow request"
		}
		log.LogAttrs(c.Request.Context(), level, msg, attrs...)
	}
}
