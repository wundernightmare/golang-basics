package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Server wires a [gin.Engine] together with the cross-cutting concerns every
// service shares: structured request logging, panic recovery, Prometheus
// metrics, and health endpoints. Services call [Server.Engine] to register
// their own routes, then [Server.Run] to serve with graceful shutdown.
type Server struct {
	cfg     Config
	log     *slog.Logger
	engine  *gin.Engine
	Metrics *Metrics
	Health  *Health
}

// NewServer constructs a server from cfg and log. The returned engine already
// serves:
//
//	GET /healthz   liveness
//	GET /readyz    readiness (gate + registered checks)
//	GET /metrics   Prometheus exposition
//
// and applies, in order: request logging → panic recovery → metrics.
func NewServer(cfg Config, log *slog.Logger) *Server {
	gin.SetMode(gin.ReleaseMode)

	m := NewMetrics()
	h := NewHealth()

	e := gin.New()
	e.Use(requestLogger(log), gin.Recovery(), m.Middleware())

	e.GET("/healthz", h.LiveHandler())
	e.GET("/readyz", h.ReadyHandler())
	e.GET("/metrics", gin.WrapH(m.Handler()))

	return &Server{cfg: cfg, log: log, engine: e, Metrics: m, Health: h}
}

// Engine exposes the underlying gin engine so services can add routes.
func (s *Server) Engine() *gin.Engine { return s.engine }

// Logger returns the server's structured logger.
func (s *Server) Logger() *slog.Logger { return s.log }

// Run starts the HTTP listener and blocks until ctx is cancelled (typically
// by a SIGINT/SIGTERM context from [SignalContext]) or the listener fails. On
// cancellation it marks the service not-ready and drains in-flight requests
// within Config.ShutdownTimeout. A clean shutdown returns nil.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           s.engine,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", "addr", s.cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	s.Health.SetReady(true)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		s.Health.SetReady(false)
		s.log.Info("shutdown requested, draining", "timeout", s.cfg.ShutdownTimeout)

		shutCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			return err
		}
		s.log.Info("http server stopped cleanly")
		return nil
	}
}

// requestLogger logs one structured line per request at info level (4xx at
// warn, 5xx at error), including method, route, status and latency.
func requestLogger(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		attrs := []any{
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", status,
			"latency_ms", float64(time.Since(start).Microseconds()) / 1000.0,
			"client_ip", c.ClientIP(),
		}

		switch {
		case status >= http.StatusInternalServerError:
			log.Error("request", attrs...)
		case status >= http.StatusBadRequest:
			log.Warn("request", attrs...)
		default:
			log.Info("request", attrs...)
		}
	}
}
