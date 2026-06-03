package httpx

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

// CheckFunc is a single readiness probe. It returns nil when the dependency
// it guards is healthy, or an error describing why it is not.
type CheckFunc func(ctx context.Context) error

// Health tracks liveness and readiness. Liveness ("am I running?") is a flat
// 200 once the process is up. Readiness ("can I serve traffic?") runs every
// registered [CheckFunc] and also honours an explicit ready flag the server
// flips during startup and graceful shutdown.
type Health struct {
	mu     sync.RWMutex
	checks map[string]CheckFunc
	ready  atomic.Bool
}

// NewHealth returns an empty registry. Until SetReady(true) is called the
// readiness endpoint reports 503, so a service is never advertised as ready
// before it has finished binding its listener.
func NewHealth() *Health {
	return &Health{checks: make(map[string]CheckFunc)}
}

// Register adds (or replaces) a named readiness check.
func (h *Health) Register(name string, fn CheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = fn
}

// SetReady flips the readiness gate. The server sets it true once listening
// and false at the start of shutdown so load balancers drain it cleanly.
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// LiveHandler reports process liveness — always 200 while the handler runs.
func (h *Health) LiveHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}

// ReadyHandler reports readiness: 200 only when the gate is open and every
// registered check passes, otherwise 503 with a per-check breakdown.
func (h *Health) ReadyHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !h.ready.Load() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}

		h.mu.RLock()
		names := make([]string, 0, len(h.checks))
		for name := range h.checks {
			names = append(names, name)
		}
		h.mu.RUnlock()
		sort.Strings(names) // deterministic output for tests + humans

		results := make(map[string]string, len(names))
		ok := true
		for _, name := range names {
			h.mu.RLock()
			fn := h.checks[name]
			h.mu.RUnlock()
			if err := fn(c.Request.Context()); err != nil {
				results[name] = err.Error()
				ok = false
			} else {
				results[name] = "ok"
			}
		}

		status := http.StatusOK
		body := "ready"
		if !ok {
			status = http.StatusServiceUnavailable
			body = "degraded"
		}
		c.JSON(status, gin.H{"status": body, "checks": results})
	}
}
