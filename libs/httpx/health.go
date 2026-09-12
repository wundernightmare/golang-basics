package httpx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// CheckFunc is a single readiness probe. It returns nil when the dependency
// it guards is healthy, or an error describing why it is not.
type CheckFunc func(ctx context.Context) error

// CheckOption configures a registered check.
type CheckOption func(*check)

// Optional marks a check as non-critical: when it fails the service reports
// "degraded" but stays ready (200), because it can still do useful work
// without that dependency — a cache it can bypass, an event bus it publishes
// to best-effort. Only critical checks (the default) turn readiness to 503.
//
// This is the difference between "one dependency flapped" and "every replica
// was pulled out of the load balancer at once".
func Optional() CheckOption { return func(c *check) { c.optional = true } }

type check struct {
	fn       CheckFunc
	optional bool
}

type result struct {
	err       error
	checkedAt time.Time
	took      time.Duration
}

const (
	defaultHealthInterval = 10 * time.Second
	defaultHealthTimeout  = 3 * time.Second
)

// Health tracks liveness and readiness. Liveness ("am I running?") is a flat
// 200 once the process is up. Readiness ("can I serve traffic?") combines an
// explicit gate the server flips during startup and shutdown with the
// registered checks.
//
// Checks are not run by the probe. [Health.Run] evaluates them all in the
// background every interval (each bounded by timeout, in parallel), and
// /readyz answers from the cached results — so a probe returns in
// microseconds, a hundred pods probing every few seconds do not turn into a
// hundred pings per second on the database, and a slow dependency cannot
// make the probe itself time out. Results older than two intervals (the loop
// is not running, or is stuck) are refreshed inline, serialised so
// concurrent probes trigger a single refresh.
//
// Every result is exported as health_check_up{check,critical} (1 healthy,
// 0 failing), so "which dependency is down" is a metric, not a log grep.
type Health struct {
	mu      sync.RWMutex
	checks  map[string]*check
	results map[string]result
	ready   atomic.Bool

	interval, timeout time.Duration
	refreshMu         sync.Mutex // serialises inline refreshes

	up *prometheus.GaugeVec
}

// NewHealth returns an empty registry. interval is how often [Health.Run]
// re-evaluates the checks, timeout bounds each check; zero means the defaults
// (10s / 3s). Until SetReady(true) is called the readiness endpoint reports
// 503, so a service is never advertised as ready before it has finished
// binding its listener.
func NewHealth(interval, timeout time.Duration) *Health {
	if interval <= 0 {
		interval = defaultHealthInterval
	}
	if timeout <= 0 {
		timeout = defaultHealthTimeout
	}
	return &Health{
		checks:   make(map[string]*check),
		results:  make(map[string]result),
		interval: interval,
		timeout:  timeout,
		up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "health_check_up",
			Help: "Readiness check result: 1 healthy, 0 failing. critical=\"true\" checks gate /readyz.",
		}, []string{"check", "critical"}),
	}
}

// Register adds (or replaces) a named readiness check. Checks are critical
// unless [Optional] is given.
func (h *Health) Register(name string, fn CheckFunc, opts ...CheckOption) {
	c := &check{fn: fn}
	for _, o := range opts {
		o(c)
	}
	h.mu.Lock()
	h.checks[name] = c
	delete(h.results, name) // force a fresh evaluation
	h.mu.Unlock()
}

// SetReady flips the readiness gate. The server sets it true once listening
// and false at the start of shutdown so load balancers drain it cleanly.
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// Collectors returns the health metrics for the server's registry.
func (h *Health) Collectors() []prometheus.Collector { return []prometheus.Collector{h.up} }

// Run evaluates every check now and then every interval until ctx is
// cancelled. [Server.Run] starts it; call it yourself only when you use
// Health without a Server.
func (h *Health) Run(ctx context.Context) {
	h.refresh(ctx)
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.refresh(ctx)
		}
	}
}

// refresh runs all checks in parallel, each under its own timeout, and stores
// the results.
func (h *Health) refresh(ctx context.Context) {
	h.mu.RLock()
	names := make([]string, 0, len(h.checks))
	fns := make([]CheckFunc, 0, len(h.checks))
	crit := make([]bool, 0, len(h.checks))
	for name, c := range h.checks {
		names = append(names, name)
		fns = append(fns, c.fn)
		crit = append(crit, !c.optional)
	}
	h.mu.RUnlock()

	out := make([]result, len(names))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func(i int, fn CheckFunc) {
			defer wg.Done()
			out[i] = runCheck(ctx, fn, h.timeout)
		}(i, fn)
	}
	wg.Wait()

	h.mu.Lock()
	for i, name := range names {
		if _, still := h.checks[name]; !still {
			continue // unregistered while we were running
		}
		h.results[name] = out[i]
		v := 1.0
		if out[i].err != nil {
			v = 0
		}
		h.up.WithLabelValues(name, boolString(crit[i])).Set(v)
	}
	h.mu.Unlock()
}

// runCheck runs fn under timeout and does not wait longer than that: a check
// that ignores its context (a client library blocking on a frozen broker
// until its own, longer, deadline) is recorded as timed out and left to
// finish in the background. Otherwise one such check would stall the whole
// refresh loop and every probe with it — found by the chaos suite, where a
// frozen Kafka turned readiness degradation into a 15-second wait.
func runCheck(ctx context.Context, fn CheckFunc, timeout time.Duration) result {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		defer cancel()
		done <- fn(cctx)
	}()
	select {
	case err := <-done:
		return result{err: err, checkedAt: time.Now(), took: time.Since(start)}
	case <-cctx.Done():
		return result{err: fmt.Errorf("check did not return within %s: %w", timeout, context.DeadlineExceeded),
			checkedAt: time.Now(), took: time.Since(start)}
	}
}

func boolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// snapshot returns the cached results, refreshing inline first when any check
// has no result or one older than two intervals plus the check timeout — a
// slow dependency makes every background pass last the full timeout, and
// that must not turn every probe into an inline (slow) refresh. Found by the
// chaos suite: with Postgres answering in 3s the probe took 2s instead of
// microseconds.
func (h *Health) snapshot(ctx context.Context) (map[string]result, map[string]bool) {
	if h.stale() {
		h.refreshMu.Lock()
		if h.stale() { // double-checked: a concurrent probe may have refreshed
			h.refresh(ctx)
		}
		h.refreshMu.Unlock()
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	res := make(map[string]result, len(h.results))
	critical := make(map[string]bool, len(h.checks))
	for name, c := range h.checks {
		res[name] = h.results[name]
		critical[name] = !c.optional
	}
	return res, critical
}

func (h *Health) stale() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	limit := time.Now().Add(-(2*h.interval + h.timeout))
	for name := range h.checks {
		r, ok := h.results[name]
		if !ok || r.checkedAt.Before(limit) {
			return true
		}
	}
	return false
}

// LiveHandler reports process liveness — always 200 while the handler runs.
func (h *Health) LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// ReadyHandler reports readiness from the cached check results:
//
//	200 {"status":"ready"}      gate open, every check passing
//	200 {"status":"degraded"}   gate open, only optional checks failing
//	503 {"status":"not_ready"}  gate closed, or a critical check failing
//
// with a per-check breakdown ("ok" or the error) in "checks".
func (h *Health) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
			return
		}
		results, critical := h.snapshot(r.Context())

		names := make([]string, 0, len(results))
		for name := range results {
			names = append(names, name)
		}
		sort.Strings(names) // deterministic output for tests + humans

		checks := make(map[string]string, len(names))
		criticalFailing, optionalFailing := false, false
		for _, name := range names {
			res := results[name]
			if res.err == nil {
				checks[name] = "ok"
				continue
			}
			checks[name] = res.err.Error()
			if critical[name] {
				criticalFailing = true
			} else {
				optionalFailing = true
			}
		}

		status, body := http.StatusOK, "ready"
		switch {
		case criticalFailing:
			status, body = http.StatusServiceUnavailable, "not_ready"
		case optionalFailing:
			body = "degraded"
		}
		writeJSON(w, status, map[string]any{"status": body, "checks": checks})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
