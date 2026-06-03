package resilient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// maxDrainOnError caps how much of an error response body we drain before
// closing, so the keep-alive connection can be reused without reading an
// unbounded body.
const maxDrainOnError = 1 << 16

// Request is a single outbound HTTP request to be executed by a [Client].
//
// The zero Method is treated as GET. Body, when non-nil, is sent as-is and is
// safe to replay across retries (it is re-read from a fresh reader each attempt).
type Request struct {
	// Target is the logical policy group. It selects the rate limiter, circuit
	// breaker and adaptive limiter; an unknown target gets a defaulted fallback
	// policy on first use.
	Target string
	// Method is the HTTP method (default GET).
	Method string
	// URL is the fully-rendered request URL.
	URL string
	// Header carries request headers (copied onto the outbound request).
	Header http.Header
	// Body is the request body; nil for GET/HEAD.
	Body []byte
	// Timeout overrides the target/default timeout for this request (0 = inherit).
	Timeout time.Duration
	// UserAgent overrides the client-level User-Agent for this request only.
	UserAgent string
}

// FallbackFunc returns a static response to serve when the upstream is
// unavailable. Registered per-target via [WithFallback].
type FallbackFunc func() CachedResponse

// policySet bundles the per-target resilience policies.
type policySet struct {
	limiter  *rate.Limiter
	breaker  *CircuitBreaker
	adaptive *AdaptiveLimiter // nil when adaptive concurrency is disabled
	cfg      TargetConfig
	timeout  time.Duration
}

// Client is a concurrency-safe, policy-per-target HTTP client. Construct one
// with [New] and share it across the whole process; clone-free reuse of the
// connection pool is automatic.
type Client struct {
	hc             *http.Client
	defaultTimeout time.Duration
	userAgent      string
	metrics        *Metrics
	log            *slog.Logger
	cache          CacheAdapter
	fallbacks      map[string]FallbackFunc

	policies sync.Map // target string -> *policySet

	coalesceMu sync.Mutex
	coalesce   map[string]*coalesceCall

	shuttingDown atomic.Bool
	inFlight     atomic.Int64
	idle         chan struct{}
}

type coalesceCall struct {
	done chan struct{}
	resp CachedResponse
	err  error
}

// Option customizes a [Client] at construction.
type Option func(*clientOptions)

type clientOptions struct {
	metrics   *Metrics
	log       *slog.Logger
	cache     CacheAdapter
	fallbacks map[string]FallbackFunc
	hc        *http.Client
	transport http.RoundTripper
}

// WithMetrics attaches a pre-built [Metrics] (e.g. to share a registry). When
// omitted, a fresh private registry is created.
func WithMetrics(m *Metrics) Option { return func(o *clientOptions) { o.metrics = m } }

// WithLogger sets the structured logger. When omitted, logging is discarded.
func WithLogger(l *slog.Logger) Option { return func(o *clientOptions) { o.log = l } }

// WithCache attaches a read-through cache backend used by [Client.SendCached],
// [Client.SendCoalesced] and [Client.SendWithFallback].
func WithCache(c CacheAdapter) Option { return func(o *clientOptions) { o.cache = c } }

// WithFallback registers a static fallback response for target, served by
// [Client.SendWithFallback] when the upstream fails transiently.
func WithFallback(target string, fn FallbackFunc) Option {
	return func(o *clientOptions) {
		if o.fallbacks == nil {
			o.fallbacks = make(map[string]FallbackFunc)
		}
		o.fallbacks[target] = fn
	}
}

// WithHTTPClient supplies a fully-configured *http.Client, bypassing the
// pool/DNS settings in [Config]. Its Timeout is ignored — per-request timeouts
// are applied via context. Mutually exclusive with [WithTransport].
func WithHTTPClient(hc *http.Client) Option { return func(o *clientOptions) { o.hc = hc } }

// WithTransport supplies a custom [http.RoundTripper], bypassing the pool/DNS
// settings in [Config].
func WithTransport(rt http.RoundTripper) Option { return func(o *clientOptions) { o.transport = rt } }

// New builds a [Client] from cfg and the given options.
func New(cfg Config, opts ...Option) (*Client, error) {
	cfg.withDefaults()

	var o clientOptions
	for _, opt := range opts {
		opt(&o)
	}

	hc := o.hc
	if hc == nil {
		rt := o.transport
		if rt == nil {
			rt = newTransport(cfg)
		}
		hc = &http.Client{Transport: rt}
	}
	// Per-request timeouts are enforced via context, never the client-level
	// Timeout (which would also abort an in-progress body read by the caller).
	hc.Timeout = 0

	metrics := o.metrics
	if metrics == nil {
		metrics = NewMetrics()
	}
	logger := o.log
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	c := &Client{
		hc:             hc,
		defaultTimeout: cfg.DefaultTimeout,
		userAgent:      cfg.UserAgent,
		metrics:        metrics,
		log:            logger,
		cache:          o.cache,
		fallbacks:      o.fallbacks,
		coalesce:       make(map[string]*coalesceCall),
		idle:           make(chan struct{}, 1),
	}

	for i := range cfg.OutboundTargets {
		tc := cfg.OutboundTargets[i]
		c.policies.Store(tc.Name, buildPolicy(tc, cfg.DefaultTimeout))
	}
	return c, nil
}

// newTransport builds a tuned [http.Transport] from cfg, optionally fronted by
// the TTL-aware DNS cache.
func newTransport(cfg Config) *http.Transport {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: cfg.TCPKeepAlive}

	dialContext := dialer.DialContext
	if cfg.DNSCacheEnabled {
		dialContext = newDNSCache(dialer, cfg.DNSMinTTL, cfg.DNSMaxTTL).dialContext
	}

	maxIdle := cfg.PoolMaxIdlePerHost * 4
	if maxIdle < 100 {
		maxIdle = 100
	}
	return &http.Transport{
		DialContext:           dialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   cfg.PoolMaxIdlePerHost,
		IdleConnTimeout:       cfg.PoolIdleTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// Metrics returns the client's metrics handle (for exposing /metrics).
func (c *Client) Metrics() *Metrics { return c.metrics }

// Send executes req under its target's rate-limiter, circuit-breaker and
// adaptive-concurrency policies.
//
// On success it returns the *http.Response and the caller owns the body (close
// it). A non-2xx status is returned as an [OutboundError]: 4xx (except 429) is
// fatal, everything else (429, 5xx, timeouts, connection errors, circuit open,
// rate limited, shutting down) is transient. Use [IsTransient] / [IsFatal] to
// branch.
func (c *Client) Send(ctx context.Context, req Request) (*http.Response, error) {
	if c.shuttingDown.Load() {
		return nil, transient("client is shutting down", nil)
	}

	c.inFlight.Add(1)
	resp, err := c.execute(ctx, req)
	if c.inFlight.Add(-1) == 0 && c.shuttingDown.Load() {
		// Non-blocking notify; buffered so a missed select still drains.
		select {
		case c.idle <- struct{}{}:
		default:
		}
	}
	return resp, err
}

//nolint:gocyclo // a single linear status-classification switch; splitting it hurts readability.
func (c *Client) execute(ctx context.Context, req Request) (*http.Response, error) {
	policy := c.getPolicy(req.Target)
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	start := time.Now()
	target := req.Target
	template := policy.cfg.Selector

	state := policy.breaker.State()
	c.metrics.recordCBState(target, state)

	// 1. Circuit breaker.
	if !policy.breaker.Allow() {
		c.log.Warn("circuit breaker open — request rejected", "target", target, "url", req.URL)
		c.metrics.recordRequest(target, template, method, 0, "circuit_breaker_open", time.Since(start))
		return nil, transient("circuit breaker open", nil)
	}

	// 2. Rate limiter (non-blocking GCRA-style check).
	if !policy.limiter.Allow() {
		c.log.Warn("local rate limit exceeded — request rejected", "target", target, "url", req.URL)
		c.metrics.recordRequest(target, template, method, 0, "rate_limited", time.Since(start))
		return nil, transient("rate limit exceeded", nil)
	}

	// 3. Adaptive concurrency gate.
	if policy.adaptive != nil {
		if err := policy.adaptive.Acquire(ctx); err != nil {
			c.metrics.recordRequest(target, template, method, 0, "concurrency_wait_canceled", time.Since(start))
			return nil, transient("adaptive concurrency wait canceled", err)
		}
		defer policy.adaptive.Release()
	}

	// 4. Resolve timeout and build the request.
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = policy.timeout
	}
	reqCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}

	var bodyReader io.Reader
	if len(req.Body) > 0 {
		bodyReader = bytes.NewReader(req.Body)
	}
	httpReq, err := http.NewRequestWithContext(reqCtx, method, req.URL, bodyReader)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		policy.breaker.RecordFailure()
		c.metrics.recordRequest(target, template, method, 0, "build_error", time.Since(start))
		return nil, fatal("invalid request", err)
	}
	for k, vs := range req.Header {
		for _, v := range vs {
			httpReq.Header.Add(k, v)
		}
	}
	switch {
	case req.UserAgent != "":
		httpReq.Header.Set("User-Agent", req.UserAgent)
	case c.userAgent != "":
		httpReq.Header.Set("User-Agent", c.userAgent)
	}

	c.log.Debug("→ outbound request", "method", method, "url", req.URL, "target", target, "bytes", len(req.Body))

	resp, err := c.hc.Do(httpReq)
	elapsed := time.Since(start)

	// 5. Transport-level error.
	if err != nil {
		if cancel != nil {
			cancel()
		}
		policy.breaker.RecordFailure()
		if policy.adaptive != nil {
			policy.adaptive.OnFailure()
			c.metrics.recordAdaptiveLimit(target, policy.adaptive.CurrentLimit())
		}
		label, oe := classifyTransport(err)
		c.metrics.recordRequest(target, template, method, 0, label, elapsed)
		c.log.Error("← outbound request failed", "target", target, "err", err, "latency_ms", elapsed.Milliseconds())
		return nil, oe
	}

	status := resp.StatusCode

	// 6. Classify by status.
	switch {
	case status >= 200 && status < 300:
		policy.breaker.RecordSuccess()
		if policy.adaptive != nil {
			policy.adaptive.OnSuccess()
			c.metrics.recordAdaptiveLimit(target, policy.adaptive.CurrentLimit())
		}
		c.metrics.recordRequest(target, template, method, status, "ok", elapsed)
		c.log.Debug("← outbound response OK", "target", target, "status", status, "latency_ms", elapsed.Milliseconds())
		if cancel != nil {
			resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
		}
		return resp, nil

	case status == http.StatusTooManyRequests:
		// Upstream rate limit — transient, but do not penalise our CB or limiter.
		drainClose(resp)
		if cancel != nil {
			cancel()
		}
		c.metrics.recordRequest(target, template, method, status, "rate_limited_upstream", elapsed)
		c.log.Warn("← upstream 429 Too Many Requests", "target", target, "latency_ms", elapsed.Milliseconds())
		return nil, transient("HTTP 429 Too Many Requests", nil)

	case status >= 500:
		policy.breaker.RecordFailure()
		if policy.adaptive != nil {
			policy.adaptive.OnFailure()
			c.metrics.recordAdaptiveLimit(target, policy.adaptive.CurrentLimit())
		}
		drainClose(resp)
		if cancel != nil {
			cancel()
		}
		c.metrics.recordRequest(target, template, method, status, "server_error", elapsed)
		c.log.Error("← outbound server error (5xx)", "target", target, "status", status, "latency_ms", elapsed.Milliseconds())
		return nil, transient(fmt.Sprintf("HTTP %d", status), nil)

	default:
		// 4xx (except 429) — malformed request; fatal. CB counts it; adaptive
		// limiter does not (it is a request bug, not a capacity signal).
		policy.breaker.RecordFailure()
		drainClose(resp)
		if cancel != nil {
			cancel()
		}
		c.metrics.recordRequest(target, template, method, status, "client_error", elapsed)
		c.log.Error("← outbound client error (4xx)", "target", target, "status", status, "latency_ms", elapsed.Milliseconds())
		return nil, fatal(fmt.Sprintf("HTTP %d", status), nil)
	}
}

// SendCached wraps [Client.Send] with a read-through cache. For GET/HEAD the
// cache is consulted first; on a successful (2xx) response the buffered body is
// stored under cacheKey with the given ttl. Without a cache configured it simply
// buffers and returns the body.
func (c *Client) SendCached(ctx context.Context, req Request, cacheKey string, ttl time.Duration) (CachedResponse, error) {
	cacheable := isCacheableMethod(req.Method)
	if cacheable && c.cache != nil {
		if hit, ok := c.cache.Get(ctx, cacheKey); ok {
			return hit, nil
		}
	}

	resp, err := c.Send(ctx, req)
	if err != nil {
		return CachedResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return CachedResponse{}, transient("failed to read response body", err)
	}
	out := CachedResponse{Status: resp.StatusCode, Body: body}

	if cacheable && c.cache != nil {
		c.cache.Set(ctx, cacheKey, out, ttl)
	}
	return out, nil
}

// SendWithRetry executes req with full-jitter exponential-backoff retries on
// transient errors. maxAttempts ≤ 0 uses the target's RetryMaxAttempts (clamped
// to at least 1). Fatal errors are returned immediately. Each retry increments
// the retry-attempts metric.
func (c *Client) SendWithRetry(ctx context.Context, req Request, maxAttempts int) (*http.Response, error) {
	policy := c.getPolicy(req.Target)
	if maxAttempts <= 0 {
		maxAttempts = policy.cfg.RetryMaxAttempts
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var attempt int
	for {
		if attempt > 0 {
			if delay := FullJitter(attempt, policy.cfg.RetryBase, policy.cfg.RetryCap); delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return nil, transient("retry canceled", ctx.Err())
				}
			}
			c.metrics.recordRetryAttempt(req.Target)
		}

		resp, err := c.Send(ctx, req)
		if err == nil {
			return resp, nil
		}
		if IsFatal(err) {
			return nil, err
		}
		attempt++
		if attempt >= maxAttempts {
			return nil, err
		}
	}
}

// SendCoalesced deduplicates concurrent GET/HEAD requests sharing cacheKey: the
// first caller (leader) performs the upstream fetch while concurrent callers
// (followers) await its result without hitting the upstream. A fresh cache hit
// short-circuits before coalescing; non-GET/HEAD requests bypass it entirely.
func (c *Client) SendCoalesced(ctx context.Context, req Request, cacheKey string, ttl time.Duration) (CachedResponse, error) {
	if !isCacheableMethod(req.Method) {
		return c.SendCached(ctx, req, cacheKey, ttl)
	}
	if c.cache != nil {
		if hit, ok := c.cache.Get(ctx, cacheKey); ok {
			return hit, nil
		}
	}

	c.coalesceMu.Lock()
	if call, ok := c.coalesce[cacheKey]; ok {
		c.coalesceMu.Unlock()
		c.metrics.recordCoalesceHit(req.Target)
		select {
		case <-call.done:
			return call.resp, call.err
		case <-ctx.Done():
			return CachedResponse{}, transient("coalesced wait canceled", ctx.Err())
		}
	}
	call := &coalesceCall{done: make(chan struct{})}
	c.coalesce[cacheKey] = call
	c.coalesceMu.Unlock()

	call.resp, call.err = c.SendCached(ctx, req, cacheKey, ttl)
	close(call.done)

	c.coalesceMu.Lock()
	delete(c.coalesce, cacheKey)
	c.coalesceMu.Unlock()

	return call.resp, call.err
}

// SendWithFallback wraps [Client.SendCached], substituting a fallback on a
// transient failure. The fallback chain is: stale cache entry (backend-
// dependent), then the static fallback registered via [WithFallback], then the
// original error. Fatal errors bypass the fallback.
func (c *Client) SendWithFallback(ctx context.Context, req Request, cacheKey string, ttl time.Duration) (CachedResponse, error) {
	resp, err := c.SendCached(ctx, req, cacheKey, ttl)
	if err == nil {
		return resp, nil
	}
	if IsFatal(err) {
		return CachedResponse{}, err
	}

	if c.cache != nil {
		if stale, ok := c.cache.Get(ctx, cacheKey); ok {
			c.metrics.recordFallbackHit(req.Target)
			return stale, nil
		}
	}
	if fn, ok := c.fallbacks[req.Target]; ok {
		c.metrics.recordFallbackHit(req.Target)
		return fn(), nil
	}
	return CachedResponse{}, err
}

// ShutdownError is returned by [Client.Shutdown] when in-flight requests do not
// drain before the deadline.
type ShutdownError struct {
	// InFlight is the number of requests still running when the deadline fired.
	InFlight int
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf("shutdown timed out with %d requests still in flight", e.InFlight)
}

// Shutdown begins a graceful shutdown: new requests are rejected with a
// transient error, and the call blocks until in-flight requests drain or ctx is
// done. A drained shutdown returns nil; a deadline returns a [*ShutdownError].
func (c *Client) Shutdown(ctx context.Context) error {
	c.shuttingDown.Store(true)
	c.log.Info("resilient: shutdown initiated, draining in-flight requests")

	if c.inFlight.Load() == 0 {
		return nil
	}
	for {
		select {
		case <-c.idle:
			if c.inFlight.Load() == 0 {
				return nil
			}
		case <-ctx.Done():
			n := int(c.inFlight.Load())
			c.log.Warn("resilient: shutdown timed out", "in_flight", n)
			return &ShutdownError{InFlight: n}
		}
	}
}

// getPolicy returns the policy set for target, lazily creating a defaulted
// fallback policy on first use of an undeclared target.
func (c *Client) getPolicy(target string) *policySet {
	if v, ok := c.policies.Load(target); ok {
		return v.(*policySet)
	}
	c.log.Warn("no config for outbound target — using fallback policy", "target", target)
	p := buildPolicy(fallbackTarget(target), c.defaultTimeout)
	actual, _ := c.policies.LoadOrStore(target, p)
	return actual.(*policySet)
}

func buildPolicy(cfg TargetConfig, defaultTimeout time.Duration) *policySet {
	burst := cfg.RateLimit
	if burst < 1 {
		burst = 1
	}
	var adaptive *AdaptiveLimiter
	if cfg.AdaptiveConcurrencyEnabled {
		adaptive = NewAdaptiveLimiter(cfg.AdaptiveConcurrencyInitial, cfg.AdaptiveConcurrencyMin, cfg.AdaptiveConcurrencyMax)
	}
	return &policySet{
		limiter:  rate.NewLimiter(rate.Limit(cfg.RateLimit), burst),
		breaker:  NewCircuitBreaker(cfg.CBThreshold, cfg.CBMinRequests, cfg.CBWindow, cfg.CBHalfOpenTimeout),
		adaptive: adaptive,
		cfg:      cfg,
		timeout:  cfg.resolveTimeout(defaultTimeout),
	}
}

func isCacheableMethod(method string) bool {
	return method == "" || method == http.MethodGet || method == http.MethodHead
}

// drainClose drains a bounded prefix of an error response body then closes it,
// so the keep-alive connection can be reused.
func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainOnError))
	_ = resp.Body.Close()
}

// cancelBody ties a context's cancel func to the response body's lifetime: the
// per-request timeout stays armed until the caller closes the body.
type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}
