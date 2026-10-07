package resilient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

const (
	errBodyKeep  = 4 << 10  // bytes of an error response kept in OutboundError.Body
	errBodyDrain = 64 << 10 // bytes of an error response drained so the connection is reused
)

var errUnknownTarget = errors.New("unknown target")

// Option configures a [Client].
type Option func(*options)

type options struct {
	hc  *http.Client
	log *slog.Logger
	reg prometheus.Registerer
}

// WithHTTPClient builds on a copy of hc (its Transport, Jar, Timeout and
// CheckRedirect, which runs after the redirect policy) instead of a client
// built from the pool settings of [Config]. hc itself is not modified, and
// [Client.Shutdown] does not close its idle connections.
func WithHTTPClient(hc *http.Client) Option { return func(o *options) { o.hc = hc } }

// WithLogger sets the logger (default: discard).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithRegisterer registers the metrics on r instead of a private registry.
// Two clients on one registerer collide; wrap it with
// prometheus.WrapRegistererWith to tell them apart.
func WithRegisterer(r prometheus.Registerer) Option { return func(o *options) { o.reg = r } }

// policy is one target's resilience policy.
type policy struct {
	name     string
	cfg      TargetConfig
	limiter  *rate.Limiter // nil: no rate limit
	breaker  *breaker      // nil: no breaker
	bulkhead *bulkhead     // nil: no concurrency cap
	budget   *retryBudget  // nil: no retry budget
	logLimit *rate.Limiter // bounds warn logs per target
}

// Client sends HTTP requests under a per-target policy. It is safe for
// concurrent use; build one per process with [New].
type Client struct {
	hc        *http.Client
	closeIdle func() // nil when the transport is the caller's
	userAgent string
	log       *slog.Logger
	m         *metrics
	reg       *prometheus.Registry
	targets   map[string]*policy

	inFlight atomic.Int64
	closing  atomic.Bool
	idle     chan struct{}
}

// New builds a Client; cfg must pass [Config.Validate].
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	c := &Client{userAgent: cfg.UserAgent, log: o.log, targets: make(map[string]*policy, len(cfg.Targets)), idle: make(chan struct{}, 1)}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	if o.reg == nil {
		c.reg = prometheus.NewRegistry()
		o.reg = c.reg
	}
	m, err := newMetrics(o.reg)
	if err != nil {
		return nil, fmt.Errorf("resilient: register metrics: %w", err)
	}
	c.m = m

	var hc http.Client
	if o.hc != nil {
		hc = *o.hc // a copy: the caller's client is never modified
	} else {
		t := newTransport(&cfg)
		hc.Transport = t
		c.closeIdle = t.CloseIdleConnections
	}
	hc.Transport = instrument(hc.Transport)
	hc.CheckRedirect = redirectPolicy(cfg.MaxRedirects, cfg.AllowCrossHostRedirects, hc.CheckRedirect)
	c.hc = &hc

	for i := range cfg.Targets {
		c.targets[cfg.Targets[i].Name] = c.newPolicy(cfg.Targets[i])
	}
	return c, nil
}

func (c *Client) newPolicy(tc TargetConfig) *policy {
	p := &policy{name: tc.Name, cfg: tc, logLimit: rate.NewLimiter(1, 10)}
	if tc.RateLimit > 0 {
		burst := tc.RateBurst
		if burst == 0 {
			burst = int(max(1, math.Ceil(tc.RateLimit)))
		}
		p.limiter = rate.NewLimiter(rate.Limit(tc.RateLimit), burst)
	}
	p.breaker = newBreaker(&tc, time.Now, func(from, to BreakerState) {
		c.m.transitions.WithLabelValues(tc.Name, from.String(), to.String()).Inc()
		// The current state, not `to`: concurrent notifications may run out
		// of order, but each reads the state after its own transition.
		c.m.breakerState.WithLabelValues(tc.Name).Set(float64(p.breaker.state()))
		c.log.Info("resilient: circuit breaker "+to.String(), "target", tc.Name, "from", from.String())
	})
	p.bulkhead = newBulkhead(&tc)
	p.budget = newRetryBudget(&tc, time.Now)

	c.m.breakerState.WithLabelValues(tc.Name).Set(0)
	if p.bulkhead != nil {
		c.m.bulkheadInUse.WithLabelValues(tc.Name).Set(0)
	}
	return p
}

// Registry is the private metrics registry, or nil under [WithRegisterer].
func (c *Client) Registry() *prometheus.Registry { return c.reg }

// BreakerState is target's circuit-breaker state (closed when it has none).
func (c *Client) BreakerState(target string) (BreakerState, error) {
	p, ok := c.targets[target]
	if !ok {
		return 0, &OutboundError{Kind: KindInvalid, Target: target, Err: errUnknownTarget}
	}
	return p.breaker.state(), nil
}

// Send sends req once under target's policy. req's context is the overall
// deadline; the target's timeout bounds the attempt, waiting for the rate
// limiter and bulkhead included.
//
// A response with status < 400 is returned with a nil error; the caller must
// close its body (that releases the bulkhead slot and the attempt's timeout).
// Everything else is an [*OutboundError] and a nil response.
func (c *Client) Send(target string, req *http.Request) (*http.Response, error) {
	return c.send(target, req, false)
}

// SendWithRetry is [Client.Send] with retries, for requests that are safe to
// repeat: methods GET, HEAD, OPTIONS, PUT, DELETE, or any request carrying an
// Idempotency-Key header, and whose body is replayable (nil, http.NoBody or
// GetBody set — http.NewRequest sets it for in-memory bodies). Anything else
// is sent once.
//
// It retries timeouts, connection errors and statuses 408, 425, 429 and 5xx
// but 501/505, up to the target's retry_max_attempts, waiting full-jitter
// backoff or the response's Retry-After, whichever is longer. It stops
// early when the caller's context would expire during the wait, when a
// Retry-After exceeds retry_max_delay, or when the target's retry budget is
// spent; the last error is returned then.
func (c *Client) SendWithRetry(target string, req *http.Request) (*http.Response, error) {
	return c.send(target, req, true)
}

func (c *Client) send(target string, req *http.Request, retry bool) (*http.Response, error) {
	p, ok := c.targets[target]
	if !ok {
		return nil, &OutboundError{Kind: KindInvalid, Target: target, Err: errUnknownTarget}
	}
	if req.URL == nil || (req.URL.Scheme != "http" && req.URL.Scheme != "https") || req.URL.Host == "" {
		return nil, &OutboundError{Kind: KindInvalid, Target: target, Err: errors.New("request URL must be absolute http(s)")}
	}
	ctx := req.Context()
	method := methodLabel(req.Method)

	if !c.enter() {
		c.count(p, method, KindShutdown.String())
		return nil, &OutboundError{Kind: KindShutdown, Target: target}
	}
	leave := c.leave // handed over to the response body on success
	defer func() {
		if leave != nil {
			leave()
		}
	}()

	p.budget.request()
	attempts := 1
	if retry && p.cfg.RetryMaxAttempts > 1 && idempotent(req) &&
		(req.Body == nil || req.Body == http.NoBody || req.GetBody != nil) {
		attempts = p.cfg.RetryMaxAttempts
	}

	for n := 1; ; n++ {
		if err := ctx.Err(); err != nil {
			c.count(p, method, KindCanceled.String())
			return nil, &OutboundError{Kind: KindCanceled, Target: target, Err: err}
		}
		resp, release, oe := c.attempt(ctx, p, req, n, method)
		if oe == nil {
			l := leave
			leave = nil
			resp.Body = &releaseBody{ReadCloser: resp.Body, release: func() { release(); l() }}
			return resp, nil
		}
		if n >= attempts || !retryable(oe) {
			return nil, oe
		}
		delay := max(fullJitter(n, p.cfg.RetryBaseDelay, p.cfg.RetryMaxDelay), oe.RetryAfter)
		if oe.RetryAfter > p.cfg.RetryMaxDelay {
			return nil, oe
		}
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= delay {
			return nil, oe
		}
		if !p.budget.tryRetry() {
			c.m.budgetExhausted.WithLabelValues(target).Inc()
			c.warn(ctx, p, "resilient: retry budget exhausted", "url", logURL(req.URL))
			return nil, oe
		}
		if !sleep(ctx, delay) {
			c.count(p, method, KindCanceled.String())
			return nil, &OutboundError{Kind: KindCanceled, Target: target, Err: ctx.Err()}
		}
		c.m.retries.WithLabelValues(target).Inc()
		c.log.DebugContext(ctx, "resilient: retrying", "target", target, "attempt", n+1, "after", oe.Kind.String())
	}
}

// attempt sends req once. On success it returns the response and the release
// func the caller must run when done with it; on failure an error, with
// everything already released.
func (c *Client) attempt(ctx context.Context, p *policy, req *http.Request, n int, method string) (*http.Response, func(), *OutboundError) {
	// The attempt timeout starts before the waits, so they count against it.
	actx, cancel := ctx, context.CancelFunc(func() {})
	if p.cfg.Timeout > 0 {
		actx, cancel = context.WithTimeout(ctx, p.cfg.Timeout)
	}
	fail := func(kind Kind, err error) (*http.Response, func(), *OutboundError) {
		cancel()
		c.count(p, method, kind.String())
		oe := &OutboundError{Kind: kind, Target: p.name, Err: err}
		if kind != KindCanceled {
			c.warn(ctx, p, "resilient: request rejected", "url", logURL(req.URL), "reason", kind.String())
		}
		return nil, nil, oe
	}

	tk, ok := p.breaker.allow()
	if !ok {
		return fail(KindCircuitOpen, nil)
	}
	// From here the breaker must hear how the attempt ended. A local
	// rejection is "ignored": it releases a half-open probe slot.
	if p.limiter != nil {
		if err := p.limiter.Wait(actx); err != nil {
			p.breaker.record(tk, outcomeIgnored)
			if ctx.Err() != nil {
				return fail(KindCanceled, ctx.Err())
			}
			return fail(KindRateLimited, err)
		}
	}
	if !p.bulkhead.acquire(actx) {
		p.breaker.record(tk, outcomeIgnored)
		if ctx.Err() != nil {
			return fail(KindCanceled, ctx.Err())
		}
		c.m.bulkheadReject.WithLabelValues(p.name).Inc()
		return fail(KindBulkheadFull, nil)
	}
	if p.bulkhead != nil {
		c.m.bulkheadInUse.WithLabelValues(p.name).Inc()
	}
	release := func() {
		if p.bulkhead != nil {
			p.bulkhead.release()
			c.m.bulkheadInUse.WithLabelValues(p.name).Dec()
		}
		cancel()
	}

	r := req.Clone(context.WithValue(actx, targetKey{}, p.name))
	if n > 1 && req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			release()
			p.breaker.record(tk, outcomeIgnored)
			return nil, nil, &OutboundError{Kind: KindInvalid, Target: p.name, Err: fmt.Errorf("replay body: %w", err)}
		}
		r.Body = body
	}
	if c.userAgent != "" && r.Header.Get("User-Agent") == "" {
		r.Header.Set("User-Agent", c.userAgent)
	}

	start := time.Now()
	resp, err := c.hc.Do(r) //nolint:gosec // G704: sending the caller's request is this package's purpose; redirects are host-pinned
	c.m.duration.WithLabelValues(p.name, method).Observe(time.Since(start).Seconds())

	if err != nil {
		kind, o := classify(ctx, actx, err) // before release cancels actx
		release()
		p.breaker.record(tk, o)
		c.count(p, method, kind.String())
		if kind != KindCanceled {
			c.warn(ctx, p, "resilient: request failed", "url", logURL(req.URL), "reason", kind.String(), "err", err)
		}
		return nil, nil, &OutboundError{Kind: kind, Target: p.name, Err: err}
	}

	code := resp.StatusCode
	if code >= 500 {
		p.breaker.record(tk, outcomeFailure)
	} else {
		p.breaker.record(tk, outcomeSuccess)
	}
	c.count(p, method, statusOutcome(code))
	if code < 400 {
		return resp, release, nil
	}

	oe := &OutboundError{Kind: KindStatus, Target: p.name, StatusCode: code}
	if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
		oe.RetryAfter = d
	}
	oe.Body, _ = io.ReadAll(io.LimitReader(resp.Body, errBodyKeep))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errBodyDrain))
	_ = resp.Body.Close()
	release()
	if code >= 500 {
		c.warn(ctx, p, "resilient: server error", "url", logURL(req.URL), "status", code)
	} else {
		c.log.DebugContext(ctx, "resilient: client error", "target", p.name, "url", logURL(req.URL), "status", code)
	}
	return nil, nil, oe
}

// classify maps a transport error to its kind and what it tells the breaker.
func classify(parent, attempt context.Context, err error) (Kind, outcome) {
	var ne net.Error
	switch {
	case isRedirectError(err):
		return KindRedirect, outcomeSuccess
	case parent.Err() != nil:
		return KindCanceled, outcomeIgnored
	case errors.Is(attempt.Err(), context.DeadlineExceeded), errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return KindTimeout, outcomeFailure
	}
	return KindConnection, outcomeFailure
}

func (c *Client) count(p *policy, method, outcome string) {
	c.m.requests.WithLabelValues(p.name, method, outcome).Inc()
}

// warn logs at warn level, at most about once a second per target.
func (c *Client) warn(ctx context.Context, p *policy, msg string, args ...any) {
	if p.logLimit.Allow() {
		c.log.WarnContext(ctx, msg, append([]any{"target", p.name}, args...)...)
	}
}

// logURL is u without user info, query or fragment: those carry secrets.
func logURL(u *url.URL) string {
	return u.Scheme + "://" + u.Host + u.EscapedPath()
}

// sleep waits d or until ctx is done, reporting whether it waited d.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// releaseBody runs release once, on the first Close.
type releaseBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *releaseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

// enter counts a request in flight, or refuses it once shutdown began. The
// count goes up before the check, so Shutdown either sees the request or the
// request sees Shutdown — none slips between.
func (c *Client) enter() bool {
	c.inFlight.Add(1)
	if c.closing.Load() {
		c.leave()
		return false
	}
	return true
}

func (c *Client) leave() {
	if c.inFlight.Add(-1) == 0 && c.closing.Load() {
		select {
		case c.idle <- struct{}{}:
		default:
		}
	}
}

// Shutdown refuses new requests ([KindShutdown]) and waits until every
// request in flight is done — a successful one when its body is closed — or
// ctx ends. Then it closes idle connections (of a transport the client
// built). Safe to call more than once.
func (c *Client) Shutdown(ctx context.Context) error {
	c.closing.Store(true)
	if c.closeIdle != nil {
		defer c.closeIdle()
	}
	for {
		n := c.inFlight.Load()
		if n == 0 {
			return nil
		}
		select {
		case <-c.idle:
		case <-ctx.Done():
			return fmt.Errorf("resilient: shutdown with %d requests in flight: %w", n, ctx.Err())
		}
	}
}
