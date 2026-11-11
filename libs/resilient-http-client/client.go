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
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

// BreakerState is a circuit breaker's state; its value is the
// http_client_circuit_breaker_state gauge.
type BreakerState = resilience.State

// The breaker states.
const (
	// StateClosed lets every request through and counts failures.
	StateClosed = resilience.StateClosed
	// StateOpen rejects every request until the open timeout elapses.
	StateOpen = resilience.StateOpen
	// StateHalfOpen admits a fixed number of probes; they decide whether the
	// breaker closes (all succeed) or re-opens (any fails).
	StateHalfOpen = resilience.StateHalfOpen
)

const (
	errBodyKeep  = 4 << 10  // bytes of an error response kept in OutboundError.Body
	errBodyDrain = 64 << 10 // bytes of an error response drained so the connection is reused
)

var errUnknownTarget = errors.New("unknown target")

// Option configures a [Client].
type Option func(*options)

type options struct {
	hc   *http.Client
	log  *slog.Logger
	reg  prometheus.Registerer
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
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

// WithTracerProvider sets the provider the client spans (otelhttp) come from.
// Without it the global provider (otel.GetTracerProvider, installed by
// otelx.Init) is used — the OpenTelemetry convention for instrumentation.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithPropagator sets the propagator that injects traceparent into every
// attempt. Without it the global one is used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.prop = p }
}

// policy is one target's resilience policy.
type policy struct {
	name     string
	cfg      TargetConfig
	limiter  *rate.Limiter           // nil: no rate limit
	breaker  *resilience.Breaker     // nil: no breaker
	bulkhead *bulkhead               // nil: no concurrency cap
	budget   *resilience.RetryBudget // nil: no retry budget
	logLimit *rate.Limiter           // bounds warn logs per target
	stateMu  sync.Mutex              // serialises breaker-state gauge updates
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
	idle     chan struct{} // closed once shutdown began and nothing is in flight
	idleOnce sync.Once
}

// New builds a Client; cfg must pass [Config.Validate].
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var applied options
	for _, opt := range opts {
		opt(&applied)
	}

	client := &Client{userAgent: cfg.UserAgent, log: applied.log, targets: make(map[string]*policy, len(cfg.Targets)), idle: make(chan struct{})}
	if client.log == nil {
		client.log = slog.New(slog.DiscardHandler)
	}

	if applied.reg == nil {
		client.reg = prometheus.NewRegistry()
		applied.reg = client.reg
	}

	m, err := newMetrics(applied.reg)
	if err != nil {
		return nil, fmt.Errorf("resilient: register metrics: %w", err)
	}

	client.m = m

	var hc http.Client
	if applied.hc != nil {
		hc = *applied.hc // a copy: the caller's client is never modified
	} else {
		t := newTransport(&cfg)
		hc.Transport = t
		client.closeIdle = t.CloseIdleConnections
	}

	hc.Transport = instrument(hc.Transport, applied.tp, applied.prop)
	hc.CheckRedirect = redirectPolicy(cfg.MaxRedirects, cfg.AllowCrossHostRedirects, hc.CheckRedirect)
	client.hc = &hc

	for i := range cfg.Targets {
		client.targets[cfg.Targets[i].Name] = client.newPolicy(cfg.Targets[i])
	}

	return client, nil
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

	p.breaker = resilience.NewBreaker(tc.breaker(), time.Now, func(from, to BreakerState) {
		c.m.transitions.WithLabelValues(tc.Name, from.String(), to.String()).Inc()
		// The current state, not `to`, and read under the lock: concurrent
		// notifications may run out of order, but whichever sets the gauge
		// last has read a state at least as new as every earlier writer's.
		p.stateMu.Lock()
		c.m.breakerState.WithLabelValues(tc.Name).Set(float64(p.breaker.State()))
		p.stateMu.Unlock()
		c.log.Info("resilient: circuit breaker "+to.String(), "target", tc.Name, "from", from.String())
	})
	p.bulkhead = newBulkhead(&tc)
	p.budget = resilience.NewRetryBudget(tc.RetryBudgetRatio, tc.RetryBudgetMinRetries, tc.RetryBudgetWindow, time.Now)

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

	return p.breaker.State(), nil
}

// Send sends req once under target's policy. req's context is the overall
// deadline; the target's timeout bounds the attempt, waiting for the rate
// limiter and bulkhead included.
//
// A response with status < 400 is returned with a nil error; the caller must
// close its body (that releases the bulkhead slot and the attempt's timeout).
// Everything else is an [*OutboundError] and a nil response.
//
// The attempt is not over until the body is: the timeout covers reading it,
// a failed Read returns an [*OutboundError] too ([KindTimeout],
// [KindConnection] or [KindCanceled]), and only then does the circuit breaker
// hear how the attempt ended — a success once the body is read to the end or
// closed. A body that fails is never retried: the response was already
// handed over.
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

	method := methodLabel(req.Method)
	if req.URL == nil || (req.URL.Scheme != "http" && req.URL.Scheme != "https") || req.URL.Host == "" {
		c.count(p, method, KindInvalid.String())
		return nil, &OutboundError{Kind: KindInvalid, Target: target, Err: errors.New("request URL must be absolute http(s)")}
	}

	ctx := req.Context()

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

	p.budget.Request()

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

		resp, body, outErr := c.attempt(ctx, p, req, n, method)
		if outErr == nil {
			l := leave
			leave = nil
			release := body.release
			body.release = func() { release(); l() }

			return resp, nil
		}

		if n >= attempts || !retryable(outErr) {
			return nil, outErr
		}

		delay := max(fullJitter(n, p.cfg.RetryBaseDelay, p.cfg.RetryMaxDelay), outErr.RetryAfter)
		if outErr.RetryAfter > p.cfg.RetryMaxDelay {
			return nil, outErr
		}

		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= delay {
			return nil, outErr
		}

		if !p.budget.TryRetry() {
			c.m.budgetExhausted.WithLabelValues(target).Inc()
			c.warn(ctx, p, "resilient: retry budget exhausted", "url", logURL(req.URL))

			return nil, outErr
		}

		if !sleep(ctx, delay) {
			c.count(p, method, KindCanceled.String())
			return nil, &OutboundError{Kind: KindCanceled, Target: target, Err: ctx.Err()}
		}

		c.m.retries.WithLabelValues(target).Inc()
		c.log.DebugContext(ctx, "resilient: retrying", "target", target, "attempt", n+1, "after", outErr.Kind.String())
	}
}

// attempt sends req once. On success it returns the response and its body
// (already installed as resp.Body), whose Close releases the attempt; on
// failure an error, with everything already released.
func (c *Client) attempt(ctx context.Context, p *policy, req *http.Request, n int, method string) (*http.Response, *responseBody, *OutboundError) {
	// The attempt timeout starts before the waits, so they count against it.
	actx, cancel := ctx, context.CancelFunc(func() {})
	if p.cfg.Timeout > 0 {
		actx, cancel = context.WithTimeout(ctx, p.cfg.Timeout)
	}

	fail := func(kind Kind, err error) (*http.Response, *responseBody, *OutboundError) {
		cancel()
		c.count(p, method, kind.String())

		oe := &OutboundError{Kind: kind, Target: p.name, Err: err}
		if kind != KindCanceled {
			c.warn(ctx, p, "resilient: request rejected", "url", logURL(req.URL), "reason", kind.String())
		}

		return nil, nil, oe
	}

	grant, ok := p.breaker.Allow()
	if !ok {
		return fail(KindCircuitOpen, nil)
	}
	// From here the breaker must hear how the attempt ended. A local
	// rejection is "ignored": it releases a half-open probe slot.
	if p.limiter != nil {
		if err := p.limiter.Wait(actx); err != nil {
			p.breaker.Record(grant, resilience.OutcomeIgnored)

			if ctx.Err() != nil {
				return fail(KindCanceled, ctx.Err())
			}

			return fail(KindRateLimited, err)
		}
	}

	if !p.bulkhead.acquire(actx) {
		p.breaker.Record(grant, resilience.OutcomeIgnored)

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
			p.breaker.Record(grant, resilience.OutcomeIgnored)
			c.count(p, method, KindInvalid.String())

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
		p.breaker.Record(grant, o)
		c.count(p, method, kind.String())

		if kind != KindCanceled {
			c.warn(ctx, p, "resilient: request failed", "url", logURL(req.URL), "reason", kind.String(), "err", err)
		}

		return nil, nil, &OutboundError{Kind: kind, Target: p.name, Err: err}
	}

	code := resp.StatusCode
	c.count(p, method, statusOutcome(code))

	if code < 400 {
		// The dependency has only answered with headers so far: the breaker
		// hears the outcome when the body is done (see responseBody).
		body := c.newResponseBody(ctx, actx, p, req, resp.Body, grant, release)
		resp.Body = body.readCloser()

		return resp, body, nil
	}

	if code >= 500 {
		p.breaker.Record(grant, resilience.OutcomeFailure)
	} else {
		p.breaker.Record(grant, resilience.OutcomeSuccess)
	}

	return nil, nil, c.statusError(ctx, p, req, resp, release)
}

// statusError turns a >= 400 response into its OutboundError: it keeps the
// head of the body, drains and closes the rest, runs release, and logs (warn
// for 5xx, debug for 4xx).
func (c *Client) statusError(ctx context.Context, p *policy, req *http.Request, resp *http.Response, release func()) *OutboundError {
	code := resp.StatusCode

	outErr := &OutboundError{Kind: KindStatus, Target: p.name, StatusCode: code, Header: resp.Header}
	if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
		outErr.RetryAfter = d
	}

	outErr.Body, _ = io.ReadAll(io.LimitReader(resp.Body, errBodyKeep))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errBodyDrain))
	_ = resp.Body.Close()

	release()

	if code >= 500 {
		c.warn(ctx, p, "resilient: server error", "url", logURL(req.URL), "status", code)
	} else {
		c.log.DebugContext(ctx, "resilient: client error", "target", p.name, "url", logURL(req.URL), "status", code)
	}

	return outErr
}

// classify maps a transport error to its kind and what it tells the breaker.
func classify(parent, attempt context.Context, err error) (Kind, resilience.Outcome) {
	var ne net.Error
	switch {
	case isRedirectError(err):
		return KindRedirect, resilience.OutcomeSuccess
	case errors.Is(parent.Err(), context.DeadlineExceeded):
		// The caller's own deadline: reported as a cancellation (the caller's
		// budget, not the target's timeout, ended the attempt), but the
		// dependency did fail to answer in the time it was given — the breaker
		// must hear that, or a caller deadline shorter than the target's
		// timeout would hide an outage from it forever.
		return KindCanceled, resilience.OutcomeFailure
	case parent.Err() != nil:
		return KindCanceled, resilience.OutcomeIgnored
	case errors.Is(attempt.Err(), context.DeadlineExceeded), errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return KindTimeout, resilience.OutcomeFailure
	}

	return KindConnection, resilience.OutcomeFailure
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

// responseBody is the body of a successful response. The attempt lasts as
// long as the body does, so it reports to the circuit breaker when the body
// is done — read to the end or closed: success; a failed Read: by its kind;
// the attempt's context ending first (its timeout, or the caller's context):
// by its kind as well, so the breaker hears even from a caller that never
// reads or closes. Close releases the attempt (bulkhead slot, timeout,
// in-flight count) once.
type responseBody struct {
	rc      io.ReadCloser
	c       *Client
	p       *policy
	url     string
	parent  context.Context //nolint:containedctx // the body outlives the call that made it; it is the attempt
	attempt context.Context //nolint:containedctx // as above
	grant   resilience.Ticket

	settled   sync.Once // the breaker hears once
	closed    atomic.Bool
	closeOnce sync.Once
	stopWatch func() bool
	release   func()
}

func (c *Client) newResponseBody(parent, attempt context.Context, p *policy, req *http.Request, rc io.ReadCloser, grant resilience.Ticket, release func()) *responseBody {
	b := &responseBody{rc: rc, c: c, p: p, url: logURL(req.URL), parent: parent, attempt: attempt, grant: grant, release: release}
	b.stopWatch = context.AfterFunc(attempt, func() {
		_, o := classify(parent, attempt, attempt.Err())
		b.settle(o)
	})

	return b
}

// readCloser is b as the response's Body. A 101 Switching Protocols (or
// CONNECT) body is the connection itself and stays writable.
func (b *responseBody) readCloser() io.ReadCloser {
	if w, ok := b.rc.(io.Writer); ok {
		return &writableBody{responseBody: b, w: w}
	}

	return b
}

func (b *responseBody) settle(o resilience.Outcome) {
	b.settled.Do(func() { b.p.breaker.Record(b.grant, o) })
}

// Read reads the body. Its failure is an [*OutboundError].
func (b *responseBody) Read(buf []byte) (int, error) {
	n, err := b.rc.Read(buf)

	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		b.settle(resilience.OutcomeSuccess)
	case b.closed.Load(): // a read after Close: the caller's bug, not the dependency's
	default:
		kind, o := classify(b.parent, b.attempt, err)
		b.settle(o)

		if kind != KindCanceled {
			b.c.warn(b.parent, b.p, "resilient: response body failed", "url", b.url, "reason", kind.String(), "err", err)
		}

		err = &OutboundError{Kind: kind, Target: b.p.name, Err: err}
	}

	return n, err //nolint:wrapcheck // io.EOF must stay io.EOF; a failure is already an OutboundError
}

// Close closes the body and releases the attempt, once.
func (b *responseBody) Close() error {
	b.closed.Store(true)

	err := b.rc.Close()

	b.closeOnce.Do(func() {
		// Stop the watch and settle before release cancels the attempt's
		// context: closing early is the caller's choice, not a failure.
		b.stopWatch()
		b.settle(resilience.OutcomeSuccess)
		b.release()
	})

	return err //nolint:wrapcheck // the transport's error, as is
}

// writableBody is a responseBody over a writable (upgraded) connection.
type writableBody struct {
	*responseBody

	w io.Writer
}

func (b *writableBody) Write(p []byte) (int, error) {
	return b.w.Write(p) //nolint:wrapcheck // the connection's error, as is
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
	// Once shutdown began no request is admitted, so a count of zero stays
	// zero (refused requests only blip it): idle is final, and closing the
	// channel wakes every Shutdown caller, however many wait.
	if c.inFlight.Add(-1) == 0 && c.closing.Load() {
		c.idleOnce.Do(func() { close(c.idle) })
	}
}

// Shutdown refuses new requests ([KindShutdown]) and waits until every
// request in flight is done — a successful one when its body is closed — or
// ctx ends. Then it closes idle connections (of a transport the client
// built). Safe to call more than once, concurrently too.
func (c *Client) Shutdown(ctx context.Context) error {
	c.closing.Store(true)

	if c.closeIdle != nil {
		defer c.closeIdle()
	}

	// A request that left before closing was set did not close idle; the
	// count read after setting it sees that request gone.
	if c.inFlight.Load() == 0 {
		return nil
	}

	select {
	case <-c.idle:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("resilient: shutdown with %d requests in flight: %w", c.inFlight.Load(), ctx.Err())
	}
}
