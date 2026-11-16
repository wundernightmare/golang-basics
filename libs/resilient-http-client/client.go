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

	"github.com/wundernightmare/golang-basics/libs/resilience"
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

const errBodyKeep = 4 << 10 // bytes of an error response kept in OutboundError.Body

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
// Two clients on one registerer collide ([New] returns the error): give each
// its own prometheus.WrapRegistererWith(prometheus.Labels{"client": …}, r) —
// each, because one metric name carries one set of label names.
func WithRegisterer(r prometheus.Registerer) Option { return func(o *options) { o.reg = r } }

// WithTracerProvider sets the provider the client spans (otelhttp) come from.
// Without it the global provider (otel.GetTracerProvider, installed by
// otelx.Init) is used — the OpenTelemetry convention for instrumentation.
// A span's url.full is scheme, host and path: no query string, no user info.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithPropagator sets the propagator that injects traceparent into every
// attempt. Without it the global one is used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.prop = p }
}

type policy struct {
	name     string
	cfg      TargetConfig
	limiter  *rate.Limiter           // nil: no rate limit
	breaker  *resilience.Breaker     // nil: no breaker
	bulkhead *bulkhead               // nil: no concurrency cap
	budget   *resilience.RetryBudget // nil: no retry budget
	logLimit *rate.Limiter           // bounds warn logs per target
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

	gauge := func(s BreakerState) { c.m.breakerState.WithLabelValues(tc.Name).Set(float64(s)) }
	p.breaker = resilience.NewBreaker(tc.Breaker, time.Now, func(from, to BreakerState) {
		c.m.transitions.WithLabelValues(tc.Name, from.String(), to.String()).Inc()
		p.breaker.Mirror(gauge) // not `to`: see resilience.Breaker.Mirror
		c.log.Info("resilient: circuit breaker "+to.String(), "target", tc.Name, "from", from.String())
	})
	p.bulkhead = newBulkhead(&tc)
	p.budget = resilience.NewRetryBudget(tc.RetryBudgetRatio, tc.RetryBudgetMinRetries, tc.RetryBudgetWindow, time.Now)

	p.breaker.Mirror(gauge) // the series is there before the first transition

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
// A body left open is released when the attempt's time is over — the target's
// timeout or the caller's context — so with neither, it is held for good.
// Everything else is an [*OutboundError] and a nil response.
//
// The attempt is not over until the body is: the timeout covers reading it,
// a failed Read returns an [*OutboundError] too ([KindTimeout],
// [KindConnection] or [KindCanceled]), and only then does the circuit breaker
// hear how the attempt ended — a success once the body is read to the end or
// closed. A body that fails is never retried: the response was already
// handed over.
//
// That holds for an upgraded connection too (101 Switching Protocols,
// CONNECT), whose body is the connection: it is cut when the target's timeout
// expires, and the breaker counts a failure. A long-lived upgrade therefore
// needs a target with timeout 0.
func (c *Client) Send(target string, req *http.Request) (*http.Response, error) {
	return c.send(target, req, false)
}

// SendWithRetry is [Client.Send] with retries, for requests that are safe to
// repeat: methods GET, HEAD, OPTIONS, PUT, DELETE, or any request carrying an
// Idempotency-Key header, and whose body is replayable (nil, http.NoBody or
// GetBody set — http.NewRequest sets it for in-memory bodies). Anything else
// is sent once.
//
// It retries timeouts, connection errors (not a rejected server certificate)
// and statuses 408, 425, 429 and 5xx but 501/505, up to the target's
// retry_max_attempts, waiting full-jitter backoff or the response's
// Retry-After, whichever is longer. It stops early when the caller's context
// would expire during the wait, when a Retry-After exceeds retry_max_delay,
// or when the target's retry budget is spent; the last error is returned
// then.
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

		// A successful attempt owns the in-flight count from here: its body
		// gives it back when it is done (see responseBody).
		resp, outErr := c.attempt(ctx, p, req, n, method, c.leave)
		if outErr == nil {
			leave = nil

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

// admission is what an admitted attempt holds until it ends: the breaker's
// ticket, the attempt's context and, once taken, a bulkhead slot. Each is
// given back exactly once, whoever gets there first — the attempt on its way
// out, the response body, or the watch on the attempt's context.
type admission struct {
	c      *Client
	p      *policy
	grant  resilience.Ticket
	cancel context.CancelFunc
	slot   bool // set by the attempt before anything else can call release

	settled  sync.Once
	released sync.Once
}

func (a *admission) settle(o resilience.Outcome) {
	a.settled.Do(func() { a.p.breaker.Record(a.grant, o) })
}

func (a *admission) release() {
	a.released.Do(func() {
		if a.slot {
			a.p.bulkhead.release()
			a.c.m.bulkheadInUse.WithLabelValues(a.p.name).Dec()
		}

		a.cancel()
	})
}

// attempt sends req once. On success it returns the response, whose body
// releases the attempt and then calls done — when it is closed, or when the
// attempt's time is over; on failure an error, with everything already
// released and done not called.
func (c *Client) attempt(ctx context.Context, p *policy, req *http.Request, n int, method string, done func()) (resp *http.Response, outErr *OutboundError) {
	// The attempt timeout starts before the waits, so they count against it.
	actx, cancel := ctx, context.CancelFunc(func() {})
	if p.cfg.Timeout > 0 {
		actx, cancel = context.WithTimeout(ctx, p.cfg.Timeout)
	}

	reject := func(kind Kind, err error) (*http.Response, *OutboundError) {
		c.count(p, method, kind.String())

		if kind != KindCanceled {
			c.warn(ctx, p, "resilient: request rejected", "url", logURL(req.URL), "reason", kind.String())
		}

		return nil, &OutboundError{Kind: kind, Target: p.name, Err: err}
	}

	grant, ok := p.breaker.Allow()
	if !ok {
		cancel()

		return reject(KindCircuitOpen, nil)
	}

	adm := &admission{c: c, p: p, grant: grant, cancel: cancel}

	// Unless a response body took the admission over, it ends here on every
	// way out, a panic of the RoundTripper included: an unrecorded half-open
	// probe would keep its slot for good, and so would a bulkhead slot. A
	// path that knows the outcome settles it first; what is left is a panic,
	// and that is a failure.
	defer func() {
		if resp == nil {
			adm.settle(resilience.OutcomeFailure)
			adm.release()
		}
	}()

	// A local rejection is "ignored": it releases a half-open probe slot.
	if p.limiter != nil {
		if err := p.limiter.Wait(actx); err != nil {
			adm.settle(resilience.OutcomeIgnored)

			if ctx.Err() != nil {
				return reject(KindCanceled, ctx.Err())
			}

			return reject(KindRateLimited, err)
		}
	}

	if !p.bulkhead.acquire(actx) {
		adm.settle(resilience.OutcomeIgnored)

		if ctx.Err() != nil {
			return reject(KindCanceled, ctx.Err())
		}

		c.m.bulkheadReject.WithLabelValues(p.name).Inc()

		return reject(KindBulkheadFull, nil)
	}

	if p.bulkhead != nil {
		adm.slot = true

		c.m.bulkheadInUse.WithLabelValues(p.name).Inc()
	}

	r := req.Clone(context.WithValue(actx, targetKey{}, p.name))
	if n > 1 && req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			adm.settle(resilience.OutcomeIgnored)
			c.count(p, method, KindInvalid.String())

			return nil, &OutboundError{Kind: KindInvalid, Target: p.name, Err: fmt.Errorf("replay body: %w", err)}
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
		kind, o := classify(ctx, actx, err)

		adm.settle(o)
		c.count(p, method, kind.String())

		// http.Client.Do fails with a *url.Error, whose text quotes the URL,
		// query string included: only its cause goes to the log and into
		// OutboundError.Error().
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}

		if kind != KindCanceled {
			c.warn(ctx, p, "resilient: request failed", "url", logURL(req.URL), "reason", kind.String(), "err", err)
		}

		return nil, &OutboundError{Kind: kind, Target: p.name, Err: err}
	}

	code := resp.StatusCode
	c.count(p, method, statusOutcome(code))

	if code < 400 {
		// The dependency has only answered with headers so far: the breaker
		// hears the outcome when the body is done (see responseBody).
		resp.Body = c.newResponseBody(ctx, actx, req, resp.Body, adm, done).readCloser()

		return resp, nil
	}

	if code >= 500 {
		adm.settle(resilience.OutcomeFailure)
	} else {
		adm.settle(resilience.OutcomeSuccess)
	}

	return nil, c.statusError(ctx, p, req, resp)
}

// statusError turns a >= 400 response into its OutboundError: it keeps the
// head of the body, closes it, and logs (warn for 5xx, debug for 4xx). The
// unread rest is net/http's to deal with: on HTTP/1.1 it reads a bounded
// amount for a bounded time after Close to keep the connection (go1.27
// net/http.maybeDrainBody), on HTTP/2 it resets the stream.
func (c *Client) statusError(ctx context.Context, p *policy, req *http.Request, resp *http.Response) *OutboundError {
	code := resp.StatusCode

	outErr := &OutboundError{Kind: KindStatus, Target: p.name, StatusCode: code, Header: resp.Header}
	if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
		outErr.RetryAfter = d
	}

	outErr.Body, _ = io.ReadAll(io.LimitReader(resp.Body, errBodyKeep))
	_ = resp.Body.Close()

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
		// The dependency answered — with a redirect this client refuses to
		// follow, which is not a failure of the dependency.
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

// warn logs at warn level: per target a burst of ten lines, then one a
// second (logLimit, built in newPolicy).
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
// reads or closes. The attempt (bulkhead slot, timeout, in-flight count) is
// released once: by Close, or by the attempt's context ending — after that
// nothing more can be read, so a body the caller forgot must not keep a
// bulkhead slot and hold up Shutdown for the life of the process.
type responseBody struct {
	rc      io.ReadCloser
	c       *Client
	url     string
	parent  context.Context //nolint:containedctx // the body outlives the call that made it; it is the attempt
	attempt context.Context //nolint:containedctx // the body ends with it: its end releases a body nobody closed
	adm     *admission
	done    func() // runs after the admission is released

	closed    atomic.Bool
	closeOnce sync.Once
	stopWatch func() bool
}

func (c *Client) newResponseBody(parent, attempt context.Context, req *http.Request, rc io.ReadCloser, adm *admission, done func()) *responseBody {
	b := &responseBody{rc: rc, c: c, url: logURL(req.URL), parent: parent, attempt: attempt, adm: adm, done: done}
	b.stopWatch = context.AfterFunc(attempt, func() {
		_, o := classify(parent, attempt, attempt.Err())
		b.adm.settle(o)
		b.finish()
	})

	return b
}

// readCloser is b as the response's Body. A 101 Switching Protocols (or
// CONNECT) body is the connection itself and stays writable — for as long as
// the attempt lasts: the target's timeout ends it like any other body, and
// the breaker counts that as a failure (see [Client.Send]).
func (b *responseBody) readCloser() io.ReadCloser {
	if w, ok := b.rc.(io.Writer); ok {
		return &writableBody{responseBody: b, w: w}
	}

	return b
}

// Read reads the body. Its failure is an [*OutboundError].
func (b *responseBody) Read(buf []byte) (int, error) {
	n, err := b.rc.Read(buf)

	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		b.adm.settle(resilience.OutcomeSuccess)
	case b.closed.Load(): // a read after Close: the caller's bug, not the dependency's
	default:
		kind, o := classify(b.parent, b.attempt, err)
		b.adm.settle(o)

		// Once the attempt's context has ended, that is why the read failed,
		// whatever the transport reports for a body cut under it.
		if ctxErr := b.attempt.Err(); ctxErr != nil {
			err = ctxErr
		}

		if kind != KindCanceled {
			b.c.warn(b.parent, b.adm.p, "resilient: response body failed", "url", b.url, "reason", kind.String(), "err", err)
		}

		err = &OutboundError{Kind: kind, Target: b.adm.p.name, Err: err}
	}

	return n, err // io.EOF must stay io.EOF; a failure is already an OutboundError
}

// Close closes the body and releases the attempt, once.
func (b *responseBody) Close() error {
	b.closed.Store(true)

	// Stop the watch and settle before release cancels the attempt's
	// context: closing early is the caller's choice, not a failure.
	b.stopWatch()
	b.adm.settle(resilience.OutcomeSuccess)

	err := b.rc.Close()

	b.finish()

	return err
}

// finish releases the attempt, once: from Close, or from the watch when the
// attempt's context ended with the body still open. In the second case the
// transport has already failed the body; closing it here returns the
// connection (or the stream) without waiting for a caller who may never come.
func (b *responseBody) finish() {
	b.closeOnce.Do(func() {
		_ = b.rc.Close()
		b.adm.release()
		b.done()
	})
}

// writableBody is a responseBody over a writable (upgraded) connection.
type writableBody struct {
	*responseBody

	w io.Writer
}

func (b *writableBody) Write(p []byte) (int, error) {
	return b.w.Write(p)
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
