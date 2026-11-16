package grpcx

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/wundernightmare/golang-basics/libs/resilience"
)

// Option configures a [Client].
type Option func(*options)

type options struct {
	log  *slog.Logger
	reg  prometheus.Registerer
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
	dial []grpc.DialOption
}

// WithLogger sets the logger (default: discard).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithRegisterer registers the metrics on r instead of a private registry;
// [Client.Close] takes them off r again. Two clients on one registerer
// collide ([New] returns the error): give each of them its own view of it,
// prometheus.WrapRegistererWith(prometheus.Labels{"client": "…"}, r).
func WithRegisterer(r prometheus.Registerer) Option { return func(o *options) { o.reg = r } }

// WithTracerProvider sets the provider the client spans (otelgrpc) come from.
// Without it the global provider (otel.GetTracerProvider, installed by
// otelx.Init) is used.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithPropagator sets the propagator that injects traceparent into every
// call. Without it the global one is used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.prop = p }
}

// WithDialOptions appends dial options to every target's connection, after
// the ones built from its config — so they win where gRPC lets the last one
// win: transport credentials (mTLS), a custom dialer.
//
// An interceptor given with grpc.WithChainUnaryInterceptor /
// WithChainStreamInterceptor runs inside the package's own: after the
// breaker admitted the call, under the target's deadline, and what it
// returns is what the breaker and the metrics see — so one that refuses a
// call itself returns [LocalRejection]. One given with
// grpc.WithUnaryInterceptor / WithStreamInterceptor runs outside all of
// that (gRPC puts it first), and sees the calls the breaker rejects.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dial = append(o.dial, opts...) }
}

type target struct {
	conn   *grpc.ClientConn
	policy *policy
}

// Client holds one gRPC connection per configured target. It is safe for
// concurrent use; build one per process with [New].
type Client struct {
	log     *slog.Logger
	m       *metrics
	reg     *prometheus.Registry
	shared  bool // the metrics are on the caller's registerer
	targets map[string]*target

	mu     sync.Mutex
	closed bool
}

// New builds a Client; cfg must pass [Config.Validate]. No connection is
// dialled yet: gRPC connects on the first call.
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var applied options
	for _, opt := range opts {
		opt(&applied)
	}

	client := &Client{log: applied.log, targets: make(map[string]*target, len(cfg.Targets))}
	if client.log == nil {
		client.log = slog.New(slog.DiscardHandler)
	}

	if applied.reg == nil {
		client.reg = prometheus.NewRegistry()
		applied.reg = client.reg
	}

	m, err := newMetrics(applied.reg)
	if err != nil {
		return nil, fmt.Errorf("grpcx: register metrics: %w", err)
	}

	client.m = m
	client.shared = client.reg == nil

	for i := range cfg.Targets {
		t, err := client.dial(&cfg.Targets[i], &applied)
		if err != nil {
			_ = client.Close()

			return nil, err
		}

		client.targets[cfg.Targets[i].Name] = t
	}

	return client, nil
}

func (c *Client) dial(tc *TargetConfig, applied *options) (*target, error) {
	serviceConfig, err := serviceConfigJSON(tc)
	if err != nil {
		return nil, err
	}

	p, err := c.newPolicy(tc)
	if err != nil {
		return nil, fmt.Errorf("grpcx: target %q: breaker: %w", tc.Name, err)
	}

	var tracing []otelgrpc.Option
	if applied.tp != nil {
		tracing = append(tracing, otelgrpc.WithTracerProvider(applied.tp))
	}

	if applied.prop != nil {
		tracing = append(tracing, otelgrpc.WithPropagators(applied.prop))
	}

	creds, err := tc.transport()
	if err != nil {
		return nil, fmt.Errorf("grpcx: target %q: %w", tc.Name, err)
	}

	dial := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		// The policy is this config, not whatever a resolver (a DNS TXT
		// record) would supply.
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithChainUnaryInterceptor(p.unary),
		grpc.WithChainStreamInterceptor(p.stream),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(tracing...)),
		grpc.WithStatsHandler(&attemptCounter{retries: c.m.retries.WithLabelValues(tc.Name)}),
	}
	if params, ok := tc.keepalive(); ok {
		dial = append(dial, grpc.WithKeepaliveParams(params))
	}

	conn, err := grpc.NewClient(tc.Address, append(dial, applied.dial...)...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: target %q: %w", tc.Name, err)
	}

	return &target{conn: conn, policy: p}, nil
}

func (c *Client) newPolicy(tc *TargetConfig) (*policy, error) {
	name := tc.Name
	p := &policy{name: name, timeout: tc.Timeout, m: c.m}
	gauge := func(s resilience.State) { c.m.breakerState.WithLabelValues(name).Set(float64(s)) }

	var err error

	p.breaker, err = resilience.NewBreaker(tc.Breaker, time.Now, func(from, to resilience.State) {
		c.m.transitions.WithLabelValues(name, from.String(), to.String()).Inc()
		p.breaker.Mirror(gauge)
		c.log.Info("grpcx: circuit breaker "+to.String(), "target", name, "from", from.String())
	})
	if err != nil {
		return nil, err
	}

	// So the series is there before the first transition.
	p.breaker.Mirror(gauge)

	return p, nil
}

// Conn is target's connection, to build generated clients on. It is shared:
// do not close it — [Client.Close] does.
func (c *Client) Conn(target string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("grpcx: %w", ErrClosed)
	}

	t, ok := c.targets[target]
	if !ok {
		return nil, fmt.Errorf("grpcx: %q: %w", target, ErrUnknownTarget)
	}

	return t.conn, nil
}

// BreakerState is target's circuit-breaker state (closed when it has none).
// It keeps answering after [Client.Close].
func (c *Client) BreakerState(target string) (resilience.State, error) {
	t, ok := c.targets[target]
	if !ok {
		return 0, fmt.Errorf("grpcx: %q: %w", target, ErrUnknownTarget)
	}

	return t.policy.breaker.State(), nil
}

// Registry is the private metrics registry, or nil under [WithRegisterer].
func (c *Client) Registry() *prometheus.Registry { return c.reg }

// Close closes every connection and takes the metrics off a registerer given
// with [WithRegisterer], so a new client can be built on it. Calls in flight
// fail with Canceled — gRPC has no client-side drain — so close after
// whatever makes the calls has stopped; a call started later fails with
// [ErrClosed]. Safe to call more than once.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	var errs []error

	if c.shared {
		c.m.unregister()
	}

	for name, t := range c.targets {
		t.policy.closed.Store(true)

		if err := t.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("grpcx: close %q: %w", name, err))
		}
	}

	return errors.Join(errs...)
}
