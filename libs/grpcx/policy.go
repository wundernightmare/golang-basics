package grpcx

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/wundernightmare/golang-basics/libs/resilience"
)

// policy is one target's circuit breaker and instruments, applied to every
// call as an interceptor. gRPC's own retries run below it, so it sees a call
// once.
type policy struct {
	name string
	// timeout is the target's default deadline, 0 for none. It is a context
	// deadline and not the service config's `timeout`: a channel has no
	// service config until its name has resolved, and a call made before
	// that would wait for the resolver with no deadline at all (gRPC gives a
	// DNS lookup 30 seconds).
	timeout time.Duration
	breaker *resilience.Breaker // nil: no breaker
	m       *metrics
	closed  atomic.Bool // the client was closed
}

// classify is what a finished call tells the breaker.
func classify(err error) resilience.Outcome {
	if errors.Is(err, ErrLocalRejection) {
		return resilience.OutcomeIgnored // an interceptor's own refusal
	}

	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.DataLoss:
		// The dependency failed or did not answer in time. The caller's own
		// deadline counts: a deadline shorter than the target's timeout must
		// not hide an outage.
		return resilience.OutcomeFailure
	case codes.Canceled:
		return resilience.OutcomeIgnored // the caller walked away
	default:
		// OK, or the dependency answered with its verdict on the request
		// (NotFound, InvalidArgument, ResourceExhausted, …).
		return resilience.OutcomeSuccess
	}
}

// attemptsKey carries a call's attempt counter to the stats handler.
type attemptsKey struct{}

// refuse is the error of a call that is not made — after Close, or when the
// breaker said open — and counts it; nil means: make the call.
func (p *policy) refuse(method string, open error) error {
	if p.closed.Load() {
		p.m.requests.WithLabelValues(p.name, method, codeOutcome(codes.Canceled)).Inc()

		return &closedError{target: p.name}
	}

	if open != nil {
		p.m.requests.WithLabelValues(p.name, method, outcomeCircuitOpen).Inc()

		return &rejection{target: p.name}
	}

	return nil
}

// callContext is the context to make a call with: under the target's
// deadline, and with the attempt counter. release frees the deadline's timer
// when the call is over.
func (p *policy) callContext(ctx context.Context) (_ context.Context, release func()) {
	release = func() {}
	if p.timeout > 0 {
		ctx, release = context.WithTimeout(ctx, p.timeout)
	}

	return context.WithValue(ctx, attemptsKey{}, new(atomic.Int32)), release
}

// panicked is the status code a call that panicked is counted under: it
// ended with no status, which is what Unknown says of any error that is not
// one.
const panicked = codes.Unknown

func (p *policy) observe(method string, start time.Time, code codes.Code) {
	p.m.requests.WithLabelValues(p.name, method, codeOutcome(code)).Inc()
	p.m.duration.WithLabelValues(p.name, method).Observe(time.Since(start).Seconds())
}

func (p *policy) unary(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := p.refuse(method, nil); err != nil {
		return err
	}

	var err error

	// Do, not Allow and Record: a call that panics (an interceptor of the
	// caller's, a codec) is recorded too, or a half-open probe would keep its
	// slot for good.
	open := p.breaker.Do(func() resilience.Outcome {
		ctx, release := p.callContext(ctx)
		defer release()

		start, code := time.Now(), panicked

		defer func() { p.observe(method, start, code) }()

		err = invoker(ctx, method, req, reply, conn, opts...)
		code = status.Code(err)

		return classify(err)
	})
	if open != nil {
		return p.refuse(method, open)
	}

	return err
}

// call is one admitted stream: finish or abort settles it, exactly once.
type call struct {
	p      *policy
	method string
	grant  resilience.Ticket
	start  time.Time
	once   sync.Once
}

func (c *call) finish(err error) {
	c.once.Do(func() {
		c.p.breaker.Record(c.grant, classify(err))
		c.p.observe(c.method, c.start, status.Code(err))
	})
}

// abort settles a call that panicked: a failure for the breaker, so a
// half-open probe gives its slot back.
func (c *call) abort() {
	c.once.Do(func() {
		c.p.breaker.Record(c.grant, resilience.OutcomeFailure)
		c.p.observe(c.method, c.start, panicked)
	})
}

func (p *policy) stream(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if err := p.refuse(method, nil); err != nil {
		return nil, err
	}

	// A stream outlives this function, so the breaker's Do does not fit: the
	// ticket is held until the stream ends.
	grant, ok := p.breaker.Allow()
	if !ok {
		return nil, p.refuse(method, resilience.ErrOpen)
	}

	parent := ctx
	ctx, release := p.callContext(ctx)
	admitted := &call{p: p, method: method, grant: grant, start: time.Now()}

	returned := false

	defer func() {
		if !returned { // streamer panicked
			release()
			admitted.abort()
		}
	}()

	cs, err := streamer(ctx, desc, conn, method, opts...)
	returned = true

	if err != nil {
		release()
		admitted.finish(err)

		return nil, err
	}

	s := &policyStream{ClientStream: cs, call: admitted, serverStreams: desc.ServerStreams, release: release}
	// A caller that stops reading must cancel its context (gRPC's own rule):
	// that is when an unfinished stream reports. The stream's context cannot
	// be watched instead — gRPC cancels it at every end, a good one included.
	s.stopWatch = context.AfterFunc(parent, func() {
		release()
		admitted.finish(status.FromContextError(parent.Err()).Err())
	})

	return s, nil
}

// policyStream settles its call when the stream ends: the server's status
// (io.EOF is OK), the single response of a client-streaming call, or the
// caller's context ending first.
type policyStream struct {
	grpc.ClientStream

	call          *call
	serverStreams bool
	stopWatch     func() bool
	release       func() // frees the deadline's timer; the stream is over
}

func (s *policyStream) RecvMsg(m any) error {
	returned := false

	defer func() {
		if !returned { // the codec panicked
			s.stopWatch()
			s.release()
			s.call.abort()
		}
	}()

	err := s.ClientStream.RecvMsg(m)
	returned = true

	if err == nil {
		if !s.serverStreams { // the one response of a client-streaming call
			s.done(nil)
		}

		return nil
	}

	if errors.Is(err, io.EOF) { // the stream ended well
		s.done(nil)
	} else {
		s.done(err)
	}

	return err // io.EOF or gRPC's status error, as is
}

func (s *policyStream) done(err error) {
	s.stopWatch()
	s.release()
	s.call.finish(err)
}

// attemptCounter counts the attempts gRPC makes below the interceptors: a
// call's second and later attempts are its retries.
type attemptCounter struct {
	retries interface{ Inc() }
}

func (h *attemptCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	if n, ok := ctx.Value(attemptsKey{}).(*atomic.Int32); ok && n.Add(1) > 1 {
		h.retries.Inc()
	}

	return ctx
}

func (*attemptCounter) HandleRPC(context.Context, stats.RPCStats) {}

func (*attemptCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*attemptCounter) HandleConn(context.Context, stats.ConnStats) {}
