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

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

// policy is one target's circuit breaker and instruments, applied to every
// call as an interceptor. gRPC's own retries run below it, so it sees a call
// once.
type policy struct {
	name    string
	breaker *resilience.Breaker // nil: no breaker
	m       *metrics
	stateMu sync.Mutex // serialises breaker-state gauge updates
}

// classify is what a finished call tells the breaker.
func classify(err error) resilience.Outcome {
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

// call is one admitted call: finish settles it exactly once.
type call struct {
	p      *policy
	method string
	grant  resilience.Ticket
	start  time.Time
	once   sync.Once
}

// begin admits a call or rejects it; ctx is the one to make the call with.
func (p *policy) begin(ctx context.Context, method string) (context.Context, *call, error) {
	grant, ok := p.breaker.Allow()
	if !ok {
		p.m.requests.WithLabelValues(p.name, method, outcomeCircuitOpen).Inc()

		return ctx, nil, &rejection{target: p.name}
	}

	return context.WithValue(ctx, attemptsKey{}, new(atomic.Int32)),
		&call{p: p, method: method, grant: grant, start: time.Now()}, nil
}

func (c *call) finish(err error) {
	c.once.Do(func() {
		c.p.breaker.Record(c.grant, classify(err))
		c.p.m.requests.WithLabelValues(c.p.name, c.method, codeOutcome(status.Code(err))).Inc()
		c.p.m.duration.WithLabelValues(c.p.name, c.method).Observe(time.Since(c.start).Seconds())
	})
}

func (p *policy) unary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, admitted, err := p.begin(ctx, method)
	if err != nil {
		return err
	}

	err = invoker(ctx, method, req, reply, cc, opts...)
	admitted.finish(err)

	return err //nolint:wrapcheck // gRPC's status error, as is
}

func (p *policy) stream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	parent := ctx

	ctx, admitted, err := p.begin(ctx, method)
	if err != nil {
		return nil, err
	}

	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		admitted.finish(err)

		return nil, err //nolint:wrapcheck // gRPC's status error, as is
	}

	s := &policyStream{ClientStream: cs, call: admitted, serverStreams: desc.ServerStreams}
	// A caller that stops reading must cancel its context (gRPC's own rule):
	// that is when an unfinished stream reports. The stream's context cannot
	// be watched instead — gRPC cancels it at every end, a good one included.
	s.stopWatch = context.AfterFunc(parent, func() { admitted.finish(status.FromContextError(parent.Err()).Err()) })

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
}

func (s *policyStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
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

	return err //nolint:wrapcheck // io.EOF or gRPC's status error, as is
}

func (s *policyStream) done(err error) {
	s.stopWatch()
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
