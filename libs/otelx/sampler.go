package otelx

import (
	"encoding/binary"
	"math/rand/v2"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

// sampler is the head-sampling policy of every service.
//
// One rule for a pipeline: the decision is made once, at the root of the
// trace, and everyone downstream follows it. A span with a parent takes the
// parent's sampled flag; only a root is drawn at the ratio. A request
// sampled at the edge stays sampled through every service behind it, and an
// unsampled one is never sampled again at some other service's ratio.
//
// The exception is the public ingress (RedrawRemoteParent): a traceparent
// on a request from the internet is the client's — its sampled flag, and its
// trace id, whose low bits the ratio sampler keys on. Following either would
// let any client have 100% of its requests traced through the whole
// pipeline. For a remote parent the decision is therefore drawn afresh at
// the ratio from a random id, and the client's tracestate is dropped. The
// trace id itself is kept — a caller that sends traceparent can still find
// its trace — and local parents are followed as everywhere.
//
// A span started under [httpx.ForceTrace] (a request with the debug token)
// is sampled whatever the ratio and the parent say, and its tracestate gets
// the debug mark: every service downstream follows the sampled flag as
// usual.
//
// With recordUnsampled (the retroactive buffer is on) a span that is not
// sampled is recorded all the same, so that libs/otelx/retro can export it
// later; its sampled flag, and so the decision downstream, is unchanged.
type sampler struct {
	root            sdktrace.Sampler
	trustRemote     bool
	recordUnsampled bool
	description     string
}

func newSampler(cfg Config, recordUnsampled bool) sdktrace.Sampler {
	root := sdktrace.NeverSample()
	if cfg.SamplerRatio >= 0 {
		root = sdktrace.TraceIDRatioBased(cfg.SamplerRatio)
	}

	description := "ParentBased{root:" + root.Description()
	if cfg.RedrawRemoteParent {
		description += ",remote:redrawn"
	}

	if recordUnsampled {
		description += ",unsampled:recorded"
	}

	return sampler{
		root: root, trustRemote: !cfg.RedrawRemoteParent, recordUnsampled: recordUnsampled,
		description: description + "}",
	}
}

func (s sampler) Description() string { return s.description }

func (s sampler) ShouldSample(params sdktrace.SamplingParameters) sdktrace.SamplingResult {
	result := s.decide(params)
	if s.recordUnsampled && result.Decision == sdktrace.Drop {
		result.Decision = sdktrace.RecordOnly
	}

	return result
}

func (s sampler) decide(params sdktrace.SamplingParameters) sdktrace.SamplingResult {
	parent := trace.SpanContextFromContext(params.ParentContext)
	untrusted := !s.trustRemote && parent.IsRemote()

	// What the new span inherits: nothing from a parent that is not there or
	// not trusted.
	var state trace.TraceState
	if parent.IsValid() && !untrusted {
		state = parent.TraceState()
	}

	switch {
	case httpx.TraceForced(params.ParentContext):
		// A value that cannot be inserted is a programming error in the
		// constants; the trace is sampled either way.
		if marked, err := state.Insert(httpx.DebugTraceStateKey, httpx.DebugTraceStateValue); err == nil {
			state = marked
		}

		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state}
	case !parent.IsValid():
		return sdktrace.SamplingResult{Decision: s.root.ShouldSample(params).Decision}
	case untrusted:
		redrawn := params
		redrawn.TraceID = randomTraceID()

		return sdktrace.SamplingResult{Decision: s.root.ShouldSample(redrawn).Decision}
	case parent.IsSampled():
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: state}
	default:
		return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: state}
	}
}

// randomTraceID is an id for a draw at the ratio: decide hands it to the root
// sampler only, and the span keeps the id it had.
func randomTraceID() trace.TraceID {
	var id trace.TraceID

	binary.BigEndian.PutUint64(id[:8], rand.Uint64()) //nolint:gosec // G404: a sampling draw, not a secret
	binary.BigEndian.PutUint64(id[8:], rand.Uint64()) //nolint:gosec // G404: a sampling draw, not a secret

	return id
}
