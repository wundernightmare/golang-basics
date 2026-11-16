package otelx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tracehubmmp/golang-basics/libs/httpx"
)

const (
	sampled  = sdktrace.RecordAndSample
	dropped  = sdktrace.Drop
	recorded = sdktrace.RecordOnly
)

// A trace id the ratio sampler keeps at any ratio above zero (its low bits
// are the lowest there are), and one it drops at any ratio below one.
var (
	alwaysKept  = trace.TraceID{0: 1}
	neverKept   = trace.TraceID{0: 1, 8: 0xff, 9: 0xff, 10: 0xff, 11: 0xff, 12: 0xff, 13: 0xff, 14: 0xff, 15: 0xff}
	vendorState = "vendor=x"
)

func parentContext(t *testing.T, id trace.TraceID, isSampled, remote bool, state string) context.Context {
	t.Helper()

	cfg := trace.SpanContextConfig{TraceID: id, SpanID: trace.SpanID{1}, Remote: remote}
	if isSampled {
		cfg.TraceFlags = trace.FlagsSampled
	}

	if state != "" {
		parsed, err := trace.ParseTraceState(state)
		require.NoError(t, err)

		cfg.TraceState = parsed
	}

	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(cfg))
}

func sample(s sdktrace.Sampler, ctx context.Context, id trace.TraceID) sdktrace.SamplingResult {
	return s.ShouldSample(sdktrace.SamplingParameters{ParentContext: ctx, TraceID: id, Name: "x"})
}

func TestSampler_ARootIsDrawnAtTheRatio(t *testing.T) {
	t.Parallel()

	half := newSampler(Config{SamplerRatio: 0.5}, false)
	assert.Equal(t, sampled, sample(half, context.Background(), alwaysKept).Decision)
	assert.Equal(t, dropped, sample(half, context.Background(), neverKept).Decision)

	all := newSampler(Config{SamplerRatio: 1}, false)
	assert.Equal(t, sampled, sample(all, context.Background(), neverKept).Decision)

	never := newSampler(Config{}, false)
	assert.Equal(t, dropped, sample(never, context.Background(), alwaysKept).Decision, "a negative ratio samples no root")
}

// The decision is made once: whatever this service's ratio, a span with a
// trusted parent takes the parent's flag — and its tracestate.
func TestSampler_AChildFollowsItsParent(t *testing.T) {
	t.Parallel()

	for _, ratio := range []float64{-1, 0.5, 1} {
		for _, remote := range []bool{false, true} {
			s := newSampler(Config{SamplerRatio: ratio}, false)

			kept := sample(s, parentContext(t, neverKept, true, remote, vendorState), neverKept)
			assert.Equal(t, sampled, kept.Decision, "ratio %v remote %v", ratio, remote)
			assert.Equal(t, vendorState, kept.Tracestate.String())

			lost := sample(s, parentContext(t, alwaysKept, false, remote, vendorState), alwaysKept)
			assert.Equal(t, dropped, lost.Decision, "ratio %v remote %v: an unsampled trace is not sampled again", ratio, remote)
			assert.Equal(t, vendorState, lost.Tracestate.String())
		}
	}
}

// On the edge a remote parent is the client's: neither its flag nor its
// trace id decides, and its tracestate does not travel further.
func TestSampler_AnUntrustedRemoteParentIsDrawnAfresh(t *testing.T) {
	t.Parallel()

	never := newSampler(Config{RedrawRemoteParent: true}, false)
	got := sample(never, parentContext(t, alwaysKept, true, true, vendorState), alwaysKept)
	assert.Equal(t, dropped, got.Decision, "the client's sampled flag does not force sampling")
	assert.Zero(t, got.Tracestate.Len(), "the client's tracestate is dropped")

	all := newSampler(Config{SamplerRatio: 1, RedrawRemoteParent: true}, false)
	got = sample(all, parentContext(t, neverKept, false, true, vendorState), neverKept)
	assert.Equal(t, sampled, got.Decision, "nor does its unsampled flag prevent it")
	assert.Zero(t, got.Tracestate.Len())

	// The draw is at the ratio, from an id the client did not choose: an id
	// built to be kept at any ratio is kept about half of the time at 0.5.
	half := newSampler(Config{SamplerRatio: 0.5, RedrawRemoteParent: true}, false)
	crafted := parentContext(t, alwaysKept, true, true, "")

	kept := 0

	const draws = 4000
	for range draws {
		if sample(half, crafted, alwaysKept).Decision == sampled {
			kept++
		}
	}

	assert.InDelta(t, draws/2, kept, draws/10, "kept %d of %d", kept, draws)

	// A local parent — a span of this process — is followed as everywhere.
	local := sample(never, parentContext(t, neverKept, true, false, vendorState), neverKept)
	assert.Equal(t, sampled, local.Decision)
	assert.Equal(t, vendorState, local.Tracestate.String())
}

// A request with the debug token is traced whatever the ratio and the
// parent say, and its trace is marked for the services behind this one.
func TestSampler_AForcedTraceIsSampledAndMarked(t *testing.T) {
	t.Parallel()

	const mark = httpx.DebugTraceStateKey + "=" + httpx.DebugTraceStateValue

	for name, redraw := range map[string]bool{"trusting": false, "edge": true} {
		s := newSampler(Config{RedrawRemoteParent: redraw}, false)

		root := sample(s, httpx.ForceTrace(context.Background()), neverKept)
		assert.Equal(t, sampled, root.Decision, name)
		assert.Equal(t, mark, root.Tracestate.String(), name)

		local := sample(s, httpx.ForceTrace(parentContext(t, neverKept, false, false, vendorState)), neverKept)
		assert.Equal(t, sampled, local.Decision, name)
		assert.Equal(t, mark+","+vendorState, local.Tracestate.String(), "%s: the parent's tracestate is kept", name)

		remote := sample(s, httpx.ForceTrace(parentContext(t, neverKept, false, true, vendorState)), neverKept)
		assert.Equal(t, sampled, remote.Decision, name)

		if !redraw {
			assert.Equal(t, mark+","+vendorState, remote.Tracestate.String())
		} else {
			assert.Equal(t, mark, remote.Tracestate.String(), "the edge keeps nothing of the client's tracestate")
		}
	}

	// The mark alone — a tracestate a client made up — forces nothing on the edge.
	edge := newSampler(Config{RedrawRemoteParent: true}, false)
	spoofed := sample(edge, parentContext(t, alwaysKept, true, true, mark), alwaysKept)
	assert.Equal(t, dropped, spoofed.Decision)
	assert.Zero(t, spoofed.Tracestate.Len())
}

// With the retroactive buffer on, what is not sampled is recorded — and
// still not sampled: the flag that travels downstream does not change.
func TestSampler_RecordsUnsampledSpansForTheBuffer(t *testing.T) {
	t.Parallel()

	s := newSampler(Config{SamplerRatio: 0.5}, true)

	assert.Equal(t, recorded, sample(s, context.Background(), neverKept).Decision, "a root the ratio dropped")
	assert.Equal(t, recorded, sample(s, parentContext(t, alwaysKept, false, true, ""), alwaysKept).Decision, "a child of an unsampled parent")
	assert.Equal(t, sampled, sample(s, context.Background(), alwaysKept).Decision, "a sampled span is sampled as before")
	assert.Equal(t, sampled, sample(s, parentContext(t, neverKept, true, true, ""), neverKept).Decision)

	edge := newSampler(Config{RedrawRemoteParent: true}, true)
	assert.Equal(t, recorded, sample(edge, parentContext(t, alwaysKept, true, true, ""), alwaysKept).Decision)
}

func TestSampler_Description(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ParentBased{root:TraceIDRatioBased{0.5}}",
		newSampler(Config{SamplerRatio: 0.5}, false).Description())
	assert.Equal(t, "ParentBased{root:TraceIDRatioBased{0},remote:redrawn,unsampled:recorded}",
		newSampler(Config{RedrawRemoteParent: true}, true).Description())
}

func TestRandomTraceID(t *testing.T) {
	t.Parallel()

	seen := map[trace.TraceID]bool{}

	var or trace.TraceID

	for range 64 {
		id := randomTraceID()
		require.False(t, seen[id], "ids repeat")
		seen[id] = true

		for i := range id {
			or[i] |= id[i]
		}
	}

	for i, b := range or {
		assert.NotZero(t, b, "byte %d is never set", i)
	}
}
