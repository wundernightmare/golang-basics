package httpx

import (
	"context"
	"log/slog"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"
)

// A debug trace is a request traced and logged in every service it passes
// through, whatever the sampling ratio:
//
//  1. Ingress. A request with a valid [DebugTokenHeader] is marked by
//     [ForceTrace] before its server span starts. The sampler of libs/otelx
//     samples a span started under such a context and puts the mark
//     [DebugTraceStateKey] into its tracestate.
//  2. Propagation. The mark travels with traceparent / tracestate — HTTP
//     headers, gRPC metadata, Kafka headers; the sampled flag makes every
//     service downstream export its spans (they follow the parent).
//  3. Downstream. A server span or a Kafka process span whose own context
//     carries the mark ([IsDebugTrace]) turns [WithDebugLogging] on for that
//     unit of work: its records pass whatever the level and the log sampler.
//
// The mark cannot come from the internet: a service that does not trust
// remote parents (OTEL_TRACES_TRUST_REMOTE_PARENT=false, the edge) drops a
// remote tracestate, so only its own debug token sets it.
const (
	// DebugTraceStateKey is the tracestate key of the mark (a W3C simple key).
	DebugTraceStateKey = "dbg"
	// DebugTraceStateValue is the value of the mark.
	DebugTraceStateValue = "1"
)

type forceTraceKey struct{}

// ForceTrace returns ctx asking the sampler to sample the spans started
// under it and to mark their trace as a debug trace. Call it before the span
// is started: the decision of a span is taken once.
func ForceTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceTraceKey{}, true)
}

// TraceForced reports whether ctx was marked by [ForceTrace]. It is what a
// sampler asks.
func TraceForced(ctx context.Context) bool {
	forced, _ := ctx.Value(forceTraceKey{}).(bool)

	return forced
}

// IsDebugTrace reports whether sc belongs to a debug trace: sampled and
// marked.
func IsDebugTrace(sc trace.SpanContext) bool {
	return sc.IsSampled() && sc.TraceState().Get(DebugTraceStateKey) == DebugTraceStateValue
}

// FollowDebugTrace returns ctx with debug logging on when the span of ctx
// belongs to a debug trace, and ctx itself otherwise. A library that starts
// the span of a unit of work (a server, a consumer) calls it right after.
func FollowDebugTrace(ctx context.Context) context.Context {
	if DebugLogging(ctx) || !IsDebugTrace(trace.SpanContextFromContext(ctx)) {
		return ctx
	}

	return WithDebugLogging(ctx)
}

// TraceTrigger is the trigger side of retroactive sampling
// (libs/otelx/retro): what a logger tells about an unsampled trace, and asks
// of it.
type TraceTrigger interface {
	// TriggerByLog is called when a warn or error line of an unsampled trace
	// is written; it reports whether the trace is exported, now or before.
	TriggerByLog(traceID trace.TraceID) bool
	// IsTriggered reports whether the unsampled trace was triggered.
	IsTriggered(traceID trace.TraceID) bool
}

// traceTriggers is the binding of a logger family to its [TraceTrigger]:
// shared by a logger of [NewLogger] and everything derived from it.
type traceTriggers struct {
	bound atomic.Pointer[TraceTrigger]
}

// exported reports whether the unsampled trace of a record of the given
// level has a trace to link to — and makes it have one when the record is a
// warn or an error.
func (t *traceTriggers) exported(traceID trace.TraceID, level slog.Level) bool {
	bound := t.bound.Load()
	if bound == nil {
		return false
	}

	if level >= slog.LevelWarn {
		return (*bound).TriggerByLog(traceID)
	}

	return (*bound).IsTriggered(traceID)
}

// BindTraceTrigger makes the warn and error lines of log — a logger built by
// [NewLogger], or one derived from it — trigger the export of their
// unsampled trace through trigger (nil unbinds). otelx.Init does it for the
// logger it is given. It reports false for any other logger.
func BindTraceTrigger(log *slog.Logger, trigger TraceTrigger) bool {
	level := LogLevelOf(log)
	if level == nil {
		return false
	}

	if trigger == nil {
		level.triggers.bound.Store(nil)
	} else {
		level.triggers.bound.Store(&trigger)
	}

	return true
}

// triggerTrace asks the trigger bound to log to export the trace of ctx when
// it is unsampled: for a caller that knows the trace matters without having
// a warn line to say so (a debug subject).
func triggerTrace(ctx context.Context, log *slog.Logger) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() || sc.IsSampled() {
		return
	}

	if level := LogLevelOf(log); level != nil {
		if bound := level.triggers.bound.Load(); bound != nil {
			(*bound).TriggerByLog(sc.TraceID())
		}
	}
}
