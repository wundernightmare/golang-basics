// Package retro is retroactive sampling inside one process: the spans of an
// unsampled trace are kept for a while, and a warn / error log line of that
// trace sends them to the trace store after all.
//
// Head sampling decides before the request is processed, so the trace of a
// failure is kept only by luck (the ratio). With the buffer on:
//
//  1. the sampler records an unsampled span instead of dropping it (otelx
//     does that when the buffer is configured);
//  2. [Processor] passes sampled spans on to the exporting processor; an
//     unsampled one is encoded into an arena of byte rings and let go — the
//     oldest spans leave the arena when it is full;
//  3. a log line of level warn or error written with the context of an
//     unsampled span triggers its trace (libs/httpx does that for a logger
//     of its own): the spans of the trace are decoded and exported, and so
//     is every span of it that ends later. The line carries trace_id, as the
//     lines of a sampled trace do.
//
// The budget is [Config]: the memory of the arena, allocated once and never
// exceeded, and the traces triggered a second at most — a storm of errors
// does not become a storm of exports.
//
// Limits. What is not guaranteed is the past: a span that left the arena
// before the trigger is gone, and trace_retro_buffer_horizon_seconds shows
// how far back it reaches. A restart loses the arena.
//
// Across services. The trigger is local: the decision sent downstream stays
// "not sampled", so the trace holds the spans of this process (the lines of
// the other services carry the same id as trace_unsampled_id).
// [Processor.Trigger] is where a bus between the services plugs in:
// it exports a trace another service asked for, and
// [Processor.OnLocalTrigger] tells the bus what this one triggered.
//
// At sample ratio 1 with trusted parents nothing is unsampled and the arena
// stays empty.
package retro

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config is the budget of retroactive sampling. Its keys sit under the
// owner's prefix (otelx: OTEL_RETRO_BUFFER_MIB, OTEL_RETRO_TRIGGERS_PER_SECOND;
// YAML retro.buffer_mib, retro.triggers_per_second).
type Config struct {
	// BufferMiB is the memory of the arena in MiB, allocated at start. Zero
	// is the default (16); a negative value turns retroactive sampling off.
	BufferMiB int `env:"BUFFER_MIB" envDefault:"16" yaml:"buffer_mib"`
	// TriggersPerSecond is the traces triggered a second at most; the
	// triggers above it are refused. Zero is the default (50); under a
	// negative value every trigger is refused, with the buffer still
	// allocated and filled.
	TriggersPerSecond int `env:"TRIGGERS_PER_SECOND" envDefault:"50" yaml:"triggers_per_second"`
}

// Defaults of a [Config] built by hand.
const (
	DefaultBufferMiB         = 16
	DefaultTriggersPerSecond = 50
)

// Enabled reports whether the configuration asks for a buffer.
func (c Config) Enabled() bool { return c.BufferMiB >= 0 }

func (c Config) withDefaults() Config {
	if c.BufferMiB == 0 {
		c.BufferMiB = DefaultBufferMiB
	}

	if c.TriggersPerSecond == 0 {
		c.TriggersPerSecond = DefaultTriggersPerSecond
	}

	return c
}

// Processor is a span processor in front of the exporting one: sampled spans
// pass through, unsampled ones wait in the arena for a trigger.
type Processor struct {
	inner sdktrace.SpanProcessor
	arena *arena
	codec *codec
	now   func() time.Time

	// resource is the one of the provider, taken from the first span seen:
	// the bytes do not repeat it.
	resource atomic.Pointer[resource.Resource]
	observer atomic.Pointer[func(trace.TraceID)]
	// scratch holds the buffers spans are encoded into: reused, so encoding
	// allocates nothing once a buffer has seen its largest span.
	scratch sync.Pool

	// Counted where they happen, which is on a trigger or a failure — not
	// per span.
	exported, undecodable, unencodable atomic.Uint64
}

var _ sdktrace.SpanProcessor = (*Processor)(nil)

// New wraps inner — the batch processor in front of the exporter — with the
// buffer cfg describes. The caller checks [Config.Enabled] first.
func New(inner sdktrace.SpanProcessor, cfg Config) *Processor {
	cfg = cfg.withDefaults()

	return newProcessor(inner, newArena(max(cfg.BufferMiB, 0)<<20, cfg.TriggersPerSecond, time.Now()), time.Now)
}

func newProcessor(inner sdktrace.SpanProcessor, a *arena, now func() time.Time) *Processor {
	return &Processor{
		inner: inner, arena: a, codec: newCodec(), now: now,
		scratch: sync.Pool{New: func() any { return new([]byte) }},
	}
}

// sampledSpan is a span exported after all: an exporter may skip spans
// without the flag, and the batch processor does.
type sampledSpan struct{ sdktrace.ReadOnlySpan }

func (s sampledSpan) SpanContext() trace.SpanContext {
	sc := s.ReadOnlySpan.SpanContext()

	return sc.WithTraceFlags(sc.TraceFlags() | trace.FlagsSampled)
}

// OnStart implements sdktrace.SpanProcessor.
func (p *Processor) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	p.inner.OnStart(parent, span)
}

// OnEnd implements sdktrace.SpanProcessor: a sampled span passes, an
// unsampled one is kept, or exported when its trace is triggered.
func (p *Processor) OnEnd(span sdktrace.ReadOnlySpan) {
	sc := span.SpanContext()
	if sc.IsSampled() {
		p.inner.OnEnd(span)

		return
	}

	traceID := sc.TraceID()

	if p.resource.Load() == nil {
		p.resource.CompareAndSwap(nil, span.Resource())
	}

	scratch := p.scratch.Get().(*[]byte) //nolint:forcetypeassert // the pool only ever holds *[]byte

	encoded, ok := p.codec.encode((*scratch)[:0], span)
	*scratch = encoded

	// One lock of one shard decides: the arena keeps the span, or says its
	// trace is triggered and keeps nothing. (Asking first would skip the
	// encoding of a triggered trace's span at the price of a second lock for
	// every other span, once anything was ever triggered.)
	triggered := false
	if ok {
		triggered = p.arena.offer(traceID, span.EndTime().UnixNano(), encoded)
	} else {
		p.unencodable.Add(1)
	}

	p.scratch.Put(scratch)

	// Exported with no lock held and no buffer borrowed: the processor
	// behind may log, and its warn line may come back here as a trigger.
	if triggered {
		p.export(span)
	}
}

func (p *Processor) export(span sdktrace.ReadOnlySpan) {
	p.exported.Add(1)
	p.inner.OnEnd(sampledSpan{span})
}

// Shutdown implements sdktrace.SpanProcessor.
func (p *Processor) Shutdown(ctx context.Context) error { return p.inner.Shutdown(ctx) }

// ForceFlush implements sdktrace.SpanProcessor.
func (p *Processor) ForceFlush(ctx context.Context) error { return p.inner.ForceFlush(ctx) }

// Trigger exports the unsampled trace traceID: the spans of it the arena
// kept, and every span of it that ends later. This is the entry for a
// trigger that comes from outside the process; it is not reported to
// [Processor.OnLocalTrigger].
func (p *Processor) Trigger(traceID trace.TraceID) Outcome {
	return p.trigger(traceID, false)
}

// TriggerByLog is the local trigger: a warn / error line of an unsampled
// trace was written. It reports whether the trace is exported, now or
// before — whether the line may carry trace_id.
func (p *Processor) TriggerByLog(traceID trace.TraceID) bool {
	return p.trigger(traceID, true) != RateLimited
}

// IsTriggered reports whether the unsampled trace was triggered: its lines
// link to a trace.
func (p *Processor) IsTriggered(traceID trace.TraceID) bool {
	return p.arena.isTriggered(traceID)
}

// OnLocalTrigger calls observer with the id of every trace this process
// triggers by itself (a warn / error line) and that the rate limit accepted
// — what a bus would tell the other services. One observer per process:
// false when there is one already. It runs on the goroutine that wrote the
// log line and must not block.
func (p *Processor) OnLocalTrigger(observer func(trace.TraceID)) bool {
	return p.observer.CompareAndSwap(nil, &observer)
}

func (p *Processor) trigger(traceID trace.TraceID, local bool) Outcome {
	outcome, encoded := p.arena.trigger(traceID, p.now())

	res := p.resource.Load()

	for _, raw := range encoded {
		stub, ok := p.codec.decode(raw)
		if !ok {
			p.undecodable.Add(1)

			continue
		}

		stub.Resource = res
		p.export(stub.Snapshot())
	}

	if local && outcome == Accepted {
		if observer := p.observer.Load(); observer != nil {
			(*observer)(traceID)
		}
	}

	return outcome
}

// Capacity is the bytes the arena was allocated with: the configured size,
// whichever way it was arrived at.
func (p *Processor) Capacity() int { return p.arena.allocated() }

// Collectors returns the Prometheus collectors of the buffer (the
// trace_retro_* families of docs/METRICS.md) for the service's registry.
func (p *Processor) Collectors() []prometheus.Collector { return []prometheus.Collector{collector{p}} }

type collector struct{ p *Processor }

//nolint:gochecknoglobals // metric descriptors: immutable, built once
var (
	descBytes = prometheus.NewDesc("trace_retro_buffer_bytes",
		"Bytes of the retroactive sampling buffer in use.", nil, nil)
	descCapacity = prometheus.NewDesc("trace_retro_buffer_capacity_bytes",
		"Bytes the retroactive sampling buffer was allocated with: its limit.", nil, nil)
	descSpans = prometheus.NewDesc("trace_retro_buffer_spans",
		"Unsampled spans kept for retroactive sampling.", nil, nil)
	descHorizon = prometheus.NewDesc("trace_retro_buffer_horizon_seconds",
		"Time between the oldest and the newest span kept: how far back a trigger reaches.", nil, nil)
	descSpansTotal = prometheus.NewDesc("trace_retro_spans_total",
		"Unsampled spans by what became of them: kept in the buffer, exported by a trigger, evicted by newer ones, or lost to the codec (unencodable, undecodable).",
		[]string{"outcome"}, nil)
	descTriggers = prometheus.NewDesc("trace_retro_triggers_total",
		"Triggers of retroactive sampling: accepted, repeated for a trace already triggered, or refused by the rate limit.",
		[]string{"outcome"}, nil)
)

func (collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{descBytes, descCapacity, descSpans, descHorizon, descSpansTotal, descTriggers} {
		ch <- desc
	}
}

func (c collector) Collect(ch chan<- prometheus.Metric) {
	use := c.p.arena.usage()

	gauge := func(desc *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v)
	}
	counter := func(desc *prometheus.Desc, v uint64, outcome string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(v), outcome)
	}

	gauge(descBytes, float64(use.bytes))
	gauge(descCapacity, float64(c.p.arena.allocated()))
	gauge(descSpans, float64(use.spans))
	gauge(descHorizon, use.horizon.Seconds())

	counter(descSpansTotal, use.kept, "kept")
	counter(descSpansTotal, c.p.exported.Load(), "exported")
	counter(descSpansTotal, use.evicted, "evicted")
	counter(descSpansTotal, c.p.unencodable.Load(), "unencodable")
	counter(descSpansTotal, c.p.undecodable.Load(), "undecodable")

	counter(descTriggers, use.accepted, Accepted.String())
	counter(descTriggers, use.repeated, Repeated.String())
	counter(descTriggers, use.refused, RateLimited.String())
}
