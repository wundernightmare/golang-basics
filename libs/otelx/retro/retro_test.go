package retro

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// sink is the exporting processor behind the buffer: it keeps what it is
// given.
type sink struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
	count atomic.Int64

	started, flushed, stopped atomic.Int64
}

func (s *sink) OnStart(context.Context, sdktrace.ReadWriteSpan) { s.started.Add(1) }
func (s *sink) Shutdown(context.Context) error                  { s.stopped.Add(1); return nil }
func (s *sink) ForceFlush(context.Context) error                { s.flushed.Add(1); return nil }

func (s *sink) OnEnd(span sdktrace.ReadOnlySpan) {
	s.count.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.spans = append(s.spans, span)
}

func (s *sink) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := []string{}
	for _, span := range s.spans {
		out = append(out, span.Name())
	}

	return out
}

var testResource = resource.NewSchemaless(attribute.String("service.name", "retro-test"))

// spanOf is an ended span of trace tr, sampled or not.
func spanOf(tr int, id byte, name string, isSampled bool) sdktrace.ReadOnlySpan {
	flags := trace.TraceFlags(0)
	if isSampled {
		flags = trace.FlagsSampled
	}

	end := time.Unix(1_700_000_000+int64(id), 0)

	return tracetest.SpanStub{
		Name: name,
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceOf(tr), SpanID: trace.SpanID{7: id}, TraceFlags: flags,
		}),
		StartTime:            end.Add(-time.Millisecond),
		EndTime:              end,
		Attributes:           []attribute.KeyValue{attribute.String("http.request.method", "GET"), attribute.Int("n", int(id))},
		Resource:             testResource,
		InstrumentationScope: instrumentation.Scope{Name: "retro-test"},
	}.Snapshot()
}

func processor(size, rate int) (*Processor, *sink) {
	out := &sink{}

	return newProcessor(out, newArena(size, rate, time.Now()), time.Now), out
}

const mib = 1 << 20

func TestConfig(t *testing.T) {
	t.Parallel()

	assert.True(t, Config{}.Enabled(), "the zero value is the default budget")
	assert.Equal(t, Config{BufferMiB: DefaultBufferMiB, TriggersPerSecond: DefaultTriggersPerSecond}, Config{}.withDefaults())
	assert.Equal(t, Config{BufferMiB: 64, TriggersPerSecond: 5}, Config{BufferMiB: 64, TriggersPerSecond: 5}.withDefaults())
	assert.False(t, Config{BufferMiB: -1}.Enabled())

	proc := New(&sink{}, Config{BufferMiB: 1, TriggersPerSecond: 3})
	assert.Equal(t, mib, proc.arena.allocated())
	assert.Equal(t, uint64(3), proc.arena.perSecond)

	assert.Equal(t, DefaultBufferMiB*mib, New(&sink{}, Config{}).arena.allocated())
}

func TestProcessor_SampledSpansPassAndUnsampledOnesWait(t *testing.T) {
	t.Parallel()

	proc, out := processor(mib, 50)

	proc.OnEnd(spanOf(1, 1, "sampled", true))
	proc.OnEnd(spanOf(2, 2, "unsampled", false))

	assert.Equal(t, []string{"sampled"}, out.names())
	assert.Equal(t, 1, proc.arena.usage().spans)

	proc.OnStart(context.Background(), nil)
	require.NoError(t, proc.ForceFlush(context.Background()))
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, []int64{1, 1, 1}, []int64{out.started.Load(), out.flushed.Load(), out.stopped.Load()},
		"the rest of the processor's life goes to the one behind it")
}

// The entry a bus between the services will use: another service asks for a
// trace, and this one exports what it kept of it and what ends later —
// without a log line of its own.
func TestProcessor_ATriggerFromOutsideExportsTheTraceAndIsNotEchoed(t *testing.T) {
	t.Parallel()

	proc, out := processor(mib, 50)

	var echoed []trace.TraceID

	require.True(t, proc.OnLocalTrigger(func(id trace.TraceID) { echoed = append(echoed, id) }))

	proc.OnEnd(spanOf(7, 1, "http.request", false))
	proc.OnEnd(spanOf(8, 2, "another request", false))
	assert.False(t, proc.IsTriggered(traceOf(7)))

	assert.Equal(t, Accepted, proc.Trigger(traceOf(7)))
	assert.Equal(t, []string{"http.request"}, out.names())
	assert.True(t, proc.IsTriggered(traceOf(7)))

	// A span of the trace that ends after the trigger.
	proc.OnEnd(spanOf(7, 3, "handler", false))
	assert.Equal(t, []string{"http.request", "handler"}, out.names())
	assert.Equal(t, Repeated, proc.Trigger(traceOf(7)))
	assert.Empty(t, echoed, "what came from outside is not sent back to the bus")
	assert.Equal(t, 1, proc.arena.usage().spans, "the other trace still waits")
}

func TestProcessor_TheObserverHearsOfLocalTriggersTheRateLimitAccepted(t *testing.T) {
	t.Parallel()

	now := time.Now()
	out := &sink{}
	proc := newProcessor(out, newArena(mib, 2, now), func() time.Time { return now })

	var heard []trace.TraceID

	require.True(t, proc.OnLocalTrigger(func(id trace.TraceID) { heard = append(heard, id) }))
	assert.False(t, proc.OnLocalTrigger(func(trace.TraceID) {}), "one observer for a process")

	assert.True(t, proc.TriggerByLog(traceOf(1)))
	assert.True(t, proc.TriggerByLog(traceOf(1)), "triggered before: still exported")
	assert.True(t, proc.TriggerByLog(traceOf(2)))
	assert.False(t, proc.TriggerByLog(traceOf(3)), "over the rate: the trace stays unsampled")
	assert.Equal(t, []trace.TraceID{traceOf(1), traceOf(2)}, heard, "once per trace, and not for the one refused")

	now = now.Add(time.Second)

	assert.True(t, proc.TriggerByLog(traceOf(3)), "the next second has its own budget")
}

// What comes out of the arena is the span that went in — every field, its
// scope, the resource of the process — with the sampled flag set: an
// exporter may skip a span without it, and the batch processor does.
func TestProcessor_AnExportedSpanIsTheOneKeptButSampled(t *testing.T) {
	t.Parallel()

	proc, out := processor(mib, 50)

	state, err := trace.ParseTraceState("vendor=x")
	require.NoError(t, err)

	kept := full()
	kept.Resource = testResource
	kept.SpanContext = trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceOf(9), SpanID: trace.SpanID{4}, Remote: true, TraceState: state,
	})

	other := spanOf(9, 5, "of another tracer", false)

	proc.OnEnd(kept.Snapshot())
	proc.OnEnd(other)
	require.Equal(t, Accepted, proc.Trigger(traceOf(9)))
	proc.OnEnd(spanOf(9, 6, "ends later", false))

	require.Len(t, out.spans, 3)

	for i, span := range out.spans {
		sc := span.SpanContext()
		assert.True(t, sc.IsSampled(), "span %d", i)
		assert.Equal(t, traceOf(9), sc.TraceID())
		assert.Same(t, testResource, span.Resource(), "span %d carries the resource of the process", i)
	}

	got := tracetest.SpanStubFromReadOnlySpan(out.spans[0])
	assert.True(t, got.SpanContext.IsRemote())
	assert.Equal(t, "vendor=x", got.SpanContext.TraceState().String())
	assert.Equal(t, trace.SpanID{4}, got.SpanContext.SpanID())

	got.SpanContext = kept.SpanContext
	requireSameSpan(t, tracetest.SpanStubFromReadOnlySpan(kept.Snapshot()), got)

	assert.Equal(t, "retro-test", out.spans[1].InstrumentationScope().Name, "each span comes back with its own scope")
	assert.Equal(t, "libs/httpx", out.spans[0].InstrumentationScope().Name)
}

func TestProcessor_ARingTooSmallForThePastStillExportsWhatEndsLater(t *testing.T) {
	t.Parallel()

	proc, out := processor(1, 50) // nothing fits

	proc.OnEnd(spanOf(1, 1, "decode", false))
	assert.Zero(t, proc.arena.usage().spans)

	require.True(t, proc.TriggerByLog(traceOf(1)))
	proc.OnEnd(spanOf(1, 2, "store", false))
	proc.OnEnd(spanOf(1, 3, "kafka.consume", false))

	assert.Equal(t, []string{"store", "kafka.consume"}, out.names())
}

// A span the codec cannot write (more scopes than its table) is counted and
// lost; nothing else is.
func TestProcessor_ASpanThatCannotBeEncodedIsCounted(t *testing.T) {
	t.Parallel()

	proc, out := processor(mib, 50)

	for i := range scopesMax + 3 {
		stub := tracetest.SpanStubFromReadOnlySpan(spanOf(1, byte(i), "x", false))
		stub.InstrumentationScope = instrumentation.Scope{Name: "scope", Version: strings.Repeat("v", i)}
		proc.OnEnd(stub.Snapshot())
	}

	assert.Equal(t, uint64(3), proc.unencodable.Load())
	assert.Equal(t, scopesMax, proc.arena.usage().spans)

	require.Equal(t, Accepted, proc.Trigger(traceOf(1)))
	assert.Len(t, out.spans, scopesMax)
}

// Bytes in the arena that are not a span (they cannot be, short of a bug)
// are counted and skipped: the rest of the trace is exported.
func TestProcessor_UndecodableBytesAreCountedAndSkipped(t *testing.T) {
	t.Parallel()

	proc, out := processor(mib, 50)

	proc.OnEnd(spanOf(1, 1, "first", false))
	require.False(t, proc.arena.offer(traceOf(1), 0, []byte{0xff, 0xff}))
	proc.OnEnd(spanOf(1, 2, "last", false))

	require.Equal(t, Accepted, proc.Trigger(traceOf(1)))
	assert.Equal(t, []string{"first", "last"}, out.names())
	assert.Equal(t, uint64(1), proc.undecodable.Load())
	assert.Equal(t, uint64(2), proc.exported.Load())
}

func TestProcessor_Metrics(t *testing.T) {
	t.Parallel()

	now := time.Now()
	out := &sink{}
	proc := newProcessor(out, newArenaWithShards(3*200, 1, 1, now), func() time.Time { return now })

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(proc.Collectors()...)

	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	assert.Empty(t, problems)

	for i := range byte(5) {
		proc.OnEnd(spanOf(1, i, "span", false)) // each record is a bit over 100 bytes: the ring holds five, then evicts
	}

	proc.OnEnd(spanOf(1, 9, "span", false))
	proc.OnEnd(spanOf(2, 10, "span", false))
	require.True(t, proc.TriggerByLog(traceOf(1)))
	require.True(t, proc.TriggerByLog(traceOf(1)))
	require.False(t, proc.TriggerByLog(traceOf(2)))
	proc.OnEnd(spanOf(1, 11, "span", false))

	use := proc.arena.usage()
	require.Positive(t, use.evicted, "the ring is small enough to evict")

	value := func(name string, labels ...string) float64 {
		t.Helper()

		families, err := reg.Gather()
		require.NoError(t, err)

		for _, family := range families {
			if family.GetName() != name {
				continue
			}

			for _, metric := range family.GetMetric() {
				if len(labels) == 0 || metric.GetLabel()[0].GetValue() == labels[0] {
					if metric.GetGauge() != nil {
						return metric.GetGauge().GetValue()
					}

					return metric.GetCounter().GetValue()
				}
			}
		}

		t.Fatalf("no %s %v", name, labels)

		return 0
	}

	assert.InDelta(t, 600, value("trace_retro_buffer_capacity_bytes"), 0)
	assert.InDelta(t, float64(use.bytes), value("trace_retro_buffer_bytes"), 0)
	assert.InDelta(t, 1, value("trace_retro_buffer_spans"), 0, "the span of the trace that was refused")
	assert.InDelta(t, 0, value("trace_retro_buffer_horizon_seconds"), 0)
	assert.InDelta(t, 7, value("trace_retro_spans_total", "kept"), 0)
	assert.InDelta(t, float64(use.evicted), value("trace_retro_spans_total", "evicted"), 0)
	assert.InDelta(t, float64(use.taken)+1, value("trace_retro_spans_total", "exported"), 0, "taken from the ring, and the one that ended later")
	assert.InDelta(t, 0, value("trace_retro_spans_total", "unencodable"), 0)
	assert.InDelta(t, 0, value("trace_retro_spans_total", "undecodable"), 0)
	assert.InDelta(t, 1, value("trace_retro_triggers_total", "accepted"), 0)
	assert.InDelta(t, 1, value("trace_retro_triggers_total", "repeated"), 0)
	assert.InDelta(t, 1, value("trace_retro_triggers_total", "rate_limited"), 0)
	assert.InDelta(t, 7, float64(use.evicted+use.taken)+float64(use.spans), 0, "every span kept is still there, was evicted, or was taken")
}

// Under concurrent offers and triggers no span is lost or exported twice:
// every unsampled span is exported, evicted, or still in the arena.
func TestProcessor_SpansAreConservedUnderConcurrency(t *testing.T) {
	t.Parallel()

	const (
		workers = 8
		traces  = 64
		perWork = 3000
	)

	out := &sink{}
	proc := newProcessor(out, newArena(mib/2, 1<<20, time.Now()), time.Now)

	var group sync.WaitGroup

	for w := range workers {
		group.Go(func() {
			for i := range perWork {
				tr := (w*31 + i) % traces
				proc.OnEnd(spanOf(tr, byte(i), "span", false))

				if i%97 == 0 {
					proc.TriggerByLog(traceOf((tr + 13) % traces))
				}

				if i%101 == 0 {
					proc.Trigger(traceOf((tr + 29) % traces))
				}
			}
		})
	}

	group.Wait()

	use := proc.arena.usage()
	total := uint64(workers * perWork)

	assert.Equal(t, uint64(out.count.Load()), proc.exported.Load(), "every export reached the processor behind")
	assert.Equal(t, use.kept, use.evicted+use.taken+uint64(use.spans), "kept = evicted + taken + still there")
	assert.Equal(t, total, use.kept+(proc.exported.Load()-use.taken), "offered = kept + exported at their end")
	assert.Zero(t, proc.undecodable.Load())
	assert.Zero(t, proc.unencodable.Load())
	assert.LessOrEqual(t, use.bytes, mib/2)

	for _, span := range out.spans {
		require.True(t, span.SpanContext().IsSampled())
	}
}

// Keeping a span allocates nothing once the buffers are warm, and the arena
// does not grow: the memory of retroactive sampling is its budget.
//
//nolint:paralleltest // serial: counts the allocations and the heap of the process
func TestProcessor_KeepingASpanAllocatesNothing(t *testing.T) {
	if testing.Short() && raceEnabled {
		t.Skip("the race detector allocates")
	}

	proc, _ := processor(mib, 50)
	span := spanOf(1, 1, "GET /tasks/{id}", false)

	for range 100 {
		proc.OnEnd(span)
	}

	allocs := testing.AllocsPerRun(2000, func() { proc.OnEnd(span) })
	if !raceEnabled {
		assert.Zero(t, allocs)
	}

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	for i := range 200_000 {
		proc.OnEnd(spanOf(i%500, byte(i), "GET /tasks/{id}", false))
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	assert.Equal(t, mib, proc.arena.allocated())
	assert.Less(t, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(mib/4),
		"several budgets of spans went through and the heap did not keep them")
	assert.Positive(t, proc.arena.usage().evicted)
}

func BenchmarkKeep(b *testing.B) {
	proc, _ := processor(16*mib, 50)
	spans := make([]sdktrace.ReadOnlySpan, 1024)

	for i := range spans {
		stub := full()
		stub.Attributes = stub.Attributes[:8]
		stub.SpanContext = trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceOf(i), SpanID: trace.SpanID{1}})
		spans[i] = stub.Snapshot()
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			proc.OnEnd(spans[i%len(spans)])
			i++
		}
	})
}

func BenchmarkTriggerCheckWhileNothingIsTriggered(b *testing.B) {
	proc, _ := processor(mib, 50)
	id := traceOf(1)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if proc.IsTriggered(id) {
				b.Fatal("triggered")
			}
		}
	})
}

// relogger is a processor behind the buffer that does what a real one may:
// it logs a warning while it is handed a span — which comes back to the
// buffer as a trigger — and ends a span of its own.
type relogger struct {
	sink

	buffer *Processor
	depth  atomic.Int64
}

func (r *relogger) OnEnd(span sdktrace.ReadOnlySpan) {
	r.sink.OnEnd(span)

	if r.depth.Add(1) == 1 {
		r.buffer.TriggerByLog(span.SpanContext().TraceID())
		r.buffer.TriggerByLog(traceOf(77))
		r.buffer.OnEnd(spanOf(77, 9, "the exporter's own span", false))
		r.buffer.OnEnd(spanOf(78, 9, "kept while exporting", false))
	}

	r.depth.Add(-1)
}

// The processor behind is called with no lock held: it may trigger and end
// spans from inside OnEnd without waiting for itself.
func TestProcessor_TheProcessorBehindMayComeBackIn(t *testing.T) {
	t.Parallel()

	out := &relogger{}
	proc := newProcessor(out, newArenaWithShards(mib, 1, 50, time.Now()), time.Now) // one shard: every trace meets at one lock
	out.buffer = proc

	proc.OnEnd(spanOf(1, 1, "first", false))
	proc.OnEnd(spanOf(77, 1, "of the exporter's trace", false))

	done := make(chan Outcome, 1)
	go func() { done <- proc.Trigger(traceOf(1)) }()

	select {
	case outcome := <-done:
		assert.Equal(t, Accepted, outcome)
	case <-time.After(10 * time.Second):
		t.Fatal("the trigger waits for a lock it holds")
	}

	assert.ElementsMatch(t, []string{"first", "of the exporter's trace", "the exporter's own span"}, out.names())
	assert.Equal(t, 1, proc.arena.usage().spans, "the span kept during the export is in the arena")
}
