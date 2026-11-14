package retro

import (
	"encoding/binary"
	"maps"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// A span as bytes, for the arena: everything a ReadOnlySpan holds but its
// resource (one for the process), written once into a buffer the caller owns
// and read back when the trace is triggered.
//
// The format is private to the process and to the build — the bytes never
// leave the arena and do not outlive it — so it has no compatibility to
// keep: counts and lengths are varints, fixed-width integers little-endian,
// strings UTF-8 with a length in front.
//
// Two things a service repeats in every span are written once:
//
//   - the instrumentation scope (a tracer per library): its number in a
//     table of the scopes seen;
//   - strings that come from the code — attribute keys, span and event
//     names, the values of the closed sets (http.request.method,
//     messaging.*, db.*, …): their number in the dictionary instead of the
//     text. Values that come from data (ids, addresses, paths) stay inline:
//     they would fill the dictionary and repeat nothing.
const (
	// dictionaryMax is the most strings the dictionary holds. They are never
	// removed — a record in the arena may name any of them — so the count
	// bounds its memory.
	dictionaryMax = 4096
	// internedLenMax: a longer string is written inline.
	internedLenMax = 128
	// scopesMax is the most instrumentation scopes remembered; a span of a
	// scope beyond it is not kept.
	scopesMax = 256
	// depthMax bounds the nesting of slice and map values.
	depthMax = 8
)

// table numbers the distinct values of K it is shown, in the order it first
// sees them, and never forgets one. Looking a known value up takes no lock:
// readers load an immutable snapshot, a new value replaces the snapshot.
type table[K comparable] struct {
	mu    sync.Mutex
	state atomic.Pointer[tableState[K]]
}

type tableState[K comparable] struct {
	ids    map[K]uint32
	values []K
}

func newTable[K comparable]() *table[K] {
	t := &table[K]{}
	t.state.Store(&tableState[K]{ids: map[K]uint32{}})

	return t
}

// intern returns the number of value, giving it one if fewer than room
// values have one; false = the value has no number.
func (t *table[K]) intern(value K, room int) (uint32, bool) {
	if id, ok := t.state.Load().ids[value]; ok {
		return id, true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.state.Load()
	if id, ok := state.ids[value]; ok {
		return id, true
	}

	if len(state.values) >= room {
		return 0, false
	}

	id := uint32(len(state.values)) //nolint:gosec // G115: below room
	next := &tableState[K]{ids: maps.Clone(state.ids), values: append(state.values, value)}
	next.ids[value] = id
	t.state.Store(next)

	return id, true
}

func (t *table[K]) resolve(id uint64) (K, bool) {
	values := t.state.Load().values
	if id >= uint64(len(values)) {
		var zero K

		return zero, false
	}

	return values[id], true
}

func (t *table[K]) len() int { return len(t.state.Load().values) }

// codec holds what the encoded spans name by number.
type codec struct {
	strings *table[string]
	scopes  *table[instrumentation.Scope]
}

func newCodec() *codec {
	return &codec{strings: newTable[string](), scopes: newTable[instrumentation.Scope]()}
}

// hasStaticValue reports whether the string value of key comes from a closed
// set — the code or the deployment — rather than from data.
func hasStaticValue(key attribute.Key) bool {
	k := string(key)

	switch k {
	case "http.request.method", "http.route", "url.scheme", "error.type", "server.address", "peer.service":
		return true
	}

	for _, prefix := range [...]string{"messaging.", "db.", "rpc.", "network.", "code.", "otel."} {
		if strings.HasPrefix(k, prefix) {
			// A statement or a message id is data.
			return k != "db.query.text" && k != "messaging.message.id"
		}
	}

	return false
}

// ── Writing ────────────────────────────────────────────────────────────────

func putUvarint(dst []byte, v int) []byte {
	return binary.AppendUvarint(dst, uint64(max(v, 0)))
}

func putInt64(dst []byte, v int64) []byte {
	return binary.LittleEndian.AppendUint64(dst, uint64(v)) //nolint:gosec // G115: the bit pattern is read back as int64
}

// putString writes text inline: twice its length, then the bytes. The low
// bit tells it from a number of the dictionary.
func putString(dst []byte, text string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(text))<<1)

	return append(dst, text...)
}

// putInterned writes text that repeats: its number in the dictionary, or the
// text inline when it has none and fewer than room strings have one.
func (c *codec) putInterned(dst []byte, text string, room int) []byte {
	if text != "" && len(text) <= internedLenMax {
		if id, ok := c.strings.intern(text, room); ok {
			return binary.AppendUvarint(dst, uint64(id)<<1|1)
		}
	}

	return putString(dst, text)
}

// putRepeating is putInterned for text the code may format data into (a span
// name with a path in it): it gets a number only while the dictionary is
// less than half full, so that it cannot crowd out the keys.
func (c *codec) putRepeating(dst []byte, text string) []byte {
	return c.putInterned(dst, text, dictionaryMax/2)
}

func putTime(dst []byte, at time.Time) []byte {
	if at.IsZero() {
		return putInt64(dst, math.MinInt64)
	}

	return putInt64(dst, at.UnixNano())
}

func putContext(dst []byte, sc trace.SpanContext) []byte {
	traceID, spanID := sc.TraceID(), sc.SpanID()
	dst = append(dst, traceID[:]...)
	dst = append(dst, spanID[:]...)
	dst = append(dst, byte(sc.TraceFlags()))

	if sc.IsRemote() {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}

	// Nearly every span has no tracestate, and String builds nothing then.
	return putString(dst, sc.TraceState().String())
}

func (c *codec) putValue(dst []byte, value attribute.Value, repeats bool, depth int) ([]byte, bool) {
	kind := value.Type()
	dst = append(dst, byte(kind)) //nolint:gosec // G115: a dozen kinds

	switch kind {
	case attribute.EMPTY:
	case attribute.BOOL:
		if value.AsBool() {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
	case attribute.INT64:
		dst = putInt64(dst, value.AsInt64())
	case attribute.FLOAT64:
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value.AsFloat64()))
	case attribute.STRING:
		if repeats {
			dst = c.putRepeating(dst, value.AsString())
		} else {
			dst = putString(dst, value.AsString())
		}
	case attribute.BOOLSLICE:
		items := value.AsBoolSlice()
		dst = putUvarint(dst, len(items))

		for _, item := range items {
			if item {
				dst = append(dst, 1)
			} else {
				dst = append(dst, 0)
			}
		}
	case attribute.INT64SLICE:
		items := value.AsInt64Slice()
		dst = putUvarint(dst, len(items))

		for _, item := range items {
			dst = putInt64(dst, item)
		}
	case attribute.FLOAT64SLICE:
		items := value.AsFloat64Slice()
		dst = putUvarint(dst, len(items))

		for _, item := range items {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(item))
		}
	case attribute.STRINGSLICE:
		items := value.AsStringSlice()
		dst = putUvarint(dst, len(items))

		for _, item := range items {
			dst = putString(dst, item)
		}
	case attribute.BYTESLICE:
		raw := value.AsByteSlice()
		dst = putUvarint(dst, len(raw))
		dst = append(dst, raw...)
	case attribute.SLICE:
		if depth >= depthMax {
			return dst, false
		}

		items := value.AsSlice()
		dst = putUvarint(dst, len(items))

		for _, item := range items {
			var ok bool
			if dst, ok = c.putValue(dst, item, false, depth+1); !ok {
				return dst, false
			}
		}
	case attribute.MAP:
		if depth >= depthMax {
			return dst, false
		}

		return c.putAttributes(dst, value.AsMap(), depth+1)
	default:
		// A kind this build does not know: the span is not kept rather than
		// kept without it.
		return dst, false
	}

	return dst, true
}

func (c *codec) putAttributes(dst []byte, attrs []attribute.KeyValue, depth int) ([]byte, bool) {
	dst = putUvarint(dst, len(attrs))

	for _, attr := range attrs {
		dst = c.putInterned(dst, string(attr.Key), dictionaryMax)

		var ok bool
		if dst, ok = c.putValue(dst, attr.Value, hasStaticValue(attr.Key), depth); !ok {
			return dst, false
		}
	}

	return dst, true
}

// encode appends span to dst; false when the span cannot be written (a
// value of an unknown kind, more scopes than the table holds).
func (c *codec) encode(dst []byte, span sdktrace.ReadOnlySpan) ([]byte, bool) {
	scope, ok := c.scopes.intern(span.InstrumentationScope(), scopesMax)
	if !ok {
		return dst, false
	}

	dst = binary.AppendUvarint(dst, uint64(scope))
	dst = c.putRepeating(dst, span.Name())
	dst = putContext(dst, span.SpanContext())
	dst = putContext(dst, span.Parent())
	dst = putUvarint(dst, int(span.SpanKind()))
	dst = putTime(dst, span.StartTime())
	dst = putTime(dst, span.EndTime())

	if dst, ok = c.putAttributes(dst, span.Attributes(), 0); !ok {
		return dst, false
	}

	events := span.Events()
	dst = putUvarint(dst, len(events))

	for i := range events {
		dst = c.putRepeating(dst, events[i].Name)
		dst = putTime(dst, events[i].Time)

		if dst, ok = c.putAttributes(dst, events[i].Attributes, 0); !ok {
			return dst, false
		}

		dst = putUvarint(dst, events[i].DroppedAttributeCount)
	}

	links := span.Links()
	dst = putUvarint(dst, len(links))

	for i := range links {
		dst = putContext(dst, links[i].SpanContext)

		if dst, ok = c.putAttributes(dst, links[i].Attributes, 0); !ok {
			return dst, false
		}

		dst = putUvarint(dst, links[i].DroppedAttributeCount)
	}

	status := span.Status()
	dst = putUvarint(dst, int(status.Code))
	dst = putString(dst, status.Description)
	dst = putUvarint(dst, span.DroppedAttributes())
	dst = putUvarint(dst, span.DroppedLinks())
	dst = putUvarint(dst, span.DroppedEvents())
	dst = putUvarint(dst, span.ChildSpanCount())

	return dst, true
}

// ── Reading ────────────────────────────────────────────────────────────────

// reader walks the bytes of one record. A read past the end, or of something
// that cannot be, sets bad and returns zero values from then on: the caller
// checks once, at the end.
type reader struct {
	rest []byte
	bad  bool
	c    *codec
}

func (r *reader) take(n int) []byte {
	if r.bad || n < 0 || n > len(r.rest) {
		r.bad = true

		return nil
	}

	out := r.rest[:n]
	r.rest = r.rest[n:]

	return out
}

func (r *reader) uvarint() uint64 {
	if r.bad {
		return 0
	}

	v, n := binary.Uvarint(r.rest)
	if n <= 0 {
		r.bad = true

		return 0
	}

	r.rest = r.rest[n:]

	return v
}

// count reads a number of things that each take at least one byte: more of
// them than bytes left is a corrupt record, not an allocation to make.
func (r *reader) count() int {
	v := r.uvarint()
	if v > uint64(len(r.rest)) {
		r.bad = true

		return 0
	}

	return int(v) //nolint:gosec // G115: bounded by the bytes left
}

// number reads a small non-negative integer written by putUvarint.
func (r *reader) number() int {
	v := r.uvarint()
	if v > math.MaxInt32 {
		r.bad = true

		return 0
	}

	return int(v)
}

func (r *reader) byte() byte {
	raw := r.take(1)
	if raw == nil {
		return 0
	}

	return raw[0]
}

func (r *reader) int64() int64 {
	raw := r.take(8)
	if raw == nil {
		return 0
	}

	return int64(binary.LittleEndian.Uint64(raw)) //nolint:gosec // G115: written from an int64
}

func (r *reader) time() time.Time {
	nanos := r.int64()
	if nanos == math.MinInt64 {
		return time.Time{}
	}

	return time.Unix(0, nanos)
}

func (r *reader) string() string {
	v := r.uvarint()
	if v&1 == 1 {
		text, ok := r.c.strings.resolve(v >> 1)
		if !ok {
			r.bad = true
		}

		return text
	}

	if v>>1 > uint64(len(r.rest)) {
		r.bad = true

		return ""
	}

	return string(r.take(int(v >> 1)))
}

func (r *reader) context() trace.SpanContext {
	var cfg trace.SpanContextConfig

	copy(cfg.TraceID[:], r.take(len(cfg.TraceID)))
	copy(cfg.SpanID[:], r.take(len(cfg.SpanID)))
	cfg.TraceFlags = trace.TraceFlags(r.byte())
	cfg.Remote = r.byte() == 1

	if header := r.string(); header != "" {
		state, err := trace.ParseTraceState(header)
		if err != nil {
			r.bad = true
		}

		cfg.TraceState = state
	}

	return trace.NewSpanContext(cfg)
}

func (r *reader) value(depth int) attribute.Value {
	switch kind := attribute.Type(r.byte()); kind {
	case attribute.EMPTY:
		return attribute.Value{}
	case attribute.BOOL:
		return attribute.BoolValue(r.byte() == 1)
	case attribute.INT64:
		return attribute.Int64Value(r.int64())
	case attribute.FLOAT64:
		return attribute.Float64Value(math.Float64frombits(uint64(r.int64()))) //nolint:gosec // G115: a bit pattern
	case attribute.STRING:
		return attribute.StringValue(r.string())
	case attribute.BOOLSLICE:
		items := make([]bool, r.count())
		for i := range items {
			items[i] = r.byte() == 1
		}

		return attribute.BoolSliceValue(items)
	case attribute.INT64SLICE:
		items := make([]int64, r.count())
		for i := range items {
			items[i] = r.int64()
		}

		return attribute.Int64SliceValue(items)
	case attribute.FLOAT64SLICE:
		items := make([]float64, r.count())
		for i := range items {
			items[i] = math.Float64frombits(uint64(r.int64())) //nolint:gosec // G115: a bit pattern
		}

		return attribute.Float64SliceValue(items)
	case attribute.STRINGSLICE:
		items := make([]string, r.count())
		for i := range items {
			items[i] = r.string()
		}

		return attribute.StringSliceValue(items)
	case attribute.BYTESLICE:
		return attribute.ByteSliceValue(append([]byte(nil), r.take(r.count())...))
	case attribute.SLICE:
		if depth >= depthMax {
			r.bad = true

			return attribute.Value{}
		}

		items := make([]attribute.Value, r.count())
		for i := range items {
			items[i] = r.value(depth + 1)
		}

		return attribute.SliceValue(items...)
	case attribute.MAP:
		if depth >= depthMax {
			r.bad = true

			return attribute.Value{}
		}

		return attribute.MapValue(r.attributes(depth + 1)...)
	default:
		r.bad = true

		return attribute.Value{}
	}
}

func (r *reader) attributes(depth int) []attribute.KeyValue {
	n := r.count()
	if n == 0 {
		return nil
	}

	attrs := make([]attribute.KeyValue, n)
	for i := range attrs {
		attrs[i] = attribute.KeyValue{Key: attribute.Key(r.string()), Value: r.value(depth)}
	}

	return attrs
}

// decode reads back what encode wrote; false for bytes it did not write.
// The resource is not in the bytes: the caller knows it.
func (c *codec) decode(raw []byte) (tracetest.SpanStub, bool) {
	src := &reader{rest: raw, c: c}

	scope, ok := c.scopes.resolve(src.uvarint())
	if !ok {
		return tracetest.SpanStub{}, false
	}

	stub := tracetest.SpanStub{
		InstrumentationScope: scope,
		Name:                 src.string(),
		SpanContext:          src.context(),
		Parent:               src.context(),
		SpanKind:             trace.SpanKind(src.number()),
		StartTime:            src.time(),
		EndTime:              src.time(),
		Attributes:           src.attributes(0),
	}

	if n := src.count(); n > 0 {
		stub.Events = make([]sdktrace.Event, n)
		for i := range stub.Events {
			stub.Events[i] = sdktrace.Event{
				Name: src.string(), Time: src.time(), Attributes: src.attributes(0), DroppedAttributeCount: src.number(),
			}
		}
	}

	if n := src.count(); n > 0 {
		stub.Links = make([]sdktrace.Link, n)
		for i := range stub.Links {
			stub.Links[i] = sdktrace.Link{
				SpanContext: src.context(), Attributes: src.attributes(0), DroppedAttributeCount: src.number(),
			}
		}
	}

	stub.Status = sdktrace.Status{Code: codes.Code(src.number()), Description: src.string()} //nolint:gosec // G115: bounded by number
	stub.DroppedAttributes = src.number()
	stub.DroppedLinks = src.number()
	stub.DroppedEvents = src.number()
	stub.ChildSpanCount = src.number()

	if src.bad || len(src.rest) != 0 {
		return tracetest.SpanStub{}, false
	}

	return stub, true
}
