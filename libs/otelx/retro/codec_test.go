package retro

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func contextOf(tr, span byte, flags trace.TraceFlags, remote bool, state string) trace.SpanContext {
	cfg := trace.SpanContextConfig{
		TraceID: trace.TraceID{0: 0xaa, 15: tr}, SpanID: trace.SpanID{0: 0xbb, 7: span},
		TraceFlags: flags, Remote: remote,
	}

	if state != "" {
		parsed, err := trace.ParseTraceState(state)
		if err != nil {
			panic(err)
		}

		cfg.TraceState = parsed
	}

	return trace.NewSpanContext(cfg)
}

// full is a span with something in every field the SDK exports.
func full() tracetest.SpanStub {
	start := time.Unix(1_700_000_000, 123_456_789)

	return tracetest.SpanStub{
		Name:        "GET /tasks/{id}",
		SpanContext: contextOf(1, 2, 0, false, "vendor=x,dbg=1"),
		Parent:      contextOf(1, 1, trace.FlagsSampled, true, ""),
		SpanKind:    trace.SpanKindServer,
		StartTime:   start,
		EndTime:     start.Add(1500 * time.Microsecond),
		Attributes: []attribute.KeyValue{
			attribute.String("http.request.method", "GET"),
			attribute.String("url.path", "/tasks/0198c0de"),
			attribute.Int64("http.response.status_code", 404),
			attribute.Int64("negative", -7),
			attribute.Bool("yes", true),
			attribute.Bool("no", false),
			attribute.Float64("ratio", 0.25),
			attribute.String("empty", ""),
			attribute.BoolSlice("bools", []bool{true, false}),
			attribute.Int64Slice("ints", []int64{-1, 0, 1 << 40}),
			attribute.Float64Slice("floats", []float64{1.5, -2}),
			attribute.StringSlice("strings", []string{"a", "", "c"}),
			{Key: "bytes", Value: attribute.ByteSliceValue([]byte{0, 1, 255})},
			{Key: "nothing", Value: attribute.Value{}},
			{Key: "mixed", Value: attribute.SliceValue(attribute.IntValue(1), attribute.StringValue("two"),
				attribute.SliceValue(attribute.BoolValue(true)))},
			{Key: "map", Value: attribute.MapValue(attribute.String("k", "v"), attribute.Int("n", 2))},
			attribute.String("long", strings.Repeat("x", 70_000)),
		},
		Events: []sdktrace.Event{
			{
				Name: "exception", Time: start.Add(time.Millisecond), DroppedAttributeCount: 3,
				Attributes: []attribute.KeyValue{attribute.String("exception.message", "not found")},
			},
			{Name: "bare"},
		},
		Links: []sdktrace.Link{
			{
				SpanContext: contextOf(9, 9, trace.FlagsSampled, true, "other=1"), DroppedAttributeCount: 1,
				Attributes: []attribute.KeyValue{attribute.Int("n", 1)},
			},
			{SpanContext: contextOf(8, 8, 0, false, "")},
		},
		Status:            sdktrace.Status{Code: codes.Error, Description: "task not found"},
		DroppedAttributes: 4,
		DroppedEvents:     5,
		DroppedLinks:      6,
		ChildSpanCount:    7,
		InstrumentationScope: instrumentation.Scope{
			Name: "libs/httpx", Version: "1.2.3", SchemaURL: "https://example.test/schema",
			Attributes: attribute.NewSet(attribute.String("scope", "attr")),
		},
	}
}

// requireSameSpan compares what the exporter reads: every field, the times
// as instants.
func requireSameSpan(t *testing.T, want, got tracetest.SpanStub) {
	t.Helper()

	require.True(t, want.StartTime.Equal(got.StartTime), "start %v != %v", want.StartTime, got.StartTime)
	require.True(t, want.EndTime.Equal(got.EndTime), "end %v != %v", want.EndTime, got.EndTime)
	require.Len(t, got.Events, len(want.Events))

	for i := range want.Events {
		require.True(t, want.Events[i].Time.Equal(got.Events[i].Time), "event %d", i)
		got.Events[i].Time = want.Events[i].Time
	}

	got.StartTime, got.EndTime = want.StartTime, want.EndTime
	require.Equal(t, want, got)
}

func TestCodec_EveryFieldComesBack(t *testing.T) {
	t.Parallel()

	enc := newCodec()
	want := full()

	raw, ok := enc.encode(nil, want.Snapshot())
	require.True(t, ok)

	got, ok := enc.decode(raw)
	require.True(t, ok)
	requireSameSpan(t, want, got)

	// And a span with nothing in it.
	bare := tracetest.SpanStub{Name: "bare"}

	raw, ok = enc.encode(raw[:0], bare.Snapshot())
	require.True(t, ok)

	got, ok = enc.decode(raw)
	require.True(t, ok)
	assert.Equal(t, bare, got)
	assert.True(t, got.StartTime.IsZero(), "a span without times has none after the round trip")
}

// encode appends: what was in the buffer stays, and the buffer is reused.
func TestCodec_EncodeAppendsToTheCallersBuffer(t *testing.T) {
	t.Parallel()

	enc := newCodec()
	span := full().Snapshot()

	first, ok := enc.encode(nil, span)
	require.True(t, ok)

	prefixed, ok := enc.encode([]byte("prefix"), span)
	require.True(t, ok)
	assert.Equal(t, "prefix", string(prefixed[:6]))
	assert.Equal(t, first, prefixed[6:], "the second encoding names the same strings by the same numbers")

	reused, ok := enc.encode(first[:0], span)
	require.True(t, ok)
	assert.Same(t, &first[0], &reused[0], "no new buffer when the old one is large enough")
}

// Any cut of a record is refused; none of them panics or is taken for a
// shorter span.
func TestCodec_ATruncatedRecordIsRefused(t *testing.T) {
	t.Parallel()

	enc := newCodec()
	want := full()
	want.Attributes = want.Attributes[:16] // without the 70 KiB one: every cut is tried

	raw, ok := enc.encode(nil, want.Snapshot())
	require.True(t, ok)

	for cut := range raw {
		_, ok := enc.decode(raw[:cut])
		require.False(t, ok, "cut at %d of %d", cut, len(raw))
	}

	_, ok = enc.decode(append(append([]byte(nil), raw...), 0))
	assert.False(t, ok, "a byte too many is not a record either")
}

func TestCodec_CorruptRecordsAreRefused(t *testing.T) {
	t.Parallel()

	enc := newCodec()

	raw, ok := enc.encode(nil, tracetest.SpanStub{Name: "a"}.Snapshot())
	require.True(t, ok)

	for name, corrupt := range map[string]func([]byte) []byte{
		"a scope nobody has seen":     func(b []byte) []byte { b[0] = 200; return b },
		"a string number nobody gave": func(b []byte) []byte { return append([]byte{b[0], 0xff, 0xff, 0x03}, b[2:]...) },
		"a count beyond the bytes":    func(b []byte) []byte { b[len(b)-8] = 0xff; return b },
		"nothing at all":              func([]byte) []byte { return nil },
	} {
		_, ok := enc.decode(corrupt(append([]byte(nil), raw...)))
		assert.False(t, ok, name)
	}

	// A value nested deeper than the limit: not written, and not read.
	deep := attribute.IntValue(1)
	for range depthMax + 1 {
		deep = attribute.SliceValue(deep)
	}

	_, ok = enc.encode(nil, tracetest.SpanStub{Attributes: []attribute.KeyValue{{Key: "deep", Value: deep}}}.Snapshot())
	assert.False(t, ok)

	atLimit := attribute.IntValue(1)
	for range depthMax {
		atLimit = attribute.SliceValue(atLimit)
	}

	stub := tracetest.SpanStub{Attributes: []attribute.KeyValue{{Key: "deep", Value: atLimit}}}

	raw, ok = enc.encode(nil, stub.Snapshot())
	require.True(t, ok)

	got, ok := enc.decode(raw)
	require.True(t, ok)
	assert.Equal(t, stub, got)
}

// Strings of the code are written as numbers: the second span of a kind is a
// fraction of the first.
func TestCodec_RepeatingStringsAreWrittenOnce(t *testing.T) {
	t.Parallel()

	enc := newCodec()
	span := tracetest.SpanStub{
		Name: "GET /tasks/{id}",
		Attributes: []attribute.KeyValue{
			attribute.String("http.request.method", "GET"),
			attribute.String("http.route", "/tasks/{id}"),
			attribute.String("url.scheme", "https"),
			attribute.String("network.protocol.version", "1.1"),
			attribute.String("server.address", "tasks.internal"),
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", "tasks.events"),
			attribute.String("db.operation.name", "SELECT"),
		},
	}

	raw, ok := enc.encode(nil, span.Snapshot())
	require.True(t, ok)

	inline := len(span.Name)
	for _, attr := range span.Attributes {
		inline += len(attr.Key) + len(attr.Value.AsString())
	}

	bare, ok := enc.encode(nil, tracetest.SpanStub{}.Snapshot())
	require.True(t, ok)

	// A number and a kind byte for each of the seventeen strings.
	assert.LessOrEqual(t, len(raw)-len(bare), 3*len(span.Attributes)+1, "%d bytes for %d bytes of text", len(raw)-len(bare), inline)
	assert.Equal(t, 1+2*len(span.Attributes), enc.strings.len(), "the name, the keys and the values")

	got, ok := enc.decode(raw)
	require.True(t, ok)
	assert.Equal(t, span, got)
}

// What comes from data is written as it is: an id, a path, a statement and
// a message would fill the dictionary and repeat nothing.
func TestCodec_DataStaysInline(t *testing.T) {
	t.Parallel()

	enc := newCodec()

	for i := range 50 {
		id := strings.Repeat(string(rune('a'+i%26)), 8+i)
		span := tracetest.SpanStub{
			Name: "process",
			Attributes: []attribute.KeyValue{
				attribute.String("url.path", "/tasks/"+id),
				attribute.String("task.id", id),
				attribute.String("db.query.text", "SELECT "+id),
				attribute.String("messaging.message.id", id),
				attribute.String("client.address", id),
				attribute.StringSlice("ids", []string{id}),
			},
			Status: sdktrace.Status{Code: codes.Error, Description: "failed " + id},
		}

		_, ok := enc.encode(nil, span.Snapshot())
		require.True(t, ok)
	}

	assert.Equal(t, 1+6, enc.strings.len(), "the span name and the six keys; none of the values")

	long := tracetest.SpanStub{Attributes: []attribute.KeyValue{attribute.String(strings.Repeat("k", internedLenMax+1), "v")}}

	raw, ok := enc.encode(nil, long.Snapshot())
	require.True(t, ok)
	assert.Equal(t, 7, enc.strings.len(), "a string longer than the limit is inline, whatever it is")

	got, ok := enc.decode(raw)
	require.True(t, ok)
	assert.Equal(t, long, got)
}

// A full dictionary does not lose a span: what has no number is written out.
// What may turn out to be data stops getting numbers early, so that it
// cannot crowd out what repeats for certain: values at a quarter of the
// dictionary, names the code formats data into at half, keys never.
func TestCodec_AFullDictionaryFallsBackToText(t *testing.T) {
	t.Parallel()

	enc := newCodec()

	name := func(i int) string {
		return "span " + string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676))
	}

	for i := range dictionaryMax {
		stub := tracetest.SpanStub{Attributes: []attribute.KeyValue{attribute.String("db.operation.name", name(i))}}

		raw, ok := enc.encode(nil, stub.Snapshot())
		require.True(t, ok)

		got, ok := enc.decode(raw)
		require.True(t, ok)
		require.Equal(t, stub, got, "value %d", i)
	}

	require.Equal(t, valuesMax, enc.strings.len(), "values get numbers while a quarter of it is free")

	_, ok := enc.encode(nil, tracetest.SpanStub{Name: "a name after the values"}.Snapshot())
	require.True(t, ok)
	require.Equal(t, valuesMax+1, enc.strings.len(), "and leave numbers for the names")

	for i := range dictionaryMax {
		_, ok := enc.encode(nil, tracetest.SpanStub{Name: name(i) + " name"}.Snapshot())
		require.True(t, ok)
	}

	require.Equal(t, namesMax, enc.strings.len(), "names while half of it is free")

	for i := range dictionaryMax {
		stub := tracetest.SpanStub{Attributes: []attribute.KeyValue{attribute.Int(name(i)+" key", i)}}

		raw, ok := enc.encode(nil, stub.Snapshot())
		require.True(t, ok)

		got, ok := enc.decode(raw)
		require.True(t, ok)
		require.Equal(t, stub, got, "key %d", i)
	}

	assert.Equal(t, dictionaryMax, enc.strings.len(), "keys take the rest, and no more")

	stub := tracetest.SpanStub{Name: "one more name", Attributes: []attribute.KeyValue{attribute.Int("one more key", 1)}}

	raw, ok := enc.encode(nil, stub.Snapshot())
	require.True(t, ok)

	got, ok := enc.decode(raw)
	require.True(t, ok)
	assert.Equal(t, stub, got)
	assert.Equal(t, dictionaryMax, enc.strings.len())
}

func TestCodec_ScopesAreNumberedAndBounded(t *testing.T) {
	t.Parallel()

	enc := newCodec()

	for i := range scopesMax {
		stub := tracetest.SpanStub{InstrumentationScope: instrumentation.Scope{Name: "lib", Version: string(rune('a' + i))}}

		raw, ok := enc.encode(nil, stub.Snapshot())
		require.True(t, ok)

		got, ok := enc.decode(raw)
		require.True(t, ok)
		require.Equal(t, stub, got)
	}

	_, ok := enc.encode(nil, tracetest.SpanStub{InstrumentationScope: instrumentation.Scope{Name: "one too many"}}.Snapshot())
	assert.False(t, ok, "a span of a scope beyond the table is not kept")

	_, ok = enc.encode(nil, tracetest.SpanStub{InstrumentationScope: instrumentation.Scope{Name: "lib", Version: "a"}}.Snapshot())
	assert.True(t, ok, "the scopes it knows are still served")
}

func TestHasStaticValue(t *testing.T) {
	t.Parallel()

	for key, want := range map[attribute.Key]bool{
		"http.request.method": true, "http.route": true, "url.scheme": true, "error.type": true,
		"server.address": true, "peer.service": true,
		"messaging.system": true, "messaging.destination.name": true, "db.system.name": true,
		"rpc.method": true, "network.protocol.version": true, "code.function.name": true, "otel.status_code": true,
		"db.query.text": false, "db.statement": false, "messaging.message.id": false,
		"messaging.kafka.message.key": false, "network.peer.address": false,
		"url.path": false, "url.full": false, "client.address": false, "user_agent.original": false,
		"http.request.header.x": false, "task.id": false, "": false, "db": false,
	} {
		assert.Equal(t, want, hasStaticValue(key), string(key))
	}
}

// The bytes come from the arena and nowhere else — but the decoder must not
// be what takes the service down if they are ever not what it wrote.
func FuzzDecode(f *testing.F) {
	enc := newCodec()

	for _, stub := range []tracetest.SpanStub{full(), {Name: "bare"}, {}} {
		raw, ok := enc.encode(nil, stub.Snapshot())
		require.True(f, ok)
		f.Add(raw)
	}

	f.Add([]byte{})
	f.Add([]byte{0, 0})

	f.Fuzz(func(t *testing.T, raw []byte) {
		stub, ok := enc.decode(raw)
		if !ok {
			return
		}

		// What decodes, encodes — and decodes to the same span.
		again, ok := enc.encode(nil, stub.Snapshot())
		require.True(t, ok)

		back, ok := enc.decode(again)
		require.True(t, ok)
		require.Equal(t, stub.Name, back.Name)
		require.Equal(t, stub.SpanContext, back.SpanContext)
		require.Len(t, back.Attributes, len(stub.Attributes))
	})
}

// full is what the round trip is checked with, so it must have something in
// every field a span has: a field the SDK adds later shows up here as a zero
// one, and the codec gets it — or a reason not to.
func TestCodec_TheReferenceSpanFillsEveryField(t *testing.T) {
	t.Parallel()

	notWritten := map[string]string{
		"Resource":               "one for the process: the processor puts it back",
		"InstrumentationLibrary": "the deprecated twin of InstrumentationScope",
	}

	value := reflect.ValueOf(full())
	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		if _, skipped := notWritten[name]; skipped {
			continue
		}

		assert.False(t, value.Field(i).IsZero(), "full() leaves %s empty: the round trip does not check it", name)
	}
}

// The size a span takes in the arena, for the README's arithmetic: a server
// span as libs/httpx records it, once its strings are in the dictionary.
func TestCodec_SizeOfAServerSpan(t *testing.T) {
	t.Parallel()

	enc := newCodec()
	span := tracetest.SpanStub{
		Name:        "GET /tasks/{id}",
		SpanContext: contextOf(1, 2, 0, false, ""),
		Parent:      contextOf(1, 1, 0, true, ""),
		SpanKind:    trace.SpanKindServer,
		StartTime:   time.Unix(1_700_000_000, 0),
		EndTime:     time.Unix(1_700_000_000, 2_000_000),
		Attributes: []attribute.KeyValue{
			attribute.String("http.request.method", "GET"),
			attribute.String("url.path", "/tasks/0198c0de-7b1a-7c3e-9f00-5a1b2c3d4e5f"),
			attribute.String("url.scheme", "https"),
			attribute.String("server.address", "tasks.internal:8080"),
			attribute.String("network.protocol.version", "1.1"),
			attribute.String("user_agent.original", "resilient-http-client/1.0"),
			attribute.String("client.address", "10.42.0.17"),
			attribute.String("http.route", "/tasks/{id}"),
			attribute.Int("http.response.status_code", 200),
		},
		InstrumentationScope: instrumentation.Scope{Name: "github.com/tracehubmmp/golang-basics/libs/httpx"},
	}

	_, ok := enc.encode(nil, span.Snapshot())
	require.True(t, ok)

	raw, ok := enc.encode(nil, span.Snapshot())
	require.True(t, ok)

	text := len(span.Name) + len(span.InstrumentationScope.Name)
	for _, attr := range span.Attributes {
		text += len(attr.Key) + len(attr.Value.String())
	}

	t.Logf("a server span: %d bytes in the arena (+%d of header), %d bytes of text alone", len(raw), headerLen, text)
	assert.Less(t, len(raw), 220, "a server span stays near two hundred bytes")
}

func TestTable_NumbersValuesInTheOrderItSeesThem(t *testing.T) {
	t.Parallel()

	numbers := newTable[string]()

	first, ok := numbers.intern("a", 2)
	require.True(t, ok)

	second, ok := numbers.intern("b", 2)
	require.True(t, ok)

	again, ok := numbers.intern("a", 2)
	require.True(t, ok)
	assert.Equal(t, []uint32{0, 1, 0}, []uint32{first, second, again})

	_, ok = numbers.intern("c", 2)
	assert.False(t, ok, "no room")
	assert.Equal(t, 2, numbers.len())

	value, ok := numbers.resolve(1)
	assert.True(t, ok)
	assert.Equal(t, "b", value)

	value, ok = numbers.resolve(2)
	assert.False(t, ok, "the number after the last is nobody's")
	assert.Empty(t, value)
}

// A string of exactly the limit is still written as a number.
func TestCodec_TheLongestInternedString(t *testing.T) {
	t.Parallel()

	enc := newCodec()
	key := strings.Repeat("k", internedLenMax)

	_, ok := enc.encode(nil, tracetest.SpanStub{Attributes: []attribute.KeyValue{attribute.Int(key, 1)}}.Snapshot())
	require.True(t, ok)
	assert.Equal(t, 1, enc.strings.len())
}

// nested wraps a value in levels of one-element slices or one-entry maps.
func nested(levels int, asMap bool) attribute.Value {
	value := attribute.IntValue(1)

	for range levels {
		if asMap {
			value = attribute.MapValue(attribute.KeyValue{Key: "k", Value: value})
		} else {
			value = attribute.SliceValue(value)
		}
	}

	return value
}

// Nesting is bounded the same way for slices and maps, when writing and
// when reading: at the limit a value comes back, one level deeper it is not
// written — and bytes that claim it are not read.
func TestCodec_NestingIsBounded(t *testing.T) {
	t.Parallel()

	for name, asMap := range map[string]bool{"slices": false, "maps": true} {
		enc := newCodec()
		atLimit := tracetest.SpanStub{Attributes: []attribute.KeyValue{{Key: "deep", Value: nested(depthMax, asMap)}}}

		raw, ok := enc.encode(nil, atLimit.Snapshot())
		require.True(t, ok, name)

		got, ok := enc.decode(raw)
		require.True(t, ok, name)
		assert.Equal(t, atLimit, got, name)

		_, ok = enc.encode(nil, tracetest.SpanStub{Attributes: []attribute.KeyValue{{Key: "deep", Value: nested(depthMax+1, asMap)}}}.Snapshot())
		assert.False(t, ok, "%s: one level too deep is not written", name)

		// The same record with one more level spliced in front of the
		// innermost value: what a deeper span would have looked like.
		level := []byte{byte(attribute.SLICE), 1}

		if asMap {
			keyRef, _ := enc.strings.intern("k", dictionaryMax)
			level = []byte{byte(attribute.MAP), 1, byte(keyRef<<1 | 1)}
		}

		innermost := bytes.LastIndex(raw, append(append([]byte(nil), level...), byte(attribute.INT64)))
		require.Positive(t, innermost, name)

		at := innermost + len(level)
		deeper := append(append(append([]byte(nil), raw[:at]...), level...), raw[at:]...)

		_, ok = enc.decode(deeper)
		assert.False(t, ok, "%s: one level too deep is not read", name)
	}
}
