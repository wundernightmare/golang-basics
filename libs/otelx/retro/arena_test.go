package retro

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

const perSecond = 50

// record is one record with a payload of 71 bytes: 100 bytes of the ring.
const (
	record      = 100
	payloadSize = record - headerLen
)

func traceOf(n int) trace.TraceID {
	return trace.TraceID{15: byte(n), 14: byte(n >> 8), 0: 1}
}

// payloadOf is a payload of size bytes that names its span.
func payloadOf(span byte, size int) []byte { return bytes.Repeat([]byte{span}, size) }

func offerSpan(ring *arena, tr int, span byte, endSecs int64) bool {
	return ring.offer(traceOf(tr), endSecs*int64(time.Second), payloadOf(span, payloadSize))
}

// names is the first byte of every payload: which spans, in which order.
func names(taken [][]byte) []byte {
	out := []byte{}
	for _, payload := range taken {
		out = append(out, payload[0])
	}

	return out
}

func TestArena_HasAsManyShardsAsKeepEachAQuarterMebibyte(t *testing.T) {
	t.Parallel()

	const mib = 1 << 20

	shards := func(size int) int { return len(newArena(size, perSecond, time.Now()).shards) }

	assert.Equal(t, 1, shards(1))
	assert.Equal(t, 1, shards(shardMinBytes))
	assert.Equal(t, 2, shards(2*shardMinBytes))
	assert.Equal(t, 2, shards(3*shardMinBytes), "a power of two")
	assert.Equal(t, 4, shards(mib))
	assert.Equal(t, 16, shards(4*mib))
	assert.Equal(t, shardsMax, shards(16*mib))
	assert.Equal(t, shardsMax, shards(4096*mib))
	assert.Len(t, newArenaWithShards(mib, 0, perSecond, time.Now()).shards, 1)
	assert.Len(t, newArena(-1, perSecond, time.Now()).shards, 1, "a negative size is an empty arena, not a panic")
}

func TestArena_TheMemoryIsAllocatedOnceAtTheSizeAskedFor(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(8*record, 2, perSecond, time.Now())
	require.Equal(t, 8*record, ring.allocated())

	backing := make([]*byte, len(ring.shards))
	for i, s := range ring.shards {
		backing[i] = &s.buf[0]
	}

	// Fill it several times over, with triggers in between.
	now := time.Now()

	for span := range 200 {
		offerSpan(ring, span%7, byte(span), int64(span))

		if span%13 == 0 {
			now = now.Add(time.Second)
			ring.trigger(traceOf(span%7), now)
		}

		use := ring.usage()
		require.LessOrEqual(t, use.bytes, 8*record)
	}

	assert.Equal(t, 8*record, ring.allocated())

	for i, s := range ring.shards {
		assert.Same(t, backing[i], &s.buf[0], "the ring was never reallocated")
		assert.Len(t, s.buf, 4*record)
		assert.Equal(t, triggeredMax/2, cap(s.order), "and each shard remembers its share of the triggered traces")
	}
}

func TestArena_TheOldestSpansLeaveWhenTheRingIsFull(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(3*record, 1, perSecond, time.Now())
	for span := range byte(5) {
		assert.False(t, offerSpan(ring, 1, span, int64(span)))
	}

	use := ring.usage()
	assert.Equal(t, 3, use.spans)
	assert.Equal(t, 3*record, use.bytes)
	assert.Equal(t, uint64(5), use.kept)
	assert.Equal(t, uint64(2), use.evicted)

	outcome, taken := ring.trigger(traceOf(1), time.Now())
	assert.Equal(t, Accepted, outcome)
	assert.Equal(t, []byte{2, 3, 4}, names(taken), "the newest three, oldest first")
}

func TestArena_ASpanLargerThanAShardIsNotKept(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(3*record, 1, perSecond, time.Now())
	offerSpan(ring, 1, 1, 1)
	assert.False(t, ring.offer(traceOf(2), 0, payloadOf(9, 3*record)))

	use := ring.usage()
	assert.Equal(t, 1, use.spans, "what was there stays")
	assert.Equal(t, uint64(1), use.evicted, "the span that did not fit is counted")

	exact := payloadOf(7, 3*record-headerLen)
	assert.False(t, ring.offer(traceOf(3), 0, exact))

	_, taken := ring.trigger(traceOf(3), time.Now())
	require.Len(t, taken, 1, "a record of exactly the shard fits")
	assert.Equal(t, exact, taken[0])
}

func TestArena_ATriggerTakesTheSpansOfItsTraceAndLeavesTheRest(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(10*record, 1, perSecond, time.Now())
	offerSpan(ring, 1, 10, 1)
	offerSpan(ring, 2, 20, 2)
	offerSpan(ring, 1, 11, 3)
	offerSpan(ring, 3, 30, 4)

	assert.False(t, ring.isTriggered(traceOf(1)))

	outcome, taken := ring.trigger(traceOf(1), time.Now())
	assert.Equal(t, Accepted, outcome)
	assert.Equal(t, []byte{10, 11}, names(taken))
	assert.True(t, ring.isTriggered(traceOf(1)))
	assert.False(t, ring.isTriggered(traceOf(2)))

	use := ring.usage()
	assert.Equal(t, 2, use.spans)
	assert.Equal(t, 3*record, use.bytes, "the taken record at the front freed its room; the one in the middle keeps its place")
	assert.Equal(t, uint64(2), use.taken)
	assert.Zero(t, use.evicted, "a taken span was not evicted")

	// A span of the triggered trace is not kept: the caller exports it.
	assert.True(t, offerSpan(ring, 1, 12, 5))
	assert.Equal(t, 2, ring.usage().spans)

	outcome, taken = ring.trigger(traceOf(1), time.Now())
	assert.Equal(t, Repeated, outcome)
	assert.Empty(t, taken)

	// The others are still there, and the taken record in the middle is
	// skipped, then dropped when it reaches the front.
	_, taken = ring.trigger(traceOf(2), time.Now())
	assert.Equal(t, []byte{20}, names(taken))
	assert.Equal(t, record, ring.usage().bytes, "both taken records left with the front")

	_, taken = ring.trigger(traceOf(3), time.Now())
	assert.Equal(t, []byte{30}, names(taken))
	assert.Zero(t, ring.usage().bytes)
}

// A record that crosses the end of the ring is read back whole: its header,
// its payload, and the state byte a trigger writes.
func TestArena_RecordsAcrossTheSeamOfTheRing(t *testing.T) {
	t.Parallel()

	for shift := range record {
		// A ring of three records and a bit: every offset of the seam.
		ring := newArenaWithShards(3*record+shift, 1, 1000, time.Now())

		for span := range byte(40) {
			size := payloadSize - int(span%5) // different lengths move the seam around
			want := payloadOf(span, size)
			require.False(t, ring.offer(traceOf(int(span)), int64(span), want))

			_, taken := ring.trigger(traceOf(int(span)), time.Now().Add(time.Duration(span)*time.Second))
			require.Len(t, taken, 1, "shift %d span %d", shift, span)
			require.Equal(t, want, taken[0], "shift %d span %d", shift, span)

			// Leave something behind so the head keeps moving.
			require.False(t, ring.offer(traceOf(1000+int(span)), int64(span), payloadOf(span, size)))
		}

		use := ring.usage()
		require.LessOrEqual(t, use.bytes, 3*record+shift)
		require.Positive(t, use.spans)
	}
}

func TestArena_TheRateLimitRefusesTriggersAboveItsSecond(t *testing.T) {
	t.Parallel()

	start := time.Now()
	ring := newArenaWithShards(10*record, 1, 2, start)

	at := func(tr int, after time.Duration) Outcome {
		outcome, _ := ring.trigger(traceOf(tr), start.Add(after))

		return outcome
	}

	assert.Equal(t, Accepted, at(1, 0))
	assert.Equal(t, Repeated, at(1, 0), "a repeat does not spend the budget")
	assert.Equal(t, Accepted, at(2, 500*time.Millisecond))
	assert.Equal(t, RateLimited, at(3, 999*time.Millisecond))
	assert.False(t, ring.isTriggered(traceOf(3)))
	assert.Equal(t, Accepted, at(3, time.Second), "a new second")
	assert.Equal(t, Accepted, at(4, 1500*time.Millisecond))
	assert.Equal(t, RateLimited, at(5, 1999*time.Millisecond))
	assert.Equal(t, Accepted, at(5, time.Hour))
	assert.Equal(t, Accepted, at(6, time.Hour-time.Minute), "a reading older than the window's start is inside it")
	assert.Equal(t, RateLimited, at(7, time.Hour-time.Minute))

	use := ring.usage()
	assert.Equal(t, uint64(6), use.accepted)
	assert.Equal(t, uint64(1), use.repeated)
	assert.Equal(t, uint64(3), use.refused)

	off := newArenaWithShards(10*record, 1, 0, start)
	outcome, _ := off.trigger(traceOf(1), start)
	assert.Equal(t, RateLimited, outcome, "no budget, no triggers")
}

func TestArena_RemembersABoundedNumberOfTriggeredTraces(t *testing.T) {
	t.Parallel()

	start := time.Now()
	ring := newArenaWithShards(record, 1, 1<<20, start)

	for tr := range triggeredMax + 10 {
		outcome, _ := ring.trigger(traceOf(tr), start)
		require.Equal(t, Accepted, outcome)
	}

	assert.Equal(t, int64(triggeredMax), ring.triggeredCount.Load())
	assert.Len(t, ring.shards[0].triggered, triggeredMax)
	assert.False(t, ring.isTriggered(traceOf(0)), "the oldest was forgotten")
	assert.False(t, ring.isTriggered(traceOf(9)))
	assert.True(t, ring.isTriggered(traceOf(10)))
	assert.True(t, ring.isTriggered(traceOf(triggeredMax+9)))

	// A forgotten trace is kept again, and can be triggered again.
	assert.False(t, offerSpan(ring, 0, 1, 1))

	outcome, taken := ring.trigger(traceOf(0), start)
	assert.Equal(t, Accepted, outcome)
	assert.Len(t, taken, 1)
}

func TestArena_TheHorizonIsFromTheOldestSpanKeptToTheNewest(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(3*record, 1, perSecond, time.Now())
	assert.Zero(t, ring.usage().horizon)

	offerSpan(ring, 1, 1, 100)
	assert.Zero(t, ring.usage().horizon, "one span")

	offerSpan(ring, 2, 2, 130)
	offerSpan(ring, 3, 3, 160)
	assert.Equal(t, time.Minute, ring.usage().horizon)

	offerSpan(ring, 4, 4, 190) // evicts the span of second 100
	assert.Equal(t, time.Minute, ring.usage().horizon)

	ring.trigger(traceOf(2), time.Now()) // the oldest kept is now the one of second 160
	assert.Equal(t, 30*time.Second, ring.usage().horizon)

	offerSpan(ring, 5, 5, 150) // ended earlier than the newest: the newest stays
	assert.Equal(t, 30*time.Second, ring.usage().horizon)

	two := newArenaWithShards(20*record, 2, perSecond, time.Now())
	for span := range 16 {
		offerSpan(two, span, byte(span), int64(span*span))
	}

	var shortest time.Duration

	for i, s := range two.shards {
		require.Positive(t, s.spans, "shard %d holds spans", i)

		if h := s.horizon(); i == 0 || h < shortest {
			shortest = h
		}
	}

	assert.Equal(t, shortest, two.usage().horizon, "the arena reaches back as far as its shortest shard")
}

// A ring whose bytes are not records any more (a bug, a panic mid-write) is
// started over, not misread.
func TestArena_ACorruptRingStartsOver(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(3*record, 1, perSecond, time.Now())
	offerSpan(ring, 1, 1, 1)
	offerSpan(ring, 1, 2, 2)

	s := ring.shards[0]
	s.buf[0], s.buf[1], s.buf[2], s.buf[3] = 0xff, 0xff, 0xff, 0x7f // a payload length beyond the ring

	_, taken := ring.trigger(traceOf(1), time.Now())
	assert.Empty(t, taken, "nothing is read past a header that cannot be")

	for span := range byte(4) {
		offerSpan(ring, 2, 10+span, int64(span))
	}

	_, taken = ring.trigger(traceOf(2), time.Now())
	assert.Equal(t, []byte{11, 12, 13}, names(taken), "the ring works again")
}

// The ring against a model: a list of records with the same rules. Whatever
// the history of offers and triggers, both hold the same spans in the same
// order, and the ring never holds more bytes than it has.
//
// The histories are drawn from seeded generators, so a failure names the
// seed that reproduces it. (Not rapid, which the pure-logic libraries use:
// a test dependency of this module becomes an indirect requirement of
// libs/otelx/integration that go mod tidy and go work sync disagree about.)
func TestArena_BehavesLikeAListOfRecords(t *testing.T) {
	t.Parallel()

	type modelRecord struct {
		tr      int
		payload []byte
		taken   bool
	}

	const (
		histories = 400
		steps     = 300
	)

	for seed := range uint64(histories) {
		rnd := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))

		capacity := headerLen + 1 + rnd.IntN(600-headerLen)
		start := time.Now()
		ring := newArenaWithShards(capacity, 1, 1<<20, start)

		var (
			model     []modelRecord
			triggered = map[int]bool{}
			next      byte
		)

		modelBytes := func() int {
			total := 0
			for _, rec := range model {
				total += headerLen + len(rec.payload)
			}

			return total
		}

		offer := func(step, owner int) {
			size := rnd.IntN(201)
			next++
			payload := payloadOf(next, size)

			got := ring.offer(traceOf(owner), int64(next), payload)
			require.Equal(t, triggered[owner], got, "seed %d step %d", seed, step)

			if triggered[owner] || headerLen+size > capacity {
				return
			}

			for modelBytes()+headerLen+size > capacity {
				model = model[1:]
			}

			model = append(model, modelRecord{tr: owner, payload: payload})
		}

		trigger := func(step, owner int) {
			outcome, taken := ring.trigger(traceOf(owner), start)

			if triggered[owner] {
				require.Equal(t, Repeated, outcome, "seed %d step %d", seed, step)
				require.Empty(t, taken)

				return
			}

			triggered[owner] = true

			var want [][]byte

			for i := range model {
				if model[i].tr == owner && !model[i].taken {
					want = append(want, model[i].payload)
					model[i].taken = true
				}
			}

			for len(model) > 0 && model[0].taken {
				model = model[1:]
			}

			require.Equal(t, Accepted, outcome, "seed %d step %d", seed, step)
			require.Equal(t, want, taken, "seed %d step %d", seed, step)
		}

		for step := range steps {
			if owner := rnd.IntN(6); rnd.IntN(5) > 0 {
				offer(step, owner)
			} else {
				trigger(step, owner)
			}

			live := 0

			for _, rec := range model {
				if !rec.taken {
					live++
				}
			}

			use := ring.usage()
			require.Equal(t, live, use.spans, "seed %d step %d", seed, step)
			require.Equal(t, modelBytes(), use.bytes, "seed %d step %d", seed, step)
			require.LessOrEqual(t, use.bytes, capacity)
		}
	}
}

// A trace forgotten by the triggered set and triggered again does not get
// the spans it was handed the first time: their records are still in the
// ring, marked taken.
func TestArena_ATakenSpanIsHandedOutOnce(t *testing.T) {
	t.Parallel()

	ring := newArenaWithShards(10*record, 1, perSecond, time.Now())
	ring.shards[0] = newShard(10*record, 1) // remembers one triggered trace

	offerSpan(ring, 2, 20, 1)
	offerSpan(ring, 1, 10, 2)

	_, taken := ring.trigger(traceOf(1), time.Now())
	require.Equal(t, []byte{10}, names(taken))

	_, _ = ring.trigger(traceOf(3), time.Now())
	require.False(t, ring.isTriggered(traceOf(1)), "forgotten")

	outcome, taken := ring.trigger(traceOf(1), time.Now())
	assert.Equal(t, Accepted, outcome)
	assert.Empty(t, taken, "its record is in the ring behind a live one, and was taken")
	assert.Equal(t, uint64(1), ring.usage().taken)
}
