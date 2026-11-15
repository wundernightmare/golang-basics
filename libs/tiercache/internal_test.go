package tiercache

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/wundernightmare/golang-basics/libs/resilience"
	"github.com/wundernightmare/golang-basics/libs/testx"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	env := append(appendHeader(nil, 1000, 2000), "payload"...)
	soft, hard, payload, ok := parseEnvelope(env)
	require.True(t, ok)
	assert.Equal(t, int64(1000), soft)
	assert.Equal(t, int64(2000), hard)
	assert.Equal(t, "payload", string(payload))

	_, _, payload, ok = parseEnvelope(appendHeader(nil, 5, 5))
	require.True(t, ok, "an empty payload is a value")
	assert.Empty(t, payload)
}

func TestEnvelopeRejectsWhatItDidNotWrite(t *testing.T) {
	t.Parallel()

	good := appendHeader(nil, 1000, 2000)

	otherVersion := append([]byte{}, good...)
	otherVersion[0] = envelopeVersion + 1

	for name, env := range map[string][]byte{
		"empty":                nil,
		"one byte short":       good[:headerSize-1],
		"another version":      otherVersion,
		"hard before soft":     appendHeader(nil, 2000, 1000),
		"deadline overflow":    appendHeader(nil, 1, -1),
		"a valkey tombstone":   []byte("\x00valkey:tombstone"),
		"somebody else's JSON": []byte(`{"id":1,"name":"a widget long enough"}`),
	} {
		_, _, _, ok := parseEnvelope(env)
		assert.False(t, ok, name)
	}
}

// Bytes from a cache are external input: any of them must parse or be
// refused, never panic, and what parses must re-encode to the same header.
func FuzzParseEnvelope(f *testing.F) {
	f.Add([]byte(nil))
	f.Add(appendHeader(nil, 1, 2))
	f.Add(append(appendHeader(nil, 1700000000000, 1700000060000), "x"...))
	f.Add([]byte("\x00valkey:tombstone"))

	f.Fuzz(func(t *testing.T, env []byte) {
		soft, hard, payload, ok := parseEnvelope(env)
		if !ok {
			return
		}

		require.LessOrEqual(t, soft, hard)
		require.Equal(t, env[:headerSize], appendHeader(nil, soft, hard))
		require.Equal(t, env[headerSize:], payload)
	})
}

func TestConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{Name: "c", HardTTL: time.Minute}
	cfg.applyDefaults()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 32<<20, cfg.MaxBytes)
	assert.Equal(t, time.Minute, cfg.SoftTTL, "no SoftTTL: plain cache-aside")
	assert.Equal(t, 16, cfg.RefreshConcurrency)

	require.ErrorContains(t, Config{HardTTL: time.Minute}.Validate(), "Name")
	require.ErrorContains(t, Config{Name: "c"}.Validate(), "HardTTL")
	require.ErrorContains(t, Config{Name: "c", HardTTL: time.Second, SoftTTL: time.Minute}.Validate(), "SoftTTL")
	require.ErrorContains(t, Config{Name: "c", HardTTL: time.Second, StaleIfError: -1}.Validate(), "StaleIfError")

	assert.Equal(t, resilience.BreakerConfig{FailureRatio: 0.5, MinRequests: 10, Window: 10 * time.Second, OpenTimeout: 5 * time.Second, HalfOpenProbes: 1}, cfg.Breaker)

	cfg.Breaker.FailureRatio = 1.5
	require.ErrorContains(t, cfg.Validate(), "breaker: failure_ratio")
}

func TestMetricsAreLintClean(t *testing.T) {
	t.Parallel()

	cache, err := New(Config{Name: "lint", HardTTL: time.Minute}, newMapStore, byteCodec{}, func(context.Context, string) ([]byte, error) { return nil, nil }, nil, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	reg := prometheus.NewRegistry()
	reg.MustRegister(cache.Collectors()...)
	testx.LintMetrics(t, reg)
}

// mapStore is the L1 of the tests in this package, which are about the
// envelope and the deadlines and need no more of a store than a map: the
// real ones are in offheap and onheap, which cannot be imported from here
// (they import this package).
type mapStore struct {
	mu      sync.Mutex
	entries map[string][]byte
}

func newMapStore(int) Store { return &mapStore{entries: map[string][]byte{}} }

func (*mapStore) MaxEntry() int { return 1 << 20 }
func (*mapStore) Bytes() int    { return 0 }

func (s *mapStore) Get(dst []byte, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, found := s.entries[key]

	return append(dst, value...), found
}

func (s *mapStore) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[key] = append([]byte(nil), value...)
}

func (s *mapStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, key)
}

func (s *mapStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.entries)
}

type byteCodec struct{}

func (byteCodec) Append(dst, value []byte) ([]byte, error) { return append(dst, value...), nil }
func (byteCodec) Decode(src []byte) ([]byte, error)        { return append([]byte(nil), src...), nil }

func newByteCache(t *testing.T, log *slog.Logger, tune func(*Config)) *Cache[[]byte] {
	t.Helper()

	cfg := Config{Name: "internal", HardTTL: time.Minute}
	if tune != nil {
		tune(&cfg)
	}

	cache, err := New(cfg, newMapStore, byteCodec{}, func(context.Context, string) ([]byte, error) { return nil, nil }, nil, log)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	return cache
}

// The deadlines are compared to the millisecond: a value is stale from its
// soft deadline on, and gone from its hard one on — not a millisecond later.
func TestDeadlinesAreExactToTheMillisecond(t *testing.T) {
	t.Parallel()

	cache := newByteCache(t, nil, nil)

	env := append(appendHeader(nil, 1000, 2000), "v"...)

	for _, tc := range []struct {
		nowMs     int64
		stale, ok bool
	}{
		{nowMs: 999, stale: false, ok: true},
		{nowMs: 1000, stale: true, ok: true},
		{nowMs: 1999, stale: true, ok: true},
		{nowMs: 2000, ok: false},
	} {
		value, stale, ok := cache.usable(env, tc.nowMs)
		require.Equal(t, tc.ok, ok, "at %d ms", tc.nowMs)

		if ok {
			assert.Equal(t, tc.stale, stale, "at %d ms", tc.nowMs)
			assert.Equal(t, "v", string(value))
		}
	}
}

// The furthest deadline this cache trusts is the furthest it sets: HardTTL
// and a tenth, rounded up to the millisecond.
func TestTheFurthestTrustedDeadlineIsTheFurthestOneSet(t *testing.T) {
	t.Parallel()

	for hardTTL, longestMs := range map[time.Duration]int64{
		time.Minute:           66_000,
		time.Second:           1_100,
		15 * time.Millisecond: 17, // 16.5 ms: a deadline at 17 ms is one this cache can set
		time.Millisecond:      2,  // 1.1 ms
	} {
		cache := newByteCache(t, nil, func(cfg *Config) { cfg.HardTTL = hardTTL })

		assert.True(t, cache.trusted(1_000_000+longestMs, 1_000_000), "%s", hardTTL)
		assert.False(t, cache.trusted(1_000_000+longestMs+1, 1_000_000), "%s", hardTTL)
	}
}

// The largest deadline the envelope accepts is the last one it can hold.
func TestEnvelopeAcceptsItsLargestDeadline(t *testing.T) {
	t.Parallel()

	_, hard, _, ok := parseEnvelope(appendHeader(nil, 1, 1<<62))
	require.True(t, ok)
	assert.Equal(t, int64(1<<62), hard)

	_, hard, _, ok = parseEnvelope(appendHeader(nil, 1, 1<<62+1))
	assert.False(t, ok)
	assert.Zero(t, hard)
}

// The cache offers L1 an entry of exactly its limit and nothing larger: one
// byte more is counted and never reaches the store.
func TestTheLargestEntryIsOfferedToL1AndNoLarger(t *testing.T) {
	t.Parallel()

	cache := newByteCache(t, nil, nil)
	limit := cache.local.MaxEntry()

	for _, key := range []string{"k", KeyOf("largest"), strings.Repeat("k", MaxKeyLen)} {
		cache.fill(cache.epoch.Load(), key, make([]byte, limit-len(key)))

		held, found := cache.local.Get(nil, key)
		require.True(t, found, "an entry of MaxEntry bytes is in L1")
		assert.Len(t, held, limit-len(key))
	}

	assert.Zero(t, testutil.ToFloat64(cache.metrics.oversize))

	cache.fill(cache.epoch.Load(), "over", make([]byte, limit-len("over")+1))

	_, found := cache.local.Get(nil, "over")
	assert.False(t, found)
	assert.InDelta(t, 1, testutil.ToFloat64(cache.metrics.oversize), 0)
}

// StaleIfError = 0 turns the fallback off, whatever the levels still hold.
func TestWithoutStaleIfErrorNothingExpiredIsServed(t *testing.T) {
	t.Parallel()

	cache := newByteCache(t, nil, nil)
	inAnHour := time.Now().Add(time.Hour).UnixMilli()

	_, ok := cache.staleFallback(KeyOf("k"), []byte("v"), inAnHour, true)
	assert.False(t, ok)

	graced := newByteCache(t, nil, func(cfg *Config) { cfg.StaleIfError = time.Minute })
	value, ok := graced.staleFallback(KeyOf("k"), []byte("v"), inAnHour, true)
	require.True(t, ok)
	assert.Equal(t, "v", string(value))
}

// The rate-limited warning is logged — once a second at most.
func TestWarningIsLoggedAndRateLimited(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	cache := newByteCache(t, slog.New(slog.NewTextHandler(&out, nil)), nil)

	cache.warn(t.Context(), "first")
	require.Equal(t, 1, bytes.Count(out.Bytes(), []byte("\n")), "the first warning is logged")

	for range 100 {
		cache.warn(t.Context(), "again")
	}

	assert.LessOrEqual(t, bytes.Count(out.Bytes(), []byte("\n")), 2, "a hundred more within the second add at most the next second's one")
}

// The envelope carries any pair of deadlines it accepts and any payload:
// what was written is what is read, and the deadlines decide the answer of
// usable at every instant — fresh before the soft one, stale from it on,
// gone from the hard one on, and not trusted while the hard one is further
// away than this cache sets it.
func TestEnvelopeRoundTripsAnyDeadlinesAndPayload(t *testing.T) {
	t.Parallel()

	cache := newByteCache(t, nil, nil)

	const longest = int64((time.Minute + time.Minute/10) / time.Millisecond)

	rapid.Check(t, func(t *rapid.T) {
		hard := rapid.Int64Range(0, 1<<62).Draw(t, "hard")
		soft := rapid.Int64Range(0, hard).Draw(t, "soft")
		payload := rapid.SliceOfN(rapid.Byte(), 0, 512).Draw(t, "payload")
		now := rapid.OneOf(
			rapid.Just(soft), rapid.Just(soft-1), rapid.Just(hard), rapid.Just(hard-1),
			rapid.Just(hard-longest), rapid.Just(hard-longest-1), rapid.Int64Range(hard-longest, hard),
			rapid.Int64Range(0, 1<<62),
		).Draw(t, "now")

		env := append(appendHeader(nil, soft, hard), payload...)

		gotSoft, gotHard, gotPayload, ok := parseEnvelope(env)
		if !ok || gotSoft != soft || gotHard != hard || !bytes.Equal(gotPayload, payload) {
			t.Fatalf("parsed (%d, %d, %d bytes, %t) from an envelope of (%d, %d, %d bytes)",
				gotSoft, gotHard, len(gotPayload), ok, soft, hard, len(payload))
		}

		value, stale, usable := cache.usable(env, now)
		if usable != (now < hard && hard-now <= longest) {
			t.Fatalf("at %d with the hard deadline at %d (%d away): usable=%t", now, hard, hard-now, usable)
		}

		if usable && (stale != (now >= soft) || !bytes.Equal(value, payload)) {
			t.Fatalf("at %d with the soft deadline at %d: stale=%t, %d bytes of %d", now, soft, stale, len(value), len(payload))
		}
	})
}
