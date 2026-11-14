package tiercache

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/testx"
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

	assert.Equal(t, resilience.BreakerConfig{FailureRatio: 0.5, MinRequests: 10, Window: 10 * time.Second, OpenTimeout: 5 * time.Second, HalfOpenProbes: 1}, cfg.breaker())

	cfg.BreakerFailureRatio = 1.5
	require.ErrorContains(t, cfg.Validate(), "breaker_failure_ratio")
}

func TestMetricsAreLintClean(t *testing.T) {
	t.Parallel()

	cache, err := New(Config{Name: "lint", HardTTL: time.Minute}, byteCodec{}, func(context.Context, Key) ([]byte, error) { return nil, nil }, nil, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	reg := prometheus.NewRegistry()
	reg.MustRegister(cache.Collectors()...)
	testx.LintMetrics(t, reg)
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

	cache, err := New(cfg, byteCodec{}, func(context.Context, Key) ([]byte, error) { return nil, nil }, nil, log)
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

// l1MaxEntry is a promise about the store underneath: an entry of exactly
// that size is held, and one byte more is refused and counted — never
// dropped in silence.
func TestLargestEntryL1AcceptsIsHeld(t *testing.T) {
	t.Parallel()

	cache := newByteCache(t, nil, nil)
	key := KeyOf("largest")

	cache.fill(cache.epoch.Load(), key, make([]byte, l1MaxEntry-len(key)))
	assert.True(t, cache.local.Has(key[:]), "an entry of l1MaxEntry bytes is in L1")
	assert.Zero(t, testutil.ToFloat64(cache.metrics.oversize))

	over := KeyOf("one byte over")
	cache.fill(cache.epoch.Load(), over, make([]byte, l1MaxEntry-len(over)+1))
	assert.False(t, cache.local.Has(over[:]))
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
