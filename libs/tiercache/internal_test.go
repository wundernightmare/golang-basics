package tiercache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestBreaker(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	failure := errors.New("refused")
	brk := &breaker{threshold: 3, cooldown: 10 * time.Second}

	assert.True(t, brk.allow(now))
	assert.False(t, brk.record(now, failure))
	assert.False(t, brk.record(now, failure))
	assert.False(t, brk.record(now, nil), "a success resets the count")
	assert.False(t, brk.record(now, failure))
	assert.False(t, brk.record(now, context.Canceled), "a caller that left says nothing about L2")
	assert.False(t, brk.record(now, failure))
	assert.True(t, brk.allow(now), "two consecutive failures: still closed")

	assert.True(t, brk.record(now, failure), "the third consecutive failure opens it")
	assert.False(t, brk.allow(now))
	assert.False(t, brk.allow(now.Add(10*time.Second-time.Nanosecond)))

	afterCooldown := now.Add(10 * time.Second)
	assert.True(t, brk.allow(afterCooldown), "calls are let through after the cooldown")
	assert.True(t, brk.record(afterCooldown, failure), "one failed probe re-opens it")
	assert.False(t, brk.allow(afterCooldown))

	later := afterCooldown.Add(10 * time.Second)
	assert.False(t, brk.record(later, nil))
	assert.False(t, brk.record(later, failure), "after a successful probe it takes the full threshold again")
	assert.True(t, brk.allow(later))
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
