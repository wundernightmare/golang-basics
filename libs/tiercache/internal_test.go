package tiercache

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
