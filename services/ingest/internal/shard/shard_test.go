package shard_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

func sample() []shard.Record {
	at := time.UnixMilli(1_794_000_000_123).UTC()

	return []shard.Record{
		{ID: "e-1", Type: "page.viewed", ReceivedAt: at, Event: []byte(`{"id":"e-1","payload":{"a":1}}`)},
		{ID: "e-2", ReceivedAt: at.Add(time.Millisecond), Event: []byte(`{"id":"e-2","payload":{}}`)},
		{ID: "e-3", Type: "x", ReceivedAt: at, Event: nil}, // the format does not require a body
	}
}

func encode(t testing.TB, records []shard.Record) []byte {
	t.Helper()

	buf := shard.AppendHeader(nil)

	for _, r := range records {
		var err error

		before := len(buf)
		buf, err = shard.AppendRecord(buf, r)
		require.NoError(t, err)
		require.Equal(t, shard.EncodedSize(r), len(buf)-before, "EncodedSize is what AppendRecord adds")
	}

	return buf
}

func decode(data []byte, maxRecord int) ([]shard.Record, error) {
	reader := shard.NewReader(bytes.NewReader(data), maxRecord)

	var out []shard.Record

	for {
		r, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}

		if err != nil {
			return out, err
		}

		r.Event = bytes.Clone(r.Event) // valid only until the next call
		out = append(out, r)
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	want := sample()
	got, err := decode(encode(t, want), 1<<20)
	require.NoError(t, err)
	require.Len(t, got, len(want))

	for i := range want {
		assert.Equal(t, want[i].ID, got[i].ID)
		assert.Equal(t, want[i].Type, got[i].Type)
		assert.True(t, want[i].ReceivedAt.Equal(got[i].ReceivedAt))
		assert.Equal(t, string(want[i].Event), string(got[i].Event))
	}

	got, err = decode(shard.AppendHeader(nil), 1<<20)
	require.NoError(t, err, "a header alone is an empty shard")
	assert.Empty(t, got)
}

func TestAppendRecordRefusesFieldsThatDoNotFit(t *testing.T) {
	t.Parallel()

	long := string(make([]byte, shard.MaxFieldBytes+1))

	for name, r := range map[string]shard.Record{
		"empty id":  {ID: ""},
		"long id":   {ID: long},
		"long type": {ID: "ok", Type: long},
	} {
		buf := shard.AppendHeader(nil)
		out, err := shard.AppendRecord(buf, r)
		require.Error(t, err, name)
		assert.Len(t, out, len(buf), "nothing is appended")
	}
}

func TestReaderDetectsCorruption(t *testing.T) {
	t.Parallel()

	good := encode(t, sample())

	flip := func(at int) []byte {
		bad := bytes.Clone(good)
		bad[at] ^= 0xff

		return bad
	}

	for name, data := range map[string][]byte{
		"empty":            {},
		"short header":     good[:5],
		"bad magic":        flip(0),
		"unknown version":  flip(4),
		"flipped payload":  flip(len(good) - 1),
		"flipped checksum": flip(8 + 4),
		"flipped length":   flip(8),
		"truncated prefix": good[:8+3],
		"truncated body":   good[:len(good)-2],
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := decode(data, 1<<20)
			require.ErrorIs(t, err, shard.ErrCorrupt)
		})
	}

	t.Run("record over the reader's limit", func(t *testing.T) {
		t.Parallel()

		_, err := decode(good, 16)
		require.ErrorIs(t, err, shard.ErrCorrupt, "the length field must not decide the allocation")
	})

	t.Run("records before the damage are returned", func(t *testing.T) {
		t.Parallel()

		got, err := decode(flip(len(good)-1), 1<<20)
		require.ErrorIs(t, err, shard.ErrCorrupt)
		assert.Len(t, got, 2)
	})
}

type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }

func TestReaderPassesOnSourceErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection reset")
	_, err := shard.NewReader(failingReader{boom}, 1<<20).Next()
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, shard.ErrCorrupt, "a failing source is not a corrupt shard")
}

func TestCompressRoundTripAndLimits(t *testing.T) {
	t.Parallel()

	plain := encode(t, sample())

	for _, level := range []string{shard.LevelFastest, shard.LevelDefault, shard.LevelBetter, shard.LevelBest} {
		comp, err := shard.NewCompressor(level)
		require.NoError(t, err)

		out, err := shard.Decompress(comp.Compress(plain), int64(len(plain)))
		require.NoError(t, err, level)
		assert.Equal(t, plain, out)
	}

	_, err := shard.NewCompressor("max")
	require.Error(t, err)

	_, err = shard.NewStreamWriter(io.Discard, "max")
	require.Error(t, err)

	comp, err := shard.NewCompressor(shard.LevelDefault)
	require.NoError(t, err)

	_, err = shard.Decompress(comp.Compress(plain), int64(len(plain))-1)
	require.ErrorIs(t, err, shard.ErrTooLarge)

	// A bomb: megabytes of zeros in a few bytes must not be expanded.
	bomb := comp.Compress(make([]byte, 64<<20))
	assert.Less(t, len(bomb), 64<<10)

	_, err = shard.Decompress(bomb, 1<<20)
	require.ErrorIs(t, err, shard.ErrTooLarge)

	_, err = shard.Decompress([]byte("definitely not zstd"), 1<<20)
	require.ErrorIs(t, err, shard.ErrCorrupt)

	var stream bytes.Buffer

	enc, err := shard.NewStreamWriter(&stream, shard.LevelBetter)
	require.NoError(t, err)

	_, err = enc.Write(plain)
	require.NoError(t, err)
	require.NoError(t, enc.Close())

	out, err := shard.Decompress(stream.Bytes(), int64(len(plain)))
	require.NoError(t, err)
	assert.Equal(t, plain, out, "a streamed object reads like a batch one")
}

// FuzzReader: whatever the bytes, the reader returns records or an error —
// it never panics, and never hands back a record that does not re-encode.
func FuzzReader(f *testing.F) {
	f.Add(encode(f, sample()))
	f.Add(shard.AppendHeader(nil))
	f.Add([]byte("EVSH\x01\x00\x00\x00\x0b\x00\x00\x00"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		records, err := decode(data, 1<<16)
		if err != nil && !errors.Is(err, shard.ErrCorrupt) {
			t.Fatalf("an in-memory source can only fail as corrupt: %v", err)
		}

		buf := shard.AppendHeader(nil)

		for _, r := range records {
			var appendErr error

			buf, appendErr = shard.AppendRecord(buf, r)
			if appendErr != nil {
				t.Fatalf("a decoded record must be encodable: %v", appendErr)
			}
		}

		again, err := decode(buf, 1<<16)
		if err != nil || len(again) != len(records) {
			t.Fatalf("re-encoded records must decode: %v (%d of %d)", err, len(again), len(records))
		}
	})
}

// FuzzDecompress: no input makes the decoder exceed its limit or panic.
func FuzzDecompress(f *testing.F) {
	comp, err := shard.NewCompressor(shard.LevelFastest)
	if err != nil {
		f.Fatal(err)
	}

	f.Add(comp.Compress(encode(f, sample())))
	f.Add([]byte{0x28, 0xb5, 0x2f, 0xfd})

	f.Fuzz(func(t *testing.T, data []byte) {
		const limit = 1 << 20

		out, err := shard.Decompress(data, limit)
		if err == nil && len(out) > limit {
			t.Fatalf("decompressed %d bytes past the limit", len(out))
		}
	})
}
