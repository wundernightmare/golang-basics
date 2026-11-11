package archive_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
)

func TestLayoutKeys(t *testing.T) {
	t.Parallel()

	// 14:59:59.999 in UTC+3 is 11:59 UTC: the partition is UTC.
	at := time.Date(2026, 11, 10, 14, 59, 59, 999_000_000, time.FixedZone("msk", 3*3600))
	hour := at.UTC().Truncate(time.Hour)

	for _, prefix := range []string{"events", "events/", "/events/"} {
		l := archive.NewLayout(prefix)

		assert.Equal(t, "events/raw/", l.RawRoot())
		assert.Equal(t, "events/raw/dt=2026-11-10/hour=11/", l.RawHour(at))
		assert.Equal(t, "events/raw/dt=2026-11-10/hour=11/1794311999999-w1-00000007.evt.zst", l.RawKey(at, "w1", 7))
		assert.Equal(t, "events/compacted/dt=2026-11-10/hour=11/_MANIFEST.json", l.ManifestKey(hour))
		assert.Equal(t, "events/compacted/dt=2026-11-10/hour=11/part-abc-00002.evt.zst", l.PartKey(hour, "abc", 2))
		assert.Equal(t, "events/quarantine/raw/dt=2026-11-10/hour=11/x.evt.zst", l.QuarantineKey(l.RawHour(at)+"x.evt.zst"))

		assert.True(t, l.IsPart(l.PartKey(hour, "abc", 2)))
		assert.False(t, l.IsPart(l.ManifestKey(hour)))
	}

	root := archive.NewLayout("")
	assert.Equal(t, "raw/", root.RawRoot())

	// Names sort in time order, then by writer and batch.
	l := archive.NewLayout("")
	assert.Less(t, l.RawKey(at, "w9", 99), l.RawKey(at.Add(time.Millisecond), "w1", 1))
	assert.Less(t, l.RawKey(at, "w1", 2), l.RawKey(at, "w1", 10))
}

func TestParseHour(t *testing.T) {
	t.Parallel()

	l := archive.NewLayout("events")
	hour := time.Date(2026, 11, 10, 23, 0, 0, 0, time.UTC)

	got, ok := archive.ParseHour(l.RawHour(hour))
	require.True(t, ok)
	assert.True(t, hour.Equal(got))

	got, ok = archive.ParseHour(l.RawKey(hour.Add(59*time.Minute), "w", 1))
	require.True(t, ok)
	assert.True(t, hour.Equal(got))

	for _, bad := range []string{"", "events/raw/", "events/raw/dt=2026-11-10/", "events/raw/dt=2026-13-40/hour=99/", "events/raw/dt=x/hour=y/"} {
		_, ok := archive.ParseHour(bad)
		assert.False(t, ok, bad)
	}
}

func TestNewWriterIDIsRandom(t *testing.T) {
	t.Parallel()

	a, err := archive.NewWriterID()
	require.NoError(t, err)

	b, err := archive.NewWriterID()
	require.NoError(t, err)

	assert.Len(t, a, 16)
	assert.NotEqual(t, a, b)
}
