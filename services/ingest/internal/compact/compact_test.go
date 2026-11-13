// The compactor is tested against a real object store (RustFS): its
// correctness is the order of puts, lists and deletes on S3, which a fake
// would only restate. Each test owns a key prefix. Skipped under -short.
package compact_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/compact"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/testsupport"
)

func TestMain(m *testing.M) { os.Exit(containers.Main(m)) }

type fixture struct {
	t      *testing.T
	store  *s3.Client
	layout archive.Layout
	hour   time.Time // a closed hour
	cfg    compact.Config
	seq    uint64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	backend := containers.S3(t)
	store, err := s3.New(context.Background(), s3.Config{
		Bucket: "ingest-compact-test", Endpoint: backend.Endpoint, Region: backend.Region,
		AccessKeyID: backend.AccessKey, SecretAccessKey: secret.New(backend.SecretKey),
		UsePathStyle: true, Insecure: true, AutoCreateBucket: true,
		UploadPartBytes: s3.MinUploadPartBytes,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	// RustFS reports /health before its S3 API is ready: for the first
	// seconds a signed call can still answer 503 (seen on CI runners).
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, store.Ensure(context.Background()))
	}, time.Minute, 250*time.Millisecond, "the bucket becomes reachable")

	return &fixture{
		t: t, store: store, layout: archive.NewLayout(testx.Unique("compact")),
		hour: time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour),
		cfg: compact.Config{
			Interval: time.Hour, CloseDelay: 15 * time.Minute, PartBytes: 64 << 20, FetchConcurrency: 3,
			MaxObjectBytes: 32 << 20, Compression: shard.LevelFastest, DeleteSources: true, HourTimeout: time.Minute,
		},
	}
}

func (fix *fixture) compactor(mutate ...func(*compact.Config)) (*compact.Compactor, *prometheus.Registry) {
	fix.t.Helper()

	cfg := fix.cfg
	for _, apply := range mutate {
		apply(&cfg)
	}

	compactor, err := compact.New(cfg, fix.store, fix.layout, nil)
	require.NoError(fix.t, err)

	reg := prometheus.NewRegistry()
	reg.MustRegister(compactor.Collectors()...)

	return compactor, reg
}

func record(id string) shard.Record {
	return shard.Record{ID: id, Type: "t.est", ReceivedAt: time.Now(), Event: []byte(`{"id":"` + id + `","payload":{}}`)}
}

// putRaw stores one raw object in hour, as an ingest pod would, and returns its key.
func (fix *fixture) putRaw(hour time.Time, records ...shard.Record) string {
	fix.t.Helper()

	buf := shard.AppendHeader(nil)

	for _, rec := range records {
		var err error

		buf, err = shard.AppendRecord(buf, rec)
		require.NoError(fix.t, err)
	}

	comp, err := shard.NewCompressor(shard.LevelFastest)
	require.NoError(fix.t, err)

	fix.seq++
	key := fix.layout.RawKey(hour.Add(time.Duration(fix.seq)*time.Second), "w1", fix.seq)
	require.NoError(fix.t, fix.store.Put(context.Background(), key, comp.Compress(buf), shard.ContentType))

	return key
}

func (fix *fixture) keys(prefix string) []string {
	fix.t.Helper()

	var out []string

	for obj, err := range fix.store.List(context.Background(), prefix) {
		require.NoError(fix.t, err)

		out = append(out, obj.Key)
	}

	return out
}

func (fix *fixture) manifest(hour time.Time) compact.Manifest {
	fix.t.Helper()

	raw, err := fix.store.Get(context.Background(), fix.layout.ManifestKey(hour), 1<<20)
	require.NoError(fix.t, err)

	var manifest compact.Manifest
	require.NoError(fix.t, json.Unmarshal(raw, &manifest))

	return manifest
}

// partIDs reads the manifest's parts back and returns the event ids in order.
func (fix *fixture) partIDs(manifest compact.Manifest) []string {
	fix.t.Helper()

	var ids []string

	for _, part := range manifest.Parts {
		object, err := fix.store.Get(context.Background(), part.Key, 256<<20)
		require.NoError(fix.t, err)
		assert.Equal(fix.t, int64(len(object)), part.Bytes, "the manifest records the stored size")

		records, err := testsupport.DecodeObject(object)
		require.NoError(fix.t, err)
		assert.Len(fix.t, records, int(part.Events))

		for _, rec := range records {
			ids = append(ids, rec.ID)
		}
	}

	return ids
}

func metric(t *testing.T, reg *prometheus.Registry, name, result string) float64 {
	t.Helper()

	var labels map[string]string
	if result != "" {
		labels = map[string]string{"result": result}
	}

	return testx.Metric(t, reg, name, labels)
}

func TestPass_MergesDeduplicatesCommitsAndDeletes(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("a"), record("b"))
	fix.putRaw(fix.hour, record("c"), record("a")) // a retried request: "a" again
	fix.putRaw(fix.hour, record("d"))

	open := time.Now().UTC().Truncate(time.Hour) // still being written
	openKey := fix.putRaw(open, record("z"))

	compactor, reg := fix.compactor()
	require.NoError(t, compactor.Pass(context.Background()))

	manifest := fix.manifest(fix.hour)
	assert.Equal(t, compact.ManifestVersion, manifest.Version)
	assert.True(t, fix.hour.Equal(manifest.Hour))
	require.Len(t, manifest.Parts, 1)
	assert.Equal(t, int64(4), manifest.Events)
	assert.Equal(t, int64(1), manifest.Duplicates)
	assert.Empty(t, manifest.Sources, "the merged raw objects are gone, so nothing is left to skip")
	assert.Equal(t, []string{"a", "b", "c", "d"}, fix.partIDs(manifest), "first occurrence wins, in arrival order")

	// The records survive whole: type and the event's bytes.
	object, err := fix.store.Get(context.Background(), manifest.Parts[0].Key, 1<<20)
	require.NoError(t, err)

	records, err := testsupport.DecodeObject(object)
	require.NoError(t, err)
	assert.Equal(t, "t.est", records[0].Type)
	assert.JSONEq(t, `{"id":"a","payload":{}}`, string(records[0].Event))

	assert.Empty(t, fix.keys(fix.layout.RawHour(fix.hour)), "the raw objects of the hour were deleted after the commit")
	assert.Equal(t, []string{openKey}, fix.keys(fix.layout.RawHour(open)), "an hour that is not closed is not touched")
	assert.Empty(t, fix.keys(fix.layout.CompactedHour(open)))

	assert.InDelta(t, 4.0, metric(t, reg, "ingest_compaction_events_total", "written"), 0)
	assert.InDelta(t, 1.0, metric(t, reg, "ingest_compaction_events_total", "duplicate"), 0)
	assert.InDelta(t, 3.0, metric(t, reg, "ingest_compaction_objects_total", "merged"), 0)
	assert.InDelta(t, 1.0, metric(t, reg, "ingest_compaction_passes_total", "ok"), 0)
	assert.Positive(t, metric(t, reg, "ingest_compaction_last_success_timestamp_seconds", ""))
	testx.LintMetrics(t, reg)

	// A second pass finds nothing to do and changes nothing.
	before := fix.keys(fix.layout.CompactedHour(fix.hour))

	require.NoError(t, compactor.Pass(context.Background()))
	assert.Equal(t, before, fix.keys(fix.layout.CompactedHour(fix.hour)))
	assert.Equal(t, manifest.Parts, fix.manifest(fix.hour).Parts)
}

func TestPass_CutsPartsAtTheTargetSize(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)

	// Incompressible events, so the stored size tracks what was written.
	const events = 8

	for i := range events {
		noise := make([]byte, 1<<20)
		_, err := rand.Read(noise)
		require.NoError(t, err)

		fix.putRaw(fix.hour, shard.Record{ID: fmt.Sprintf("big-%d", i), ReceivedAt: time.Now(), Event: noise})
	}

	compactor, _ := fix.compactor(func(cfg *compact.Config) { cfg.PartBytes = 3 << 20 })
	require.NoError(t, compactor.Pass(context.Background()))

	manifest := fix.manifest(fix.hour)
	assert.Greater(t, len(manifest.Parts), 1, "more than one part")
	assert.Equal(t, int64(events), manifest.Events)

	var total int64

	for _, part := range manifest.Parts {
		total += part.Events

		size, err := fix.store.Head(context.Background(), part.Key)
		require.NoError(t, err)
		assert.Equal(t, part.Bytes, size)
	}

	assert.Equal(t, int64(events), total)
}

// The crash window between the commit and the delete: the manifest lists the
// merged raw objects, a reader skips them, and the next run finishes the job
// without merging them twice.
func TestPass_FinishesAnInterruptedDelete(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	first := fix.putRaw(fix.hour, record("a"))
	second := fix.putRaw(fix.hour, record("b"))

	keep, _ := fix.compactor(func(cfg *compact.Config) { cfg.DeleteSources = false })
	require.NoError(t, keep.Pass(context.Background()))

	committed := fix.manifest(fix.hour)
	assert.ElementsMatch(t, []string{first, second}, committed.Sources, "committed, not yet deleted")
	assert.Len(t, fix.keys(fix.layout.RawHour(fix.hour)), 2)

	require.NoError(t, keep.Pass(context.Background()))

	again := fix.manifest(fix.hour)
	assert.Equal(t, committed.Parts, again.Parts, "already merged objects are not merged again")
	assert.Equal(t, int64(2), again.Events)

	finish, _ := fix.compactor()
	require.NoError(t, finish.Pass(context.Background()))

	done := fix.manifest(fix.hour)
	assert.Empty(t, done.Sources)
	assert.Equal(t, committed.Parts, done.Parts)
	assert.Empty(t, fix.keys(fix.layout.RawHour(fix.hour)))
	assert.Equal(t, []string{"a", "b"}, fix.partIDs(done))
}

func TestPass_LateObjectBecomesAnotherPart(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("a"))

	compactor, _ := fix.compactor()
	require.NoError(t, compactor.Pass(context.Background()))

	first := fix.manifest(fix.hour)
	require.Len(t, first.Parts, 1)

	// A put that outlived the close delay lands after the hour was compacted.
	fix.putRaw(fix.hour, record("late"))
	require.NoError(t, compactor.Pass(context.Background()))

	second := fix.manifest(fix.hour)
	require.Len(t, second.Parts, 2)
	assert.Equal(t, first.Parts[0], second.Parts[0], "the first run's part is untouched")
	assert.NotEqual(t, first.Parts[0].Key, second.Parts[1].Key)
	assert.Equal(t, int64(2), second.Events)
	assert.Equal(t, []string{"a", "late"}, fix.partIDs(second))
	assert.Empty(t, fix.keys(fix.layout.RawHour(fix.hour)))
}

func TestPass_QuarantinesWhatItCannotRead(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("good"))

	garbage := fix.layout.RawKey(fix.hour.Add(30*time.Minute), "w2", 1)
	require.NoError(t, fix.store.Put(context.Background(), garbage, []byte("not a zstd stream"), shard.ContentType))

	// A valid zstd stream over a shard cut mid-record.
	whole, err := shard.AppendRecord(shard.AppendHeader(nil), record("half"))
	require.NoError(t, err)

	comp, err := shard.NewCompressor(shard.LevelFastest)
	require.NoError(t, err)

	truncated := fix.layout.RawKey(fix.hour.Add(40*time.Minute), "w2", 2)
	require.NoError(t, fix.store.Put(context.Background(), truncated, comp.Compress(whole[:len(whole)-3]), shard.ContentType))

	compactor, reg := fix.compactor()
	require.NoError(t, compactor.Pass(context.Background()), "a damaged object does not fail the hour")

	assert.Equal(t, []string{"good"}, fix.partIDs(fix.manifest(fix.hour)))
	assert.Empty(t, fix.keys(fix.layout.RawHour(fix.hour)), "the good object was merged and deleted, the damaged ones moved")

	for _, key := range []string{garbage, truncated} {
		moved, err := fix.store.Get(context.Background(), fix.layout.QuarantineKey(key), 1<<20)
		require.NoError(t, err, "the damaged object is kept for a human")
		assert.NotEmpty(t, moved)
	}

	assert.InDelta(t, 2.0, metric(t, reg, "ingest_compaction_objects_total", "quarantined"), 0)
}

func TestPass_ObjectOverTheLimitStaysInPlace(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("small"))

	noise := make([]byte, 64<<10)
	_, err := rand.Read(noise)
	require.NoError(t, err)

	huge := fix.putRaw(fix.hour, shard.Record{ID: "huge", ReceivedAt: time.Now(), Event: noise})

	compactor, _ := fix.compactor(func(cfg *compact.Config) { cfg.MaxObjectBytes = 16 << 10 })
	require.NoError(t, compactor.Pass(context.Background()))

	assert.Equal(t, []string{"small"}, fix.partIDs(fix.manifest(fix.hour)))
	assert.Equal(t, []string{huge}, fix.keys(fix.layout.RawHour(fix.hour)), "too large to read: reported and left for an operator")
}

func TestPass_SweepsOldOrphanParts(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("a"))

	orphan := fix.layout.PartKey(fix.hour, "deadbeef0000", 0)
	require.NoError(t, fix.store.Put(context.Background(), orphan, []byte("left by a run that died before its commit"), shard.ContentType))

	compactor, _ := fix.compactor(func(cfg *compact.Config) { cfg.HourTimeout = time.Second })
	require.NoError(t, compactor.Pass(context.Background()))
	assert.Contains(t, fix.keys(fix.layout.CompactedHour(fix.hour)), orphan, "young: a run could still be about to commit it")

	time.Sleep(2500 * time.Millisecond)
	fix.putRaw(fix.hour, record("b")) // gives the hour something to do again
	require.NoError(t, compactor.Pass(context.Background()))

	manifest := fix.manifest(fix.hour)
	left := fix.keys(fix.layout.CompactedHour(fix.hour))
	assert.NotContains(t, left, orphan, "old and unreferenced: swept")
	assert.Len(t, left, len(manifest.Parts)+1, "the manifest and its parts stay")
	assert.Equal(t, []string{"a", "b"}, fix.partIDs(manifest))
}

func TestPass_BrokenManifestStopsTheHourBeforeAnythingIsDeleted(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	raw := fix.putRaw(fix.hour, record("a"))
	require.NoError(t, fix.store.Put(context.Background(), fix.layout.ManifestKey(fix.hour), []byte("{not json"), "application/json"))

	other := fix.hour.Add(-time.Hour)
	fix.putRaw(other, record("b"))

	compactor, reg := fix.compactor()
	require.ErrorContains(t, compactor.Pass(context.Background()), "not JSON")

	assert.Equal(t, []string{raw}, fix.keys(fix.layout.RawHour(fix.hour)), "nothing of the failed hour was touched")
	assert.Equal(t, []string{"b"}, fix.partIDs(fix.manifest(other)), "one failed hour does not stop the others")
	assert.InDelta(t, 1.0, metric(t, reg, "ingest_compaction_partitions_total", "failed"), 0)
	assert.InDelta(t, 1.0, metric(t, reg, "ingest_compaction_partitions_total", "ok"), 0)
	assert.Positive(t, metric(t, reg, "ingest_compaction_oldest_failed_hour_age_seconds", ""))

	future := []byte(`{"version": 99, "parts": []}`)
	require.NoError(t, fix.store.Put(context.Background(), fix.layout.ManifestKey(fix.hour), future, "application/json"))
	require.ErrorContains(t, compactor.Pass(context.Background()), "version 99")
}

func TestRun_OnceAndLoop(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("a"))

	once, _ := fix.compactor(func(cfg *compact.Config) { cfg.RunOnce = true })
	require.NoError(t, once.Run(context.Background()), "RUN_ONCE returns after one pass")
	assert.Equal(t, []string{"a"}, fix.partIDs(fix.manifest(fix.hour)))

	loop, reg := fix.compactor(func(cfg *compact.Config) { cfg.Interval = 50 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- loop.Run(ctx) }()

	require.Eventually(t, func() bool { return metric(t, reg, "ingest_compaction_passes_total", "ok") >= 2 },
		10*time.Second, 20*time.Millisecond, "the loop keeps passing")

	cancel()
	require.NoError(t, <-done)
}

func TestNew_RejectsBadConfig(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)

	for name, mutate := range map[string]func(*compact.Config){
		"compression": func(c *compact.Config) { c.Compression = "zip" },
		"interval":    func(c *compact.Config) { c.Interval = 0 },
		"part bytes":  func(c *compact.Config) { c.PartBytes = 0 },
		"concurrency": func(c *compact.Config) { c.FetchConcurrency = 0 },
	} {
		cfg := fix.cfg
		mutate(&cfg)

		_, err := compact.New(cfg, fix.store, fix.layout, nil)
		require.Error(t, err, name)
	}
}
