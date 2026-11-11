package integration_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

func TestEveryEventReachesBothSinks(t *testing.T) {
	t.Parallel()

	svc := run(t, direct(t), nil)
	svc.waitReadyz("ready")

	var sent []string

	for range 5 {
		batch := ids(4)
		got := svc.post(batch...)
		require.Equal(t, http.StatusOK, got.status, got.body)
		assert.JSONEq(t, `{"accepted":4}`, got.body)

		sent = append(sent, batch...)
	}

	assert.Positive(t, svc.metric("ingest_batches_total", map[string]string{"outcome": "kafka"}), "Kafka answers in normal operation")

	seen := svc.published(sent, 30*time.Second)
	for _, id := range sent {
		assert.Equal(t, "it.test", seen[id], "event %s in Kafka, keyed by id, with its type header", id)
	}

	svc.waitArchived(sent)

	for id, copies := range svc.archived() {
		assert.Equal(t, 1, copies, "event %s archived once", id)
	}
}

func TestArchiveAloneWithoutKafkaConfigured(t *testing.T) {
	t.Parallel()

	back := direct(t)
	back.brokers = nil

	svc := run(t, back, nil)
	svc.waitReadyz("ready")

	batch := ids(3)
	got := svc.post(batch...)
	require.Equal(t, http.StatusOK, got.status, got.body)
	assert.Equal(t, 1.0, svc.metric("ingest_batches_total", map[string]string{"outcome": "archive"}))
	assert.Equal(t, 0.0, svc.metric("app_integration_enabled", map[string]string{"integration": "kafka"}))

	// Answered means stored: no waiting.
	archived := svc.archived()
	for _, id := range batch {
		assert.Equal(t, 1, archived[id])
	}
}

// Many clients, then SIGTERM in the middle of it. Whatever was answered 200
// is in both sinks once the process has exited: the server drained, the last
// archive batch was flushed, the Kafka sends finished.
func TestShutdownUnderLoadLosesNothingAcknowledged(t *testing.T) {
	t.Parallel()

	svc := run(t, direct(t), nil)
	svc.waitReadyz("ready")

	var (
		mu    sync.Mutex
		acked []string
		wg    sync.WaitGroup
	)

	stopClients := make(chan struct{})

	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stopClients:
					return
				default:
				}

				batch := ids(5)

				got := svc.post(batch...)
				if got.err != nil {
					return // the listener is gone: the client would retry elsewhere
				}

				if got.status == http.StatusOK {
					mu.Lock()

					acked = append(acked, batch...)
					mu.Unlock()
				}
			}
		})
	}

	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, svc.stop(), "a clean shutdown")
	close(stopClients)
	wg.Wait()

	require.NotEmpty(t, acked)
	t.Logf("%d events acknowledged before shutdown", len(acked))
	assert.Contains(t, svc.log.String(), "shutdown: both sinks drained")

	archived := svc.archived()
	seen := svc.published(acked, 60*time.Second)

	var notArchived, notPublished int

	for _, id := range acked {
		if archived[id] == 0 {
			notArchived++
		}

		if _, ok := seen[id]; !ok {
			notPublished++
		}
	}

	assert.Zero(t, notArchived, "acknowledged events missing from the archive")
	assert.Zero(t, notPublished, "acknowledged events missing from Kafka")
}

// The compact role, assembled by the same app.Run: a closed hour of raw
// objects becomes a manifest and a part, and with RUN_ONCE the process ends.
func TestCompactModeRunsOnce(t *testing.T) {
	t.Parallel()

	back := direct(t)
	prefix := testx.Unique("compact-mode") + "/"
	layout := archive.NewLayout(prefix)
	hour := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour)

	store, err := s3.New(context.Background(), s3.Config{
		Bucket: bucket, Endpoint: back.store.Endpoint, Region: back.store.Region,
		AccessKeyID: back.store.AccessKey, SecretAccessKey: back.store.SecretKey,
		UsePathStyle: true, Insecure: true, AutoCreateBucket: true,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	// RustFS reports /health before its S3 API is ready: for the first
	// seconds a signed call can still answer 503 (seen on CI runners).
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, store.Ensure(context.Background()))
	}, time.Minute, 250*time.Millisecond, "the bucket becomes reachable")

	comp, err := shard.NewCompressor(shard.LevelDefault)
	require.NoError(t, err)

	for seq, id := range []string{"a", "b", "a"} {
		buf, err := shard.AppendRecord(shard.AppendHeader(nil), shard.Record{ID: id, ReceivedAt: time.Now(), Event: []byte(`{}`)})
		require.NoError(t, err)
		require.NoError(t, store.Put(context.Background(), layout.RawKey(hour.Add(time.Duration(seq)*time.Minute), "w", uint64(seq)), comp.Compress(buf), shard.ContentType))
	}

	back.brokers = nil
	svc := run(t, back, map[string]string{
		"INGEST_MODE":             "compact",
		"INGEST_ARCHIVE_PREFIX":   prefix,
		"INGEST_COMPACT_RUN_ONCE": "true",
	})

	select {
	case err := <-svc.done:
		svc.done <- err

		require.NoError(t, err, svc.log.String())
	case <-time.After(60 * time.Second):
		require.FailNow(t, "RUN_ONCE did not end the process", svc.log.String())
	}

	assert.Empty(t, svc.srv.ListenAddr(), "a worker: no API listener")
	assert.Equal(t, 2.0, svc.metric("ingest_compaction_events_total", map[string]string{"result": "written"}))
	assert.Equal(t, 1.0, svc.metric("ingest_compaction_events_total", map[string]string{"result": "duplicate"}))

	_, err = store.Head(context.Background(), layout.ManifestKey(hour))
	require.NoError(t, err, "the hour was committed")
}
