package compact_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/s3"
	"github.com/wundernightmare/golang-basics/libs/secret"
	"github.com/wundernightmare/golang-basics/services/ingest/internal/archive"
	"github.com/wundernightmare/golang-basics/services/ingest/internal/compact"
	"github.com/wundernightmare/golang-basics/services/ingest/internal/shard"
)

// A store that fails every call: the pass reports it and counts itself
// failed, and nothing else happens. No container: a healthy store never
// answers like this.
func TestPass_StoreDown(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>down</Message></Error>`))
	}))
	t.Cleanup(srv.Close)

	store, err := s3.New(context.Background(), s3.Config{
		Bucket: "b", Endpoint: srv.URL, Insecure: true, UsePathStyle: true,
		AccessKeyID: "k", SecretAccessKey: secret.New("s"), MaxAttempts: 1, UploadPartBytes: s3.MinUploadPartBytes,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(store.Close)

	compactor, err := compact.New(compact.Config{
		Interval: time.Hour, CloseDelay: 15 * time.Minute, PartBytes: 64 << 20, FetchConcurrency: 1,
		MaxObjectBytes: 1 << 20, Compression: shard.LevelFastest, DeleteSources: true, HourTimeout: time.Minute,
	}, store, archive.NewLayout("down"), nil)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	reg.MustRegister(compactor.Collectors()...)

	require.ErrorContains(t, compactor.Pass(context.Background()), "s3: list prefixes")
	assert.InDelta(t, 1.0, metric(t, reg, "ingest_compaction_passes_total", "failed"), 0)
}

// A bucket that disappears between the listing and the manifest read fails
// the hour: "no such bucket" is not "this hour has no manifest yet", which
// would start the hour from nothing.
func TestPass_MissingBucketIsNotAMissingManifest(t *testing.T) {
	t.Parallel()

	const day = "gone/raw/dt=2020-01-01/"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")

		query := r.URL.Query()

		switch {
		case !query.Has("list-type"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>gone</Message></Error>`))
		case query.Get("prefix") == day:
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><CommonPrefixes><Prefix>` + day + `hour=00/</Prefix></CommonPrefixes></ListBucketResult>`))
		case query.Has("delimiter"):
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><CommonPrefixes><Prefix>` + day + `</Prefix></CommonPrefixes></ListBucketResult>`))
		default:
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult></ListBucketResult>`))
		}
	}))
	t.Cleanup(srv.Close)

	store, err := s3.New(context.Background(), s3.Config{
		Bucket: "b", Endpoint: srv.URL, Insecure: true, UsePathStyle: true,
		AccessKeyID: "k", SecretAccessKey: secret.New("s"), MaxAttempts: 1, UploadPartBytes: s3.MinUploadPartBytes,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(store.Close)

	compactor, err := compact.New(compact.Config{
		Interval: time.Hour, CloseDelay: 15 * time.Minute, PartBytes: 64 << 20, FetchConcurrency: 1,
		MaxObjectBytes: 1 << 20, Compression: shard.LevelFastest, DeleteSources: true, HourTimeout: time.Minute,
	}, store, archive.NewLayout("gone"), nil)
	require.NoError(t, err)

	require.ErrorIs(t, compactor.Pass(context.Background()), s3.ErrBucketNotFound)
}

// Something that is not an hour under a day of the raw archive is skipped
// with a warning; the real hour next to it is compacted as usual.
func TestPass_IgnoresUnexpectedPrefix(t *testing.T) {
	t.Parallel()

	fix := newFixture(t)
	fix.putRaw(fix.hour, record("a"))

	stray := fix.layout.RawHour(fix.hour)
	stray = stray[:len(stray)-len("hour=00/")] + "lost+found/file"
	require.NoError(t, fix.store.Put(context.Background(), stray, []byte("x"), "text/plain"))

	compactor, _ := fix.compactor()
	require.NoError(t, compactor.Pass(context.Background()))
	assert.Equal(t, int64(1), fix.manifest(fix.hour).Events)

	size, err := fix.store.Head(context.Background(), stray)
	require.NoError(t, err, "the stray object is left alone")
	assert.Equal(t, int64(1), size)
}
