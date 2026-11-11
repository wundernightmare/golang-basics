// Package compact merges the many small objects the ingest pods write into a
// few large ones, hour by hour, and removes duplicate events on the way.
//
// An hour is compacted once it is closed (see archive.Layout: a fixed delay
// after it ends). For one hour, a run:
//
//  1. lists the raw objects and sets aside those an earlier run already
//     merged (the manifest's sources);
//  2. downloads the rest — several at once, consumed in key order — verifies
//     each completely (zstd, every record's checksum) before using any of
//     it, and moves an object it cannot read to quarantine/;
//  3. streams the records, minus events whose id was already seen in this
//     run, into parts of about PartBytes through multipart uploads: memory is
//     a few raw objects plus the set of ids, whatever the hour's size;
//  4. writes the manifest listing the parts and the merged raw objects. This
//     is the commit: until it exists the hour is simply not compacted, and a
//     reader uses the raw objects;
//  5. deletes the merged raw objects and clears them from the manifest.
//
// Every step can be repeated. Part names carry a digest of the set of raw
// objects the run read, so a run that died is redone into the same keys, and
// a run over a different set (an object arrived late) never touches the
// parts of another. A crash between 4 and 5 is finished by the next run from
// the manifest. Parts no manifest refers to — from a run that died before 4
// and was redone over a different set — are swept once they are old enough
// that no run can still be writing them.
//
// How to read an hour: if compacted/…/_MANIFEST.json exists, the events are
// its parts plus any raw object not among its sources; otherwise they are the
// raw objects.
//
// Compaction must run as one instance (a one-replica Deployment with the
// Recreate strategy, or a CronJob with concurrencyPolicy: Forbid): the
// manifest is read, extended and written back without a lock. Two instances
// over the same listing are harmless (same keys, same bytes); over different
// listings the later manifest write wins, which the delete step re-checks
// before it removes anything.
//
// Not deduplicated: an event sent twice into different hours, or into the
// same hour but merged by different runs. Consumers of the archive
// deduplicate by event id, as consumers of Kafka do.
package compact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/tracehubmmp/golang-basics/libs/s3"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/archive"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

// Config tunes a [Compactor] (see config.CompactConfig for the keys).
type Config struct {
	Interval         time.Duration
	RunOnce          bool
	CloseDelay       time.Duration
	PartBytes        int64
	FetchConcurrency int
	MaxObjectBytes   int64
	Compression      string
	DeleteSources    bool
	HourTimeout      time.Duration
}

// ManifestVersion is the manifest format this package writes.
const ManifestVersion = 1

// Manifest is the commit record of one compacted hour
// (compacted/dt=…/hour=…/_MANIFEST.json).
type Manifest struct {
	Version int `json:"version"`
	// Hour is the start of the hour, RFC 3339 UTC.
	Hour time.Time `json:"hour"`
	// Parts hold the hour's events, in the order they were written.
	Parts []Part `json:"parts"`
	// Events and Duplicates are totals over every run.
	Events     int64 `json:"events"`
	Duplicates int64 `json:"duplicates"`
	// Sources are raw objects whose events are in Parts and which still
	// exist: a reader skips them, the next run deletes them.
	Sources   []string  `json:"sources,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Part is one compacted object.
type Part struct {
	Key    string `json:"key"`
	Events int64  `json:"events"`
	// Bytes is the stored (compressed) size.
	Bytes int64 `json:"bytes"`
}

// Compactor compacts closed hours of one archive.
type Compactor struct {
	cfg     Config
	store   *s3.Client
	layout  archive.Layout
	log     *slog.Logger
	metrics *metrics
	now     func() time.Time
}

// New builds a compactor over store.
func New(cfg Config, store *s3.Client, layout archive.Layout, log *slog.Logger) (*Compactor, error) {
	if _, err := shard.ParseLevel(cfg.Compression); err != nil {
		return nil, err
	}

	if cfg.Interval <= 0 || cfg.CloseDelay <= 0 || cfg.PartBytes < 1 || cfg.FetchConcurrency < 1 || cfg.MaxObjectBytes < 1 || cfg.HourTimeout <= 0 {
		return nil, fmt.Errorf("compact: config: intervals, sizes and concurrency must be positive: %+v", cfg)
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Compactor{cfg: cfg, store: store, layout: layout, log: log, metrics: newMetrics(), now: time.Now}, nil
}

// Run compacts every closed hour now and then every Interval until ctx is
// cancelled. With RunOnce it returns after the first pass, with that pass's
// error. A failed hour does not stop the pass, and a failed pass does not
// stop the loop: the next one starts from the store's state.
func (c *Compactor) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	for {
		err := c.Pass(ctx)
		if c.cfg.RunOnce {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Pass compacts every closed hour that has raw objects, oldest first.
func (c *Compactor) Pass(ctx context.Context) error {
	hours, err := c.closedHours(ctx)
	if err != nil {
		c.metrics.passes.WithLabelValues(resultFailed).Inc()
		c.log.ErrorContext(ctx, "compaction pass failed: cannot list the archive", "err", err)

		return err
	}

	var (
		errs    []error
		pending time.Time // the oldest hour this pass left uncompacted
	)

	for _, hour := range hours {
		if ctx.Err() != nil {
			break
		}

		if err := c.hour(ctx, hour); err != nil {
			errs = append(errs, fmt.Errorf("hour %s: %w", hour.Format(time.RFC3339), err))

			c.metrics.hours.WithLabelValues(resultFailed).Inc()
			c.log.ErrorContext(ctx, "hour not compacted: it is retried on the next pass", "hour", hour, "err", err)

			if pending.IsZero() {
				pending = hour
			}

			continue
		}

		c.metrics.hours.WithLabelValues(resultOK).Inc()
	}

	c.metrics.oldestPending.Set(0)

	if !pending.IsZero() {
		c.metrics.oldestPending.Set(c.now().Sub(pending).Seconds())
	}

	if err := errors.Join(errs...); err != nil {
		c.metrics.passes.WithLabelValues(resultFailed).Inc()
		return err
	}

	c.metrics.passes.WithLabelValues(resultOK).Inc()
	c.metrics.lastSuccess.SetToCurrentTime()

	return nil
}

// closedHours lists the hours with raw objects that are closed, in order.
func (c *Compactor) closedHours(ctx context.Context) ([]time.Time, error) {
	days, err := c.store.ListPrefixes(ctx, c.layout.RawRoot())
	if err != nil {
		return nil, err
	}

	cutoff := c.now().Add(-c.cfg.CloseDelay)

	var hours []time.Time

	for _, day := range days {
		prefixes, err := c.store.ListPrefixes(ctx, day)
		if err != nil {
			return nil, err
		}

		for _, prefix := range prefixes {
			hour, ok := archive.ParseHour(prefix)
			if !ok {
				c.log.WarnContext(ctx, "unexpected prefix in the raw archive: ignored", "prefix", prefix)
				continue
			}

			if !hour.Add(time.Hour).After(cutoff) {
				hours = append(hours, hour)
			}
		}
	}

	slices.SortFunc(hours, time.Time.Compare)

	return hours, nil
}

// hour brings one hour to its compacted state.
func (c *Compactor) hour(ctx context.Context, hour time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.HourTimeout)
	defer cancel()

	manifest, err := c.loadManifest(ctx, hour)
	if err != nil {
		return err
	}

	var fresh []s3.Object

	for obj, err := range c.store.List(ctx, c.layout.RawHour(hour)) {
		if err != nil {
			return err
		}

		if !slices.Contains(manifest.Sources, obj.Key) {
			fresh = append(fresh, obj)
		}
	}

	if len(fresh) > 0 {
		if err := c.merge(ctx, hour, &manifest, fresh); err != nil {
			return err
		}
	}

	if err := c.deleteSources(ctx, hour, &manifest); err != nil {
		return err
	}

	return c.sweepOrphans(ctx, hour, manifest)
}

func (c *Compactor) loadManifest(ctx context.Context, hour time.Time) (Manifest, error) {
	const maxManifestBytes = 64 << 20

	raw, err := c.store.Get(ctx, c.layout.ManifestKey(hour), maxManifestBytes)
	if errors.Is(err, s3.ErrNotFound) {
		return Manifest{Version: ManifestVersion, Hour: hour}, nil
	}

	if err != nil {
		return Manifest{}, err
	}

	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("compact: manifest of %s is not JSON: %w", hour.Format(time.RFC3339), err)
	}

	if manifest.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("compact: manifest of %s has version %d, this build knows %d", hour.Format(time.RFC3339), manifest.Version, ManifestVersion)
	}

	return manifest, nil
}

func (c *Compactor) saveManifest(ctx context.Context, hour time.Time, manifest *Manifest) error {
	manifest.UpdatedAt = c.now().UTC()

	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("compact: encode manifest: %w", err)
	}

	return c.store.Put(ctx, c.layout.ManifestKey(hour), raw, "application/json")
}

// merge writes the events of the fresh raw objects into new parts and
// commits them to the manifest.
func (c *Compactor) merge(ctx context.Context, hour time.Time, manifest *Manifest, fresh []s3.Object) error {
	start := c.now()
	out := &partWriter{c: c, ctx: ctx, hour: hour, run: runID(fresh)}

	defer out.abort()

	var (
		seen       = make(map[string]struct{})
		merged     []string
		duplicates int64
	)

	err := c.fetchInOrder(ctx, fresh, func(obj s3.Object, data []byte, readErr error) error {
		if readErr != nil {
			return c.quarantine(ctx, obj, data, readErr)
		}

		reader := shard.NewReader(bytes.NewReader(data), len(data))

		for {
			record, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return fmt.Errorf("compact: %q changed between verification and use: %w", obj.Key, err)
			}

			if _, dup := seen[record.ID]; dup {
				duplicates++
				continue
			}

			seen[record.ID] = struct{}{}

			if err := out.write(record); err != nil {
				return err
			}
		}

		merged = append(merged, obj.Key)

		return nil
	})
	if err != nil {
		return err
	}

	if err := out.finish(); err != nil {
		return err
	}

	manifest.Parts = append(manifest.Parts, out.parts...)
	manifest.Events += out.events
	manifest.Duplicates += duplicates
	manifest.Sources = append(manifest.Sources, merged...)

	if err := c.saveManifest(ctx, hour, manifest); err != nil {
		return err
	}

	c.metrics.events.WithLabelValues(resultWritten).Add(float64(out.events))
	c.metrics.events.WithLabelValues(resultDuplicate).Add(float64(duplicates))
	c.metrics.objects.WithLabelValues(resultMerged).Add(float64(len(merged)))
	c.log.InfoContext(ctx, "hour compacted", "hour", hour, "objects", len(merged), "parts", len(out.parts),
		"events", out.events, "duplicates", duplicates, "took", c.now().Sub(start).String())

	return nil
}

// runID identifies the set of raw objects a run reads.
func runID(objects []s3.Object) string {
	digest := sha256.New()

	for _, obj := range objects {
		digest.Write([]byte(obj.Key))
		digest.Write([]byte{0})
	}

	return hex.EncodeToString(digest.Sum(nil))[:12]
}

// fetchInOrder downloads objects FetchConcurrency at a time and hands them to
// use in listing order. A slot is taken before a download starts and given
// back after use returns, so at most FetchConcurrency objects are in memory.
// use gets a non-nil readErr for an object that is not a readable shard; an
// error talking to the store ends the run instead.
func (c *Compactor) fetchInOrder(ctx context.Context, objects []s3.Object, use func(obj s3.Object, data []byte, readErr error) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slots := make(chan struct{}, c.cfg.FetchConcurrency)
	results := make(chan chan fetchResult, c.cfg.FetchConcurrency)

	go func() {
		defer close(results)

		for _, obj := range objects {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}

			result := make(chan fetchResult, 1)

			select {
			case results <- result:
			case <-ctx.Done():
				return
			}

			go func() { result <- c.fetch(ctx, obj) }()
		}
	}()

	for result := range results {
		var got fetchResult

		select {
		case got = <-result:
		case <-ctx.Done():
			return ctx.Err()
		}

		if got.storeErr != nil {
			return got.storeErr
		}

		if err := use(got.obj, got.data, got.err); err != nil {
			return err
		}

		<-slots
	}

	return ctx.Err()
}

// fetchResult is one raw object, downloaded and fully verified — or why not.
type fetchResult struct {
	obj      s3.Object
	data     []byte // the decompressed shard; the stored bytes when err is set and they could be read
	err      error  // the object is not a readable shard
	storeErr error  // the store failed: retry the hour later
}

func (c *Compactor) fetch(ctx context.Context, obj s3.Object) fetchResult {
	stored, err := c.store.Get(ctx, obj.Key, c.cfg.MaxObjectBytes)

	switch {
	case errors.Is(err, s3.ErrTooLarge):
		return fetchResult{obj: obj, err: err}
	case err != nil:
		return fetchResult{obj: obj, storeErr: err}
	}

	data, err := shard.Decompress(stored, c.cfg.MaxObjectBytes)
	if err != nil {
		return fetchResult{obj: obj, data: stored, err: err}
	}

	// Read it through once: an object is used whole or not at all, so half
	// of a damaged one never reaches a part.
	reader := shard.NewReader(bytes.NewReader(data), len(data))

	for {
		_, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return fetchResult{obj: obj, data: data}
		}

		if err != nil {
			return fetchResult{obj: obj, data: stored, err: err}
		}
	}
}

// quarantine moves a raw object that cannot be read out of the way: it would
// fail every pass, and deleting it would destroy evidence — and the events,
// if the damage turns out to be recoverable. An object too large to download
// stays where it is and is reported on every pass.
func (c *Compactor) quarantine(ctx context.Context, obj s3.Object, stored []byte, cause error) error {
	c.metrics.objects.WithLabelValues(resultQuarantined).Inc()

	if stored == nil {
		c.log.ErrorContext(ctx, "raw object not compacted and left in place: raise COMPACT_MAX_OBJECT_BYTES or handle it by hand",
			"key", obj.Key, "bytes", obj.Size, "err", cause)

		return nil
	}

	target := c.layout.QuarantineKey(obj.Key)
	if err := c.store.Put(ctx, target, stored, shard.ContentType); err != nil {
		return err
	}

	if err := c.store.Delete(ctx, []string{obj.Key}); err != nil {
		return err
	}

	c.log.ErrorContext(ctx, "raw object is not a readable shard: moved to quarantine, its events are not in the compacted hour",
		"key", obj.Key, "quarantine_key", target, "bytes", obj.Size, "err", cause)

	return nil
}

// deleteSources removes the raw objects the manifest says are merged — after
// checking, against the store, that the manifest still says so and that
// every part is there. This is the only place events are deleted.
func (c *Compactor) deleteSources(ctx context.Context, hour time.Time, manifest *Manifest) error {
	if !c.cfg.DeleteSources || len(manifest.Sources) == 0 {
		return nil
	}

	stored, err := c.loadManifest(ctx, hour)
	if err != nil {
		return err
	}

	if !slices.Equal(stored.Parts, manifest.Parts) || !slices.Equal(stored.Sources, manifest.Sources) {
		return errors.New("compact: the manifest changed underneath this run (another compactor?): nothing deleted")
	}

	for _, part := range manifest.Parts {
		size, err := c.store.Head(ctx, part.Key)
		if err != nil {
			return fmt.Errorf("compact: part %q of the manifest cannot be confirmed: nothing deleted: %w", part.Key, err)
		}

		if size != part.Bytes {
			return fmt.Errorf("compact: part %q is %d bytes, the manifest says %d: nothing deleted", part.Key, size, part.Bytes)
		}
	}

	if err := c.store.Delete(ctx, manifest.Sources); err != nil {
		return err
	}

	manifest.Sources = nil

	return c.saveManifest(ctx, hour, manifest)
}

// sweepOrphans deletes parts no manifest refers to, once they are older than
// two hour budgets: no run can still be about to commit them.
func (c *Compactor) sweepOrphans(ctx context.Context, hour time.Time, manifest Manifest) error {
	referenced := make(map[string]struct{}, len(manifest.Parts))
	for _, part := range manifest.Parts {
		referenced[part.Key] = struct{}{}
	}

	cutoff := c.now().Add(-2 * c.cfg.HourTimeout)

	var orphans []string

	for obj, err := range c.store.List(ctx, c.layout.CompactedHour(hour)) {
		if err != nil {
			return err
		}

		if _, ok := referenced[obj.Key]; !ok && c.layout.IsPart(obj.Key) && obj.LastModified.Before(cutoff) {
			orphans = append(orphans, obj.Key)
		}
	}

	if len(orphans) == 0 {
		return nil
	}

	c.log.WarnContext(ctx, "deleting compacted parts no manifest refers to (left by an interrupted run)",
		"hour", hour, "parts", strings.Join(orphans, ","))

	return c.store.Delete(ctx, orphans)
}

// partWriter streams records into parts of about PartBytes.
type partWriter struct {
	c    *Compactor
	ctx  context.Context //nolint:containedctx // lives for one merge call, which owns the context
	hour time.Time
	run  string

	upload  *s3.Upload
	enc     *zstd.Encoder
	scratch []byte
	current Part

	parts  []Part
	events int64
}

func (w *partWriter) write(record shard.Record) error {
	if w.upload == nil {
		key := w.c.layout.PartKey(w.hour, w.run, len(w.parts))
		w.upload = w.c.store.NewUpload(w.ctx, key, shard.ContentType)

		enc, err := shard.NewStreamWriter(w.upload, w.c.cfg.Compression)
		if err != nil {
			return err
		}

		w.enc = enc
		w.current = Part{Key: key}

		if _, err := w.enc.Write(shard.AppendHeader(nil)); err != nil {
			return err
		}
	}

	var err error

	w.scratch, err = shard.AppendRecord(w.scratch[:0], record)
	if err != nil {
		return err
	}

	if _, err := w.enc.Write(w.scratch); err != nil {
		return err
	}

	w.current.Events++

	// The upload has seen what the encoder flushed so far: close enough to
	// the final size to cut parts by.
	if w.upload.Size() >= w.c.cfg.PartBytes {
		return w.closePart()
	}

	return nil
}

func (w *partWriter) closePart() error {
	if err := w.enc.Close(); err != nil {
		return err
	}

	if err := w.upload.Close(); err != nil {
		return err
	}

	w.current.Bytes = w.upload.Size()
	w.parts = append(w.parts, w.current)
	w.events += w.current.Events
	w.upload, w.enc = nil, nil

	return nil
}

// finish closes the part being written, if any.
func (w *partWriter) finish() error {
	if w.upload == nil {
		return nil
	}

	return w.closePart()
}

// abort discards a part left open by a failed run. Completed parts stay:
// the next run over the same objects rewrites them, and the sweep removes
// them otherwise.
func (w *partWriter) abort() {
	if w.upload != nil {
		w.upload.Abort()
		w.upload = nil
	}
}
