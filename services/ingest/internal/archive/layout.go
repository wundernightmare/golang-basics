// Package archive is the S3 side of the ingest service: where objects live
// (this file), and the writer that turns accepted events into them
// (writer.go).
//
// Layout, under an optional prefix:
//
//	raw/dt=2026-11-10/hour=14/<unix ms>-<writer>-<seq>.evt.zst    written by the ingest pods
//	compacted/dt=2026-11-10/hour=14/part-<run>-<n>.evt.zst        written by compaction
//	compacted/dt=2026-11-10/hour=14/_MANIFEST.json                … its commit record
//	quarantine/<the raw key>                                      raw objects compaction could not read
//
// The partition is the hour of the service's own clock at the moment a batch
// is flushed, in UTC — never a client timestamp. An hour is therefore closed
// a fixed delay after it ends (the longest a put can take), with no
// coordination between writers: any number of pods write into the same hour
// under names that cannot collide.
//
// A raw name is <unix ms>-<writer>-<seq>: the flush time first so a listing
// is in time order, then the writer — a random id drawn once per process, not
// the pod name, because a restarted container keeps the pod name while its
// sequence starts again — then that writer's batch number. The name is fixed
// before the first put attempt, so repeating a put whose answer was lost
// replaces the object instead of adding a second one.
package archive

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

const (
	rawDir        = "raw/"
	compactedDir  = "compacted/"
	quarantineDir = "quarantine/"
	manifestName  = "_MANIFEST.json"
	hourLayout    = "dt=2006-01-02/hour=15/"
)

// Layout builds and parses the archive's keys.
type Layout struct {
	prefix string // "" or ends in "/"
}

// NewLayout returns the layout under prefix ("" for the bucket root); a
// missing trailing slash is added.
func NewLayout(prefix string) Layout {
	prefix = strings.TrimLeft(prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return Layout{prefix: prefix}
}

// NewWriterID draws the random id a process names its raw objects with.
func NewWriterID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("archive: writer id: %w", err)
	}

	return hex.EncodeToString(raw[:]), nil
}

// RawRoot is the prefix every raw object is under.
func (l Layout) RawRoot() string { return l.prefix + rawDir }

// RawHour is the prefix of one hour's raw objects.
func (l Layout) RawHour(hour time.Time) string {
	return l.RawRoot() + hour.UTC().Format(hourLayout)
}

// RawKey names the batch a writer flushed at flushedAt.
func (l Layout) RawKey(flushedAt time.Time, writer string, seq uint64) string {
	return fmt.Sprintf("%s%013d-%s-%08d%s", l.RawHour(flushedAt), flushedAt.UnixMilli(), writer, seq, shard.Extension)
}

// CompactedHour is the prefix of one hour's compacted objects.
func (l Layout) CompactedHour(hour time.Time) string {
	return l.prefix + compactedDir + hour.UTC().Format(hourLayout)
}

// ManifestKey is the commit record of one compacted hour.
func (l Layout) ManifestKey(hour time.Time) string { return l.CompactedHour(hour) + manifestName }

// PartKey names part n of the compaction run that wrote it. run identifies
// the set of raw objects the run read, so a repeated run over the same set
// rewrites the same keys and a run over a different set never touches them.
func (l Layout) PartKey(hour time.Time, run string, n int) string {
	return fmt.Sprintf("%spart-%s-%05d%s", l.CompactedHour(hour), run, n, shard.Extension)
}

// IsPart reports whether key is a compacted part (as opposed to the manifest).
func (l Layout) IsPart(key string) bool {
	name := key[strings.LastIndexByte(key, '/')+1:]

	return strings.HasPrefix(name, "part-") && strings.HasSuffix(name, shard.Extension)
}

// QuarantineKey is where a raw object that could not be read is moved, so it
// stops being retried and stays available to a human.
func (l Layout) QuarantineKey(rawKey string) string {
	return l.prefix + quarantineDir + strings.TrimPrefix(rawKey, l.prefix)
}

// ParseHour reads the hour out of a "…/dt=YYYY-MM-DD/hour=HH/" prefix (or any
// key under one).
func ParseHour(key string) (time.Time, bool) {
	start := strings.Index(key, "dt=")
	if start < 0 || len(key) < start+len(hourLayout) {
		return time.Time{}, false
	}

	hour, err := time.ParseInLocation(hourLayout, key[start:start+len(hourLayout)], time.UTC)
	if err != nil {
		return time.Time{}, false
	}

	return hour, true
}
