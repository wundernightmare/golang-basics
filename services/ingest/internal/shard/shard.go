// Package shard is the on-disk format of the event archive: the objects the
// ingest path writes to S3 and the larger ones compaction rewrites them into.
//
// An object is one zstd stream (see codec.go) over:
//
//	header   "EVSH" | version u8 | 3 reserved bytes (zero)
//	record*  payload length u32 | CRC-32C of the payload u32 | payload
//	where a payload is
//	         received at, unix ms i64 | id length u8 | id |
//	         type length u8 | type | event (the rest)
//
// All integers are little-endian. The event is the JSON object exactly as the
// client sent it; id and type are repeated in front of it so compaction can
// deduplicate, and a replay can build record headers, without parsing JSON.
//
// A record carries its own checksum because an object lives for years and is
// rewritten by compaction: corruption must be detected where it happens, not
// passed on into a bigger object.
package shard

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

// Format constants.
const (
	// Version is the format version this package writes.
	Version = 1

	headerSize       = 8
	recordPrefixSize = 8         // length + checksum
	bodyFixedSize    = 8 + 1 + 1 // timestamp + the two length bytes
	// MaxFieldBytes is the longest id or type a record can carry.
	MaxFieldBytes = 255
)

//nolint:gochecknoglobals // immutable tables
var (
	magic      = [4]byte{'E', 'V', 'S', 'H'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// ErrCorrupt marks bytes that are not a shard: a wrong magic, an unknown
// version, a checksum mismatch, a truncated or oversized record.
var ErrCorrupt = errors.New("shard: corrupt data")

// Record is one archived event.
type Record struct {
	ID   string
	Type string // "" when the event had none
	// ReceivedAt is when the service accepted the event (its own clock,
	// millisecond precision): never the client's clock.
	ReceivedAt time.Time
	// Event is the JSON object as the client sent it.
	Event []byte
}

// EncodedSize is the number of bytes [AppendRecord] adds for r.
func EncodedSize(r Record) int {
	return recordPrefixSize + bodyFixedSize + len(r.ID) + len(r.Type) + len(r.Event)
}

// AppendHeader appends the object header: the first bytes of every shard.
func AppendHeader(dst []byte) []byte {
	dst = append(dst, magic[:]...)

	return append(dst, Version, 0, 0, 0)
}

// AppendRecord appends rec in wire form.
func AppendRecord(dst []byte, rec Record) ([]byte, error) {
	if len(rec.ID) == 0 || len(rec.ID) > MaxFieldBytes || len(rec.Type) > MaxFieldBytes {
		return dst, fmt.Errorf("shard: id must be 1..%d bytes and type at most %d, got %d and %d",
			MaxFieldBytes, MaxFieldBytes, len(rec.ID), len(rec.Type))
	}

	bodyLen := bodyFixedSize + len(rec.ID) + len(rec.Type) + len(rec.Event)
	if bodyLen > math.MaxUint32 {
		return dst, fmt.Errorf("shard: a record body of %d bytes does not fit the 32-bit length", bodyLen)
	}

	start := len(dst)

	dst = binary.LittleEndian.AppendUint32(dst, uint32(bodyLen)) // checked against MaxUint32 just above
	dst = append(dst, 0, 0, 0, 0)                                // checksum, filled in below
	dst = binary.LittleEndian.AppendUint64(dst, uint64(rec.ReceivedAt.UnixMilli()))
	dst = append(dst, byte(len(rec.ID))) //nolint:gosec // 1..MaxFieldBytes, checked above
	dst = append(dst, rec.ID...)
	dst = append(dst, byte(len(rec.Type))) //nolint:gosec // at most MaxFieldBytes, checked above
	dst = append(dst, rec.Type...)
	dst = append(dst, rec.Event...)

	body := dst[start+recordPrefixSize:]
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(body, castagnoli))

	return dst, nil
}

// Reader decodes a shard from a stream.
type Reader struct {
	src       *bufio.Reader
	maxRecord int
	buf       []byte
	started   bool
}

// NewReader reads a shard from src (already decompressed). A record whose
// body is larger than maxRecordBytes is [ErrCorrupt]: the length field of a
// damaged object must not decide how much memory the reader allocates.
func NewReader(src io.Reader, maxRecordBytes int) *Reader {
	return &Reader{src: bufio.NewReaderSize(src, 64<<10), maxRecord: maxRecordBytes}
}

// Next returns the next record, or io.EOF after the last one. The record's
// Event aliases the reader's buffer: it is valid until the next call.
// Any other error wraps [ErrCorrupt] or is the source's own.
func (r *Reader) Next() (Record, error) {
	if !r.started {
		if err := r.readHeader(); err != nil {
			return Record{}, err
		}

		r.started = true
	}

	var prefix [recordPrefixSize]byte

	switch _, err := io.ReadFull(r.src, prefix[:]); {
	case errors.Is(err, io.EOF):
		return Record{}, io.EOF // a clean end, on a record boundary
	case errors.Is(err, io.ErrUnexpectedEOF):
		return Record{}, fmt.Errorf("%w: truncated record prefix", ErrCorrupt)
	case err != nil:
		return Record{}, err
	}

	bodyLen := int(binary.LittleEndian.Uint32(prefix[:4]))
	sum := binary.LittleEndian.Uint32(prefix[4:])

	if bodyLen < bodyFixedSize+1 || bodyLen > r.maxRecord {
		return Record{}, fmt.Errorf("%w: record body of %d bytes (limit %d)", ErrCorrupt, bodyLen, r.maxRecord)
	}

	if cap(r.buf) < bodyLen {
		r.buf = make([]byte, bodyLen)
	}

	body := r.buf[:bodyLen]

	switch _, err := io.ReadFull(r.src, body); {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return Record{}, fmt.Errorf("%w: truncated record body", ErrCorrupt)
	case err != nil:
		return Record{}, err
	}

	if crc32.Checksum(body, castagnoli) != sum {
		return Record{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	return decodeBody(body)
}

func (r *Reader) readHeader() error {
	var header [headerSize]byte

	switch _, err := io.ReadFull(r.src, header[:]); {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return fmt.Errorf("%w: no header", ErrCorrupt)
	case err != nil:
		return err
	}

	if [4]byte(header[:4]) != magic {
		return fmt.Errorf("%w: bad magic %q", ErrCorrupt, header[:4])
	}

	if header[4] != Version {
		return fmt.Errorf("%w: version %d, this reader knows %d", ErrCorrupt, header[4], Version)
	}

	return nil
}

// decodeBody splits a checksummed body. The lengths inside it are still
// checked: a checksum proves the bytes are the writer's, not that the writer
// was right.
func decodeBody(body []byte) (Record, error) {
	millis := int64(binary.LittleEndian.Uint64(body[:8])) //nolint:gosec // the bit pattern AppendRecord wrote from an int64
	rest := body[8:]

	idLen := int(rest[0])
	rest = rest[1:]

	if idLen == 0 || len(rest) < idLen+1 {
		return Record{}, fmt.Errorf("%w: id length %d does not fit the record", ErrCorrupt, idLen)
	}

	id := string(rest[:idLen])
	rest = rest[idLen:]

	typeLen := int(rest[0])
	rest = rest[1:]

	if len(rest) < typeLen {
		return Record{}, fmt.Errorf("%w: type length %d does not fit the record", ErrCorrupt, typeLen)
	}

	return Record{
		ID:         id,
		Type:       string(rest[:typeLen]),
		ReceivedAt: time.UnixMilli(millis).UTC(),
		Event:      rest[typeLen:],
	}, nil
}
