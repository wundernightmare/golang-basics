package shard

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// How an object is stored: one zstd stream, named for what it is. There is
// deliberately no Content-Encoding on the object — the stored bytes are the
// artefact, and a client or proxy that decoded them in transit would break
// length and checksum verification.
const (
	// Extension is the suffix of every object key.
	Extension = ".evt.zst"
	// ContentType is the media type objects are stored with.
	ContentType = "application/zstd"
)

// Compression levels, by name (the SHARD compression config values).
const (
	LevelFastest = "fastest"
	LevelDefault = "default"
	LevelBetter  = "better"
	LevelBest    = "best"
)

// ParseLevel maps a level name to the encoder's.
func ParseLevel(name string) (zstd.EncoderLevel, error) {
	switch name {
	case LevelFastest:
		return zstd.SpeedFastest, nil
	case LevelDefault:
		return zstd.SpeedDefault, nil
	case LevelBetter:
		return zstd.SpeedBetterCompression, nil
	case LevelBest:
		return zstd.SpeedBestCompression, nil
	}

	return 0, fmt.Errorf("shard: compression level must be %s, %s, %s or %s, got %q",
		LevelFastest, LevelDefault, LevelBetter, LevelBest, name)
}

// Compressor compresses whole shards in memory. It is safe for concurrent
// use.
type Compressor struct{ enc *zstd.Encoder }

// NewCompressor builds a compressor at the named level.
func NewCompressor(level string) (*Compressor, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}

	// A nil writer: the encoder is used for EncodeAll only, which needs no
	// goroutines of its own.
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(lvl), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(maxWindow))
	if err != nil {
		return nil, fmt.Errorf("shard: zstd encoder: %w", err)
	}

	return &Compressor{enc: enc}, nil
}

// Compress returns shard compressed into a new slice.
func (c *Compressor) Compress(shard []byte) []byte {
	return c.enc.EncodeAll(shard, make([]byte, 0, len(shard)/4))
}

// NewStreamWriter returns a zstd writer over dst for an object built
// incrementally (compaction). Close flushes the stream; it does not close dst.
func NewStreamWriter(dst io.Writer, level string) (*zstd.Encoder, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(lvl), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(maxWindow))
	if err != nil {
		return nil, fmt.Errorf("shard: zstd encoder: %w", err)
	}

	return enc, nil
}

// maxWindow bounds the history a decoder keeps: what a stream may claim
// before a single byte is produced. The encoders here are capped to it, so
// every object this package wrote is readable.
const maxWindow = 8 << 20

// ErrTooLarge is returned by [Decompress] for an object that would expand
// past the limit.
var ErrTooLarge = errors.New("shard: decompressed object exceeds the limit")

// Decompress expands one stored object into memory, refusing to produce more
// than limit bytes: a small damaged (or hostile) object must not be able to
// claim gigabytes. The output is produced as a stream and cut at the limit;
// the decoder's own window is bounded separately. Bytes that are not a zstd stream are [ErrCorrupt].
func Decompress(object []byte, limit int64) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(object),
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxWindow))
	if err != nil {
		return nil, fmt.Errorf("shard: zstd decoder: %w", err)
	}
	defer dec.Close()

	// One byte past the limit tells "exactly the limit" from "more".
	out, err := io.ReadAll(io.LimitReader(dec, limit+1))

	switch {
	case errors.Is(err, zstd.ErrDecoderSizeExceeded), errors.Is(err, zstd.ErrWindowSizeExceeded):
		return nil, fmt.Errorf("%w (%d bytes)", ErrTooLarge, limit)
	case err != nil:
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	case int64(len(out)) > limit:
		return nil, fmt.Errorf("%w (%d bytes)", ErrTooLarge, limit)
	}

	return out, nil
}
