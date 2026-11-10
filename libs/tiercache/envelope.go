package tiercache

import "encoding/binary"

// The envelope is what both levels store: a fixed header in front of the
// codec's payload. Expiry travels with the value, so L1 needs no TTL support
// of its own and an entry copied from L2 keeps its original deadlines.
//
//	byte 0      format version
//	bytes 1–8   soft deadline, Unix milliseconds, big endian
//	bytes 9–16  hard deadline, Unix milliseconds, big endian
//	bytes 17…   payload
const (
	envelopeVersion = 1
	headerSize      = 1 + 8 + 8
)

// appendHeader appends the envelope header to dst.
func appendHeader(dst []byte, softMs, hardMs int64) []byte {
	dst = append(dst, envelopeVersion)
	dst = binary.BigEndian.AppendUint64(dst, uint64(softMs)) //nolint:gosec // a Unix-millisecond deadline is never negative
	dst = binary.BigEndian.AppendUint64(dst, uint64(hardMs)) //nolint:gosec // (same)

	return dst
}

// parseEnvelope splits an envelope. ok is false for anything this version did
// not write — too short, another format version, deadlines out of order — and
// the caller treats that as a miss: bytes from a cache are never trusted.
func parseEnvelope(env []byte) (softMs, hardMs int64, payload []byte, ok bool) {
	if len(env) < headerSize || env[0] != envelopeVersion {
		return 0, 0, nil, false
	}

	soft := binary.BigEndian.Uint64(env[1:9])
	hard := binary.BigEndian.Uint64(env[9:17])

	if hard < soft || hard > 1<<62 {
		return 0, 0, nil, false
	}

	return int64(soft), int64(hard), env[headerSize:], true //nolint:gosec // bounded by the check above
}
