// Package lend turns a key into the bytes a byte-keyed store reads, without
// copying it.
package lend

import "unsafe"

// Bytes is key as a slice over the string's own memory: a read of any key
// length then costs no allocation. It is for a callee that neither keeps nor
// changes the slice — fastcache and freecache both copy a key into their
// ring — and for nothing else: a string is immutable, and a write through
// the slice would be a write into every copy of it.
func Bytes(key string) []byte {
	return unsafe.Slice(unsafe.StringData(key), len(key)) //nolint:gosec // G103: a read-only view of an immutable string, see above
}
