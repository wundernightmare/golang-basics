package testx

import (
	"fmt"
	"os"
	"sync/atomic"
)

var seq atomic.Uint64

// Unique returns a name unique within the test binary, for the schema, key
// prefix or topic a test owns on a shared dependency. Tests isolate through
// names on one shared container (see libs/testx/containers), never through
// fresh containers.
//
// The name is "<prefix>-<pid>-<n>": at most 29 characters on top of the
// prefix (two hyphens, a pid of up to 7 digits on Linux, a 64-bit counter),
// about ten in practice.
// It is not adapted to where it goes: a Postgres identifier cannot hold a
// hyphen unquoted and is cut at 63 bytes, an S3 bucket name allows 63
// characters and no upper case or underscore, a Kafka topic 249 — the caller
// picks a prefix that fits and quotes or replaces the hyphens where needed.
func Unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), seq.Add(1))
}
