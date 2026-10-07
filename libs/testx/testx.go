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
func Unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), seq.Add(1))
}
