package tiercache

import (
	"os"
	"strconv"
)

// SuiteL1BytesEnv names the L1 capacity the suite's caches are built with
// when a test does not choose one: unset is the default (the off-heap
// store). TestTheSuiteHoldsOnTheHeapStore runs the suite again with it set.
const SuiteL1BytesEnv = "TIERCACHE_TEST_L1_BYTES"

// SuiteL1Bytes is the capacity [SuiteL1BytesEnv] asks for, or 0.
func SuiteL1Bytes() int {
	n, _ := strconv.Atoi(os.Getenv(SuiteL1BytesEnv))

	return n
}
