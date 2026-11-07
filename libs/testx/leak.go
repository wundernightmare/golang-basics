package testx

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

// Main runs the package's tests and then fails the binary when a goroutine
// they started is still running: a server, client, consumer loop or pool
// somebody forgot to Close. Use it as the package's TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(testx.Main(m)) }
//
// opts add goleak ignores for goroutines that are not the code's to stop
// (containers.Main passes the Docker client's). A run that already failed is
// reported as is — a leak check on a failed run only adds noise.
func Main(m *testing.M, opts ...goleak.Option) int {
	code := m.Run()
	if code != 0 {
		return code
	}

	return CheckLeaks(opts...)
}

// CheckLeaks is the check [Main] runs after the tests: it reports leaked
// goroutines on stderr and returns 1, or returns 0. goleak retries for a
// while, so goroutines that are just finishing (a closed server's listener
// loop) are not reported.
func CheckLeaks(opts ...goleak.Option) int {
	if err := goleak.Find(opts...); err != nil {
		fmt.Fprintf(os.Stderr, "testx: goroutines leaked by the tests:\n%v\n", err)

		return 1
	}

	return 0
}
