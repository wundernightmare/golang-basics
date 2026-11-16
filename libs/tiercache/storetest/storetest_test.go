package storetest_test

import (
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/tiercache"
	"github.com/tracehubmmp/golang-basics/libs/tiercache/storetest"
)

// simple is the least a store can be: a map behind a mutex that forgets its
// oldest entries past a budget of bytes. It is here to show that the
// contract asks for nothing a store of thirty lines cannot do — and, with a
// defect switched on, that the suite notices each way of breaking it.
type simple struct {
	mu      sync.Mutex
	entries map[string][]byte
	order   []string
	bytes   int

	budget int
	defect string
}

func newSimple(defect string) tiercache.StoreFactory {
	return func(maxBytes int) tiercache.Store {
		return &simple{entries: map[string][]byte{}, budget: maxBytes, defect: defect}
	}
}

func (s *simple) MaxEntry() int { return 4096 }

func (s *simple) Bytes() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bytes
}

func (s *simple) Get(dst []byte, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, found := s.entries[key]

	switch {
	case !found:
		return dst, false
	case s.defect == "Get overwrites the buffer":
		return append(dst[:0], value...), true
	case s.defect == "Get returns its own memory":
		return value, true
	}

	return append(dst, value...), true
}

func (s *simple) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(key)+len(value) > s.MaxEntry() {
		if s.defect != "a Set it cannot keep leaves the old value" {
			s.drop(key)
		}

		return
	}

	s.drop(key)

	if s.defect != "Set keeps the caller's slice" {
		value = append([]byte(nil), value...)
	}

	s.entries[key] = value
	s.order = append(s.order, key)
	s.bytes += len(key) + len(value)

	for s.bytes > s.budget && s.defect != "no bound" {
		if s.defect == "evicts what was just set" {
			s.drop(key)

			break
		}

		s.drop(s.order[0])
	}
}

// drop must be called with s.mu held.
func (s *simple) drop(key string) {
	value, found := s.entries[key]
	if !found {
		return
	}

	delete(s.entries, key)
	s.bytes -= len(key) + len(value)

	for i, held := range s.order {
		if held == key {
			s.order = append(s.order[:i], s.order[i+1:]...)

			break
		}
	}
}

func (s *simple) Delete(key string) {
	if s.defect == "Delete does nothing" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.drop(key)
}

func (s *simple) Reset() {
	if s.defect == "Reset does nothing" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.entries)
	s.order, s.bytes = nil, 0
}

// defects are the ways simple can be broken, each of which the suite fails:
// run this test with STORETEST_DEFECT set to one of them to see which check
// notices. TestTheSuiteNoticesEveryDefect does it for all of them.
var defects = []string{
	"Get overwrites the buffer",
	"Get returns its own memory",
	"Set keeps the caller's slice",
	"a Set it cannot keep leaves the old value",
	"evicts what was just set",
	"no bound",
	"Delete does nothing",
	"Reset does nothing",
}

const defectEnv = "STORETEST_DEFECT"

func TestASimpleStoreKeepsTheContract(t *testing.T) {
	t.Parallel()

	storetest.Run(t, newSimple(os.Getenv(defectEnv)), storetest.Capacity{Ask: 256 << 10, Bound: 256 << 10})
}

// A suite is worth its name once a broken store fails it: each defect of
// simple, switched on in a run of this test binary of its own, turns
// TestASimpleStoreKeepsTheContract red.
func TestTheSuiteNoticesEveryDefect(t *testing.T) {
	t.Parallel()

	if os.Getenv(defectEnv) != "" {
		t.Skip("this is a run with a defect switched on")
	}

	for _, defect := range defects {
		t.Run(defect, func(t *testing.T) {
			t.Parallel()

			child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestASimpleStoreKeepsTheContract$", "-test.count=1")

			child.Env = append(os.Environ(), defectEnv+"="+defect)

			out, err := child.CombinedOutput()

			var exit *exec.ExitError

			require.ErrorAs(t, err, &exit, "the suite passed a store with this defect:\n%s", out)
			assert.Contains(t, string(out), "--- FAIL: TestASimpleStoreKeepsTheContract/")
		})
	}
}
