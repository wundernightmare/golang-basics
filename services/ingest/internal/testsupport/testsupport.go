// Package testsupport holds the fakes the ingest unit tests share: an
// in-memory object store and a Kafka publisher whose failures and latency a
// test controls. Imported from _test files only.
package testsupport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
	"github.com/tracehubmmp/golang-basics/services/ingest/internal/shard"
)

// ErrDown is what a fake returns while it is told to fail.
var ErrDown = errors.New("fake: dependency down")

// Store is an in-memory archive.Store.
type Store struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	fail    bool
	gate    chan struct{} // non-nil: puts block until it is closed
}

// NewStore returns an empty store that accepts every put.
func NewStore() *Store { return &Store{objects: map[string][]byte{}} }

// Put implements archive.Store.
func (s *Store) Put(ctx context.Context, key string, body []byte, _ string) error {
	s.mu.Lock()
	gate := s.gate
	s.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.puts++

	if s.fail {
		return ErrDown
	}

	s.objects[key] = bytes.Clone(body)

	return nil
}

// Fail makes every following put fail (or succeed again).
func (s *Store) Fail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

// Block makes puts wait until the returned func is called.
func (s *Store) Block() (release func()) {
	gate := make(chan struct{})

	s.mu.Lock()
	s.gate = gate
	s.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.gate = nil
			s.mu.Unlock()
			close(gate)
		})
	}
}

// Puts is the number of put attempts so far.
func (s *Store) Puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.puts
}

// Keys returns the stored keys, sorted.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// IDs decodes every stored object and returns the event ids, in key order.
func (s *Store) IDs() ([]string, error) {
	var ids []string

	for _, key := range s.Keys() {
		s.mu.Lock()
		object := s.objects[key]
		s.mu.Unlock()

		records, err := DecodeObject(object)
		if err != nil {
			return nil, err
		}

		for _, r := range records {
			ids = append(ids, r.ID)
		}
	}

	return ids, nil
}

// DecodeObject expands one stored object into its records.
func DecodeObject(object []byte) ([]shard.Record, error) {
	const limit = 256 << 20

	plain, err := shard.Decompress(object, limit)
	if err != nil {
		return nil, err
	}

	reader := shard.NewReader(bytes.NewReader(plain), len(plain))

	var out []shard.Record

	for {
		r, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}

		if err != nil {
			return nil, err
		}

		r.Event = bytes.Clone(r.Event)
		out = append(out, r)
	}
}

// Publisher is a pipeline.Publisher that records what it was sent.
type Publisher struct {
	mu   sync.Mutex
	sent []kafka.Message
	err  error
	gate chan struct{}
}

// Send implements pipeline.Publisher.
func (p *Publisher) Send(ctx context.Context, msg kafka.Message) error {
	p.mu.Lock()
	gate, err := p.gate, p.err
	p.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}

		p.mu.Lock()
		err = p.err
		p.mu.Unlock()
	}

	if err != nil {
		return err
	}

	p.mu.Lock()
	p.sent = append(p.sent, msg)
	p.mu.Unlock()

	return nil
}

// FailWith makes every following send return err (nil: succeed again).
func (p *Publisher) FailWith(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

// Block makes sends wait until the returned func is called.
func (p *Publisher) Block() (release func()) {
	gate := make(chan struct{})

	p.mu.Lock()
	p.gate = gate
	p.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			p.mu.Lock()
			p.gate = nil
			p.mu.Unlock()
			close(gate)
		})
	}
}

// Sent returns a copy of the messages acknowledged so far.
func (p *Publisher) Sent() []kafka.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]kafka.Message(nil), p.sent...)
}
