package tiercache_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/tiercache"
	"github.com/tracehubmmp/golang-basics/libs/tiercache/credential"
)

// fakeRemote is an in-memory tiercache.Remote with the semantics of
// libs/valkey (TTL, SET NX, compare-and-set, tombstones, at-most-once
// Pub/Sub) and switches for the failures a real one has. The integration
// suite runs the same scenarios against the real thing.
type fakeRemote struct {
	mu      sync.Mutex
	data    map[string]fakeEntry
	subs    map[int]fakeSub
	nextSub int
	failure error // every call fails with it while set

	gets, writes atomic.Int64
	// beforeGetReturns runs after a Get has read its answer and before it
	// returns it: the window in which a racing invalidation lands.
	beforeGetReturns func()
}

type fakeEntry struct {
	value   string
	expires time.Time
}

type fakeSub struct {
	channel      string
	onMessage    func(string)
	onSubscribed func()
}

const fakeTombstone = "\x00tombstone"

var errRemoteDown = errors.New("fake remote: connection refused")

func newFakeRemote() *fakeRemote {
	return &fakeRemote{data: map[string]fakeEntry{}, subs: map[int]fakeSub{}}
}

func (f *fakeRemote) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.failure = err
}

// live returns the unexpired entry at key. Callers hold f.mu.
func (f *fakeRemote) live(key string) (string, bool) {
	entry, ok := f.data[key]
	if !ok || !time.Now().Before(entry.expires) {
		return "", false
	}

	return entry.value, true
}

func (f *fakeRemote) Get(_ context.Context, key string) (string, bool, error) {
	f.gets.Add(1)

	f.mu.Lock()
	value, ok := f.live(key)
	err, hook := f.failure, f.beforeGetReturns
	f.mu.Unlock()

	if err != nil {
		return "", false, err
	}

	if hook != nil {
		hook()
	}

	return value, ok && value != fakeTombstone, nil
}

func (f *fakeRemote) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failure != nil {
		return false, f.failure
	}

	if _, held := f.live(key); held {
		return false, nil
	}

	f.writes.Add(1)
	f.data[key] = fakeEntry{value: value, expires: time.Now().Add(ttl)}

	return true, nil
}

func (f *fakeRemote) Replace(_ context.Context, key, old, value string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failure != nil {
		return false, f.failure
	}

	if held, ok := f.live(key); !ok || held != old {
		return false, nil
	}

	f.writes.Add(1)
	f.data[key] = fakeEntry{value: value, expires: time.Now().Add(ttl)}

	return true, nil
}

func (f *fakeRemote) Tombstone(_ context.Context, key string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failure != nil {
		return f.failure
	}

	f.data[key] = fakeEntry{value: fakeTombstone, expires: time.Now().Add(ttl)}

	return nil
}

func (f *fakeRemote) Publish(_ context.Context, channel, message string) error {
	f.mu.Lock()

	if f.failure != nil {
		defer f.mu.Unlock()

		return f.failure
	}

	var deliver []func(string)

	for _, sub := range f.subs {
		if sub.channel == channel {
			deliver = append(deliver, sub.onMessage)
		}
	}
	f.mu.Unlock()

	for _, onMessage := range deliver {
		onMessage(message)
	}

	return nil
}

func (f *fakeRemote) Subscribe(ctx context.Context, channel string, onMessage func(string), onSubscribed func()) error {
	f.mu.Lock()

	if f.failure != nil {
		defer f.mu.Unlock()

		return f.failure
	}

	id := f.nextSub
	f.nextSub++
	f.subs[id] = fakeSub{channel: channel, onMessage: onMessage, onSubscribed: onSubscribed}
	f.mu.Unlock()

	onSubscribed()
	<-ctx.Done()

	f.mu.Lock()
	delete(f.subs, id)
	f.mu.Unlock()

	return ctx.Err()
}

// reconnect is what a connection that dropped and came back looks like to
// the subscribers: nothing was delivered meanwhile, and they are told so.
func (f *fakeRemote) reconnect() {
	f.mu.Lock()

	var notify []func()
	for _, sub := range f.subs {
		notify = append(notify, sub.onSubscribed)
	}
	f.mu.Unlock()

	for _, onSubscribed := range notify {
		onSubscribed()
	}
}

// put stores raw bytes at the cache's key for k, bypassing the cache.
func (f *fakeRemote) put(cacheName string, key tiercache.Key, raw string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.data["tc:"+cacheName+":"+string(key[:])] = fakeEntry{value: raw, expires: time.Now().Add(time.Hour)}
}

// source is the source of truth: a versioned record per key. Tests bump the
// version (a committed write), make it fail, or hold loads at a gate.
type source struct {
	mu       sync.Mutex
	versions map[tiercache.Key]int
	failure  error
	gate     chan struct{} // when set, loads wait here
	payload  int           // extra bytes in KeyEncrypted

	loads    atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
}

func newSource() *source { return &source{versions: map[tiercache.Key]int{}} }

func (s *source) set(key tiercache.Key, version int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.versions[key] = version
}

func (s *source) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failure = err
}

func (s *source) hold() (release func()) {
	gate := make(chan struct{})

	s.mu.Lock()
	s.gate = gate
	s.mu.Unlock()

	return sync.OnceFunc(func() {
		s.mu.Lock()
		s.gate = nil
		s.mu.Unlock()

		close(gate)
	})
}

func (s *source) load(ctx context.Context, key tiercache.Key) (credential.Credential, error) {
	s.loads.Add(1)

	now := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for peak := s.peak.Load(); now > peak && !s.peak.CompareAndSwap(peak, now); peak = s.peak.Load() {
	}

	// The row is read first — then the load may stall before returning it,
	// which is how a reader ends up holding a value older than a commit.
	s.mu.Lock()
	version, found := s.versions[key]
	err, gate, payload := s.failure, s.gate, s.payload
	s.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return credential.Credential{}, ctx.Err()
		}
	}

	switch {
	case err != nil:
		return credential.Credential{}, err
	case !found:
		return credential.Credential{}, fmt.Errorf("credential %x: %w", key[:4], tiercache.ErrNotFound)
	}

	return record(key, version, payload), nil
}

// record is the value the source holds for key at version: recognisably that
// key's (so a cross-key mix-up shows) and that version's.
func record(key tiercache.Key, version, payload int) credential.Credential {
	sum := sha256.Sum256(key[:])
	filler := make([]byte, payload)

	for i := range filler {
		filler[i] = 'a' + sum[i%len(sum)]%26
	}

	return credential.Credential{
		ID:           fmt.Sprintf("id-%x", key[:8]),
		CRMID:        fmt.Sprintf("v%d", version),
		KeyEncrypted: fmt.Sprintf("enc-%x-%s", sum[:8], filler),
	}
}

type harness struct {
	cache *tiercache.Cache[credential.Credential]
	src   *source
	reg   *prometheus.Registry
	name  string
}

// The TTLs every unit test runs with. Deadlines are jittered ±10%, so the
// tests step to instants that are unambiguous: 5s is fresh, 20s is stale,
// 70s is expired, and with the 60s grace 100s is within it and 200s past it.
const (
	softTTL = 10 * time.Second
	hardTTL = 60 * time.Second
)

func newHarness(t *testing.T, remote tiercache.Remote, src *source, tune func(*tiercache.Config)) *harness {
	t.Helper()

	cfg := tiercache.Config{Name: testx.Unique("unit"), SoftTTL: softTTL, HardTTL: hardTTL}
	if tune != nil {
		tune(&cfg)
	}

	cache, err := tiercache.New(cfg, credential.Codec{}, src.load, remote, nil)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	reg := prometheus.NewRegistry()
	reg.MustRegister(cache.Collectors()...)

	return &harness{cache: cache, src: src, reg: reg, name: cfg.Name}
}

func (rig *harness) reads(t *testing.T, result string) float64 {
	t.Helper()

	return max(0, testx.Metric(t, rig.reg, "tiercache_reads_total", map[string]string{"result": result}))
}

func (rig *harness) metric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	return max(0, testx.Metric(t, rig.reg, name, labels))
}

// read is version without assertions, for goroutines other than the test's.
func (rig *harness) read(key tiercache.Key) (string, error) {
	got, err := rig.cache.Get(context.Background(), key)

	return got.CRMID, err
}

// version is the source version of the value Get returns for key.
func (rig *harness) version(t *testing.T, key tiercache.Key) string {
	t.Helper()

	got, err := rig.cache.Get(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, record(key, 0, 0).ID, got.ID, "the value belongs to the key asked for")

	return got.CRMID
}
