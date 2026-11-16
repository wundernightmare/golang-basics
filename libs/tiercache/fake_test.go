package tiercache_test

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/tiercache"
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
	// beforeGetReturns runs after a Get has read its answer and before it
	// returns it: the window in which a racing invalidation lands.
	beforeGetReturns func()
	// beforeTombstone runs when a Tombstone has been called and has not yet
	// written: the round trip during which the old value is still in L2.
	beforeTombstone func()
	publishFailure  error // Publish alone fails with it while set

	gets, writes atomic.Int64
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

// Like a real client, a call whose context is done fails with its error.
func (f *fakeRemote) Get(ctx context.Context, key string) (string, bool, error) {
	f.gets.Add(1)

	f.mu.Lock()
	value, ok := f.live(key)
	err, hook := f.failure, f.beforeGetReturns
	f.mu.Unlock()

	if ctx.Err() != nil {
		return "", false, ctx.Err()
	}

	if err != nil {
		return "", false, err
	}

	if hook != nil {
		hook()
	}

	return value, ok && value != fakeTombstone, nil
}

func (f *fakeRemote) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failure != nil {
		return false, f.failure
	}

	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	if _, held := f.live(key); held {
		return false, nil
	}

	f.writes.Add(1)
	f.data[key] = fakeEntry{value: value, expires: time.Now().Add(ttl)}

	return true, nil
}

func (f *fakeRemote) Replace(ctx context.Context, key, old, value string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failure != nil {
		return false, f.failure
	}

	if ctx.Err() != nil {
		return false, ctx.Err()
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
	hook := f.beforeTombstone
	f.mu.Unlock()

	if hook != nil {
		hook()
	}

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

	if err := cmp.Or(f.failure, f.publishFailure); err != nil {
		defer f.mu.Unlock()

		return err
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
func (f *fakeRemote) put(cacheName string, key string, raw string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.data["tc:"+cacheName+":"+key] = fakeEntry{value: raw, expires: time.Now().Add(time.Hour)}
}

// source is the source of truth: a versioned record per key. Tests bump the
// version (a committed write), make it fail, or hold loads at a gate.
type source struct {
	mu       sync.Mutex
	versions map[string]int
	failure  error
	gate     chan struct{} // when set, loads wait here
	bug      any           // when set, loads panic with it
	payload  int           // extra bytes in KeyEncrypted

	loads    atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
}

func newSource() *source { return &source{versions: map[string]int{}} }

func (s *source) set(key string, version int) {
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

func (s *source) load(ctx context.Context, key string) (row, error) {
	s.loads.Add(1)

	now := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for peak := s.peak.Load(); now > peak && !s.peak.CompareAndSwap(peak, now); peak = s.peak.Load() {
	}

	// The row is read first — then the load may stall before returning it,
	// which is how a reader ends up holding a value older than a commit.
	s.mu.Lock()
	version, found := s.versions[key]
	err, gate, payload, bug := s.failure, s.gate, s.payload, s.bug
	s.mu.Unlock()

	if bug != nil {
		panic(bug)
	}

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return row{}, ctx.Err()
		}
	}

	switch {
	case err != nil:
		return row{}, err
	case !found:
		return row{}, fmt.Errorf("credential %q: %w", key, tiercache.ErrNotFound)
	}

	return record(key, version, payload), nil
}

// row is the value the suite caches: three strings, like the record of the
// credential example, in a codec of the suite's own.
type row struct {
	ID           string
	CRMID        string
	KeyEncrypted string
}

// rowCodec writes each field as its length (uvarint) and its bytes, and
// refuses anything else: a payload cut short or with bytes left over.
type rowCodec struct{}

func (rowCodec) Append(dst []byte, value row) ([]byte, error) {
	for _, field := range [...]string{value.ID, value.CRMID, value.KeyEncrypted} {
		dst = binary.AppendUvarint(dst, uint64(len(field)))
		dst = append(dst, field...)
	}

	return dst, nil
}

func (rowCodec) Decode(src []byte) (row, error) {
	var fields [3]string

	for i := range fields {
		size, width := binary.Uvarint(src)
		if width <= 0 || size > uint64(len(src)-width) {
			return row{}, errors.New("rowCodec: truncated")
		}

		fields[i] = string(src[width : width+int(size)])
		src = src[width+int(size):]
	}

	if len(src) != 0 {
		return row{}, errors.New("rowCodec: trailing bytes")
	}

	return row{ID: fields[0], CRMID: fields[1], KeyEncrypted: fields[2]}, nil
}

// record is the value the source holds for key at version: recognisably that
// key's (so a cross-key mix-up shows) and that version's.
func record(key string, version, payload int) row {
	sum := sha256.Sum256([]byte(key))
	filler := make([]byte, payload)

	for i := range filler {
		filler[i] = 'a' + sum[i%len(sum)]%26
	}

	return row{
		ID:           fmt.Sprintf("id-%x", sum[8:16]),
		CRMID:        fmt.Sprintf("v%d", version),
		KeyEncrypted: fmt.Sprintf("enc-%x-%s", sum[:8], filler),
	}
}

// harnesses numbers the caches of the suite: each gets a name of its own.
var harnesses atomic.Int64

type harness struct {
	cache *tiercache.Cache[row]
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

	return newLoggedHarness(t, remote, src, tune, nil)
}

func newLoggedHarness(t *testing.T, remote tiercache.Remote, src *source, tune func(*tiercache.Config), log *slog.Logger) *harness {
	t.Helper()

	cfg := tiercache.DefaultConfig()
	cfg.Name, cfg.SoftTTL, cfg.HardTTL = fmt.Sprint("unit-", harnesses.Add(1)), softTTL, hardTTL

	if tune != nil {
		tune(&cfg)
	}

	cache, err := tiercache.New(cfg, tiercache.NewMapStore, rowCodec{}, src.load, remote, log)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	reg := prometheus.NewRegistry()
	reg.MustRegister(cache.Collectors()...)

	return &harness{cache: cache, src: src, reg: reg, name: cfg.Name}
}

func (rig *harness) reads(t *testing.T, result string) float64 {
	t.Helper()

	return rig.metric(t, "tiercache_reads_total", map[string]string{"result": result})
}

func (rig *harness) metric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := rig.reg.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

	series:
		for _, metric := range family.GetMetric() {
			have := map[string]string{}
			for _, label := range metric.GetLabel() {
				have[label.GetName()] = label.GetValue()
			}

			for k, v := range labels {
				if have[k] != v {
					continue series
				}
			}

			return metric.GetCounter().GetValue() + metric.GetGauge().GetValue()
		}
	}

	return 0
}

// read is version without assertions, for goroutines other than the test's.
func (rig *harness) read(key string) (string, error) {
	got, err := rig.cache.Get(context.Background(), key)

	return got.CRMID, err
}

// version is the source version of the value Get returns for key.
func (rig *harness) version(t *testing.T, key string) string {
	t.Helper()

	got, err := rig.cache.Get(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, record(key, 0, 0).ID, got.ID, "the value belongs to the key asked for")

	return got.CRMID
}
