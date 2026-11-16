package integration_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/tiercache"
	"github.com/tracehubmmp/golang-basics/libs/tiercache/credential"
	"github.com/tracehubmmp/golang-basics/libs/tiercache/offheap"
)

// source is the source of truth: a versioned record per key. Tests bump the
// version (a committed write) or hold loads at a gate.
type source struct {
	mu       sync.Mutex
	versions map[string]int
	gate     chan struct{} // when set, loads wait here
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

func (s *source) load(ctx context.Context, key string) (credential.Credential, error) {
	s.loads.Add(1)

	now := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for peak := s.peak.Load(); now > peak && !s.peak.CompareAndSwap(peak, now); peak = s.peak.Load() {
	}

	// The credential.Credential is read first — then the load may stall before returning it,
	// which is how a reader ends up holding a value older than a commit.
	s.mu.Lock()
	version, found := s.versions[key]
	gate, payload := s.gate, s.payload
	s.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return credential.Credential{}, ctx.Err()
		}
	}

	if !found {
		return credential.Credential{}, fmt.Errorf("credential %q: %w", key, tiercache.ErrNotFound)
	}

	return record(key, version, payload), nil
}

// record is the value the source holds for key at version: recognisably that
// key's (so a cross-key mix-up shows) and that version's.
func record(key string, version, payload int) credential.Credential {
	sum := sha256.Sum256([]byte(key))
	filler := make([]byte, payload)

	for i := range filler {
		filler[i] = 'a' + sum[i%len(sum)]%26
	}

	return credential.Credential{
		ID:           fmt.Sprintf("id-%x", sum[8:16]),
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

func newHarness(t *testing.T, remote tiercache.Remote, src *source, tune func(*tiercache.Config)) *harness {
	t.Helper()

	return newHarnessOn(t, offheap.New, remote, src, tune)
}

// newHarnessOn is newHarness with the L1 store named: a process on the other
// store shares Valkey with the rest.
func newHarnessOn(t *testing.T, store tiercache.StoreFactory, remote tiercache.Remote, src *source, tune func(*tiercache.Config)) *harness {
	t.Helper()

	cfg := tiercache.Config{Name: testx.Unique("integration")}
	if tune != nil {
		tune(&cfg)
	}

	cache, err := tiercache.New(cfg, store, credential.Codec{}, src.load, remote, nil)
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
