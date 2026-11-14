package tiercache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"golang.org/x/sync/singleflight"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// ErrNotFound is what a [Loader] returns for a key the source of truth does
// not have, and what [Cache.Get] returns then. It is not cached.
var ErrNotFound = errors.New("tiercache: not found")

// Key is a cache key: the SHA-256 of whatever identifies the value. A fixed
// array keeps the read path free of allocations and of key-length checks.
type Key [sha256.Size]byte

// KeyOf hashes s into a [Key].
func KeyOf(s string) Key { return sha256.Sum256([]byte(s)) }

// l1MaxEntry is the largest key+envelope L1 stores: its ring buffers are made
// of 64 KiB chunks, an entry with its 4 bytes of lengths must be smaller than
// one (fastcache: `4+len(k)+len(v) >= chunkSize` is dropped), hence 5.
// A larger entry is silently dropped by the store, so it is refused (and
// counted) here instead and served from L2.
// TestLargestEntryL1AcceptsIsHeld checks the number against the store.
const l1MaxEntry = 64<<10 - 5

// Cache is a two-level read cache: an off-heap, fixed-size L1 in the process
// in front of a shared L2 ([Remote]), in front of the source of truth
// ([Loader]). See the package documentation for the read rules. It is safe
// for concurrent use; build one with [New] and [Cache.Close] it.
type Cache[T any] struct {
	cfg     Config
	local   *fastcache.Cache
	remote  Remote // nil: L1 and the loader only
	codec   Codec[T]
	load    Loader[T]
	log     *slog.Logger
	metrics *metrics
	breaker *resilience.Breaker // keeps a failing L2 from costing every miss a command timeout
	flight  singleflight.Group
	buffers sync.Pool // *[]byte

	prefix  string
	channel string

	// fillMu orders L1 fills against invalidations: a fill holds it shared
	// and checks that epoch has not moved since its read began; an
	// invalidation bumps epoch and removes the entry under the exclusive
	// lock. Without it a value read just before an invalidation could be
	// written to L1 just after it and live there for a full TTL.
	fillMu sync.RWMutex
	epoch  atomic.Uint64

	refreshMu  sync.Mutex
	refreshing map[Key]struct{}
	closed     bool

	lastWarn atomic.Int64 // Unix seconds of the last rate-limited warning

	life context.Context //nolint:containedctx // the cache's own lifetime: cancelled by Close to stop its goroutines
	stop context.CancelFunc
	wg   sync.WaitGroup
}

// New builds a cache. remote may be nil (see [Remote]); log may be nil. With
// a remote and invalidation enabled it starts the subscription goroutine,
// which [Cache.Close] stops. The remote stays the caller's to close.
func New[T any](cfg Config, codec Codec[T], load Loader[T], remote Remote, log *slog.Logger) (*Cache[T], error) {
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if codec == nil || load == nil {
		return nil, errors.New("tiercache: codec and loader are required")
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	local := fastcache.New(cfg.MaxBytes)
	life, stop := context.WithCancel(context.Background()) //nolint:gosec // G118: stop is kept in the Cache and called by Close

	cache := &Cache[T]{
		cfg:        cfg,
		local:      local,
		remote:     remote,
		codec:      codec,
		load:       load,
		log:        log,
		metrics:    newMetrics(cfg.Name, local),
		buffers:    sync.Pool{New: func() any { return new([]byte) }},
		prefix:     "tc:" + cfg.Name + ":",
		channel:    "tc:" + cfg.Name + ":inv",
		refreshing: make(map[Key]struct{}, cfg.RefreshConcurrency),
		life:       life,
		stop:       stop,
	}

	cache.breaker = resilience.NewBreaker(cfg.breaker(), time.Now, func(_, to resilience.State) {
		if to == resilience.StateOpen {
			cache.warn(life, "tiercache: L2 is failing, skipping it", "cache", cfg.Name, "for", cfg.BreakerOpenTimeout.String())
		}
	})

	if remote != nil && !cfg.DisableInvalidation {
		cache.wg.Go(cache.subscribe)
	}

	return cache, nil
}

// Close stops the subscription and waits for the background refreshes, then
// hands L1's memory back for reuse by other caches of the process. The cache
// must not be used afterwards.
func (c *Cache[T]) Close() {
	c.refreshMu.Lock()
	c.closed = true
	c.refreshMu.Unlock()

	c.stop()
	c.wg.Wait()
	c.local.Reset()
}

// Get returns the value for key.
//
//   - Younger than SoftTTL in L1: returned as is. This path does no I/O, takes
//     no exclusive lock and allocates only what the codec's Decode does.
//   - Between SoftTTL and HardTTL: returned as is, and refreshed in the
//     background — at most one refresh per key, RefreshConcurrency in all.
//   - Past HardTTL or absent: ignored. The value comes from L2 or, failing
//     that, from the loader; concurrent callers for one key share one such
//     lookup, each honouring its own ctx while it waits.
//
// An unreachable L2 never fails a read: the loader answers, L1 keeps what it
// has. When the loader fails too, a value past HardTTL is served for
// StaleIfError more; beyond that the loader's error is returned.
func (c *Cache[T]) Get(ctx context.Context, key Key) (T, error) {
	nowMs := time.Now().UnixMilli()

	buf := c.buffers.Get().(*[]byte) //nolint:forcetypeassert // the pool only ever holds *[]byte
	env, found := c.local.HasGet((*buf)[:0], key[:])
	*buf = env

	if found {
		if value, stale, ok := c.usable(env, nowMs); ok {
			c.release(buf)

			if stale {
				c.metrics.stale.Inc()
				c.refresh(ctx, key)
			} else {
				c.metrics.fresh.Inc()
			}

			return value, nil
		}
	}

	c.release(buf)

	return c.fetch(ctx, key)
}

// release returns a scratch buffer to the pool, unless a value too large for
// L1 has grown it: such a buffer is left to the garbage collector rather
// than kept for good.
func (c *Cache[T]) release(buf *[]byte) {
	if cap(*buf) <= 2*l1MaxEntry { // twice: append may over-allocate for the largest entry L1 holds
		c.buffers.Put(buf)
	}
}

// usable decodes an L1 entry that is within its hard deadline; anything else
// — expired, not an envelope, a payload the codec refuses — is a miss.
func (c *Cache[T]) usable(env []byte, nowMs int64) (value T, stale, ok bool) {
	softMs, hardMs, payload, valid := parseEnvelope(env)
	if !valid || nowMs >= hardMs {
		return value, false, false
	}

	value, err := c.codec.Decode(payload)

	return value, nowMs >= softMs, err == nil
}

// Invalidate drops key everywhere after a write to the source of truth has
// committed: from this process's L1 at once, from L2 by a tombstone that
// also refuses fills racing the write (see libs/valkey), and from every other
// process's L1 through the invalidation channel.
//
// The local part cannot fail. An error means L2 or the channel could not be
// reached: other processes then keep the old value until its TTL (or until
// their subscription reconnects, which empties their L1) — log it.
func (c *Cache[T]) Invalidate(ctx context.Context, key Key) error {
	c.invalidateLocal(key)

	if c.remote == nil {
		return nil
	}

	// The breaker is neither consulted nor told: an invalidation is worth
	// one attempt even while reads are skipping L2.
	tombErr := c.remote.Tombstone(ctx, c.remoteKey(key), c.cfg.TombstoneTTL)

	var pubErr error
	if !c.cfg.DisableInvalidation {
		pubErr = c.remote.Publish(ctx, c.channel, string(key[:]))
	}

	if err := errors.Join(tombErr, pubErr); err != nil {
		return fmt.Errorf("tiercache: invalidate: %w", err)
	}

	return nil
}

// result is what one shared lookup hands to every caller waiting on it.
type result[T any] struct {
	value  T
	source prometheusSource
}

// prometheusSource names where a looked-up value came from, for the metric.
type prometheusSource uint8

const (
	sourceL2 prometheusSource = iota
	sourceLoad
	sourceStaleError
)

// fetch is the synchronous path of a miss: one lookup per key however many
// callers. As in valkey.Aside, the shared lookup runs detached from the
// first caller's cancellation (keeping its deadline and trace), so one
// impatient caller cannot fail the others.
func (c *Cache[T]) fetch(ctx context.Context, key Key) (T, error) {
	var zero T

	shared := c.flight.DoChan(string(key[:]), func() (any, error) {
		lookupCtx := context.WithoutCancel(ctx)
		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc

			lookupCtx, cancel = context.WithDeadline(lookupCtx, deadline)
			defer cancel()
		}

		return c.resolve(lookupCtx, key, false)
	})

	select {
	case res := <-shared:
		if res.Err != nil {
			if errors.Is(res.Err, ErrNotFound) {
				c.metrics.notFound.Inc()
			} else {
				c.metrics.failed.Inc()
			}

			return zero, res.Err
		}

		got := res.Val.(result[T]) //nolint:forcetypeassert // resolve only ever returns result[T]

		switch got.source {
		case sourceL2:
			c.metrics.fromL2.Inc()
		case sourceLoad:
			c.metrics.loaded.Inc()
		case sourceStaleError:
			c.metrics.staleError.Inc()
		}

		return got.value, nil
	case <-ctx.Done():
		c.metrics.failed.Inc()

		return zero, ctx.Err()
	}
}

// resolve looks key up in L2 and then in the source of truth, and fills the
// levels above whatever answered. refreshing is the background variant: a
// stale L2 value is not an answer for it (that is what it is replacing), and
// it never falls back to an expired one.
func (c *Cache[T]) resolve(ctx context.Context, key Key, refreshing bool) (result[T], error) {
	var none result[T]

	epoch := c.epoch.Load()
	remoteKey := c.remoteKey(key)

	// What L2 holds now: the value a refresh replaces (compare-and-set), and
	// the fallback when the source fails.
	held, heldFound := c.remoteGet(ctx, remoteKey)
	heldOK := heldFound

	var (
		heldEnv              []byte
		heldSoftMs, heldHard int64
		heldPayload          []byte
	)

	if heldOK {
		heldEnv = []byte(held)
		heldSoftMs, heldHard, heldPayload, heldOK = parseEnvelope(heldEnv)
	}

	if nowMs := time.Now().UnixMilli(); heldOK && nowMs < heldHard && (!refreshing || nowMs < heldSoftMs) {
		if value, err := c.codec.Decode(heldPayload); err == nil {
			c.fill(epoch, key, heldEnv)

			return result[T]{value: value, source: sourceL2}, nil
		}

		heldOK = false // corrupt payload: as good as absent
	}

	value, err := c.load(ctx, key)

	switch {
	case errors.Is(err, ErrNotFound):
		// Gone from the source: stop serving what L1 and L2 still have.
		c.invalidateLocal(key)

		if heldFound {
			_ = c.remote.Tombstone(ctx, remoteKey, c.cfg.TombstoneTTL) // best effort: the entry's TTL bounds it otherwise
		}

		return none, err
	case err != nil:
		if !refreshing {
			if stale, ok := c.staleFallback(key, heldPayload, heldHard, heldOK); ok {
				c.warn(ctx, "tiercache: source failed, serving an expired value", "cache", c.cfg.Name, "err", err)

				return result[T]{value: stale, source: sourceStaleError}, nil
			}
		}

		return none, err
	}

	buf := c.buffers.Get().(*[]byte) //nolint:forcetypeassert // the pool only ever holds *[]byte
	defer c.release(buf)

	now := time.Now()
	hardTTL := valkey.Jitter(c.cfg.HardTTL)
	softTTL := min(valkey.Jitter(c.cfg.SoftTTL), hardTTL)

	env, err := c.codec.Append(appendHeader((*buf)[:0], now.Add(softTTL).UnixMilli(), now.Add(hardTTL).UnixMilli()), value)
	*buf = env

	if err != nil {
		// The value is good, it just cannot be cached.
		c.warn(ctx, "tiercache: cannot encode the value for the cache", "cache", c.cfg.Name, "err", err)

		return result[T]{value: value, source: sourceLoad}, nil
	}

	// L2 decides whether this load may be cached at all: a tombstone, or a
	// value somebody else wrote meanwhile, refuses the write, and then L1
	// must not keep it either. With L2 unreachable there is nobody to ask —
	// and nobody to learn of an invalidation from; L1 keeps it for its TTL.
	if c.remoteStore(ctx, remoteKey, held, string(env), hardTTL+c.cfg.StaleIfError, heldFound) {
		c.fill(epoch, key, env)
	}

	return result[T]{value: value, source: sourceLoad}, nil
}

// staleFallback finds a value past its hard deadline but within StaleIfError
// of it: the one just read from L2, else the one L1 still physically holds.
func (c *Cache[T]) staleFallback(key Key, heldPayload []byte, heldHardMs int64, heldOK bool) (T, bool) {
	var zero T

	if c.cfg.StaleIfError <= 0 {
		return zero, false
	}

	limitMs := time.Now().Add(-c.cfg.StaleIfError).UnixMilli()

	if heldOK && heldHardMs > limitMs {
		if value, err := c.codec.Decode(heldPayload); err == nil {
			return value, true
		}
	}

	env, found := c.local.HasGet(nil, key[:])
	if !found {
		return zero, false
	}

	if _, hardMs, payload, ok := parseEnvelope(env); ok && hardMs > limitMs {
		if value, err := c.codec.Decode(payload); err == nil {
			return value, true
		}
	}

	return zero, false
}

// fill writes an envelope to L1 unless an invalidation arrived since epoch.
func (c *Cache[T]) fill(epoch uint64, key Key, env []byte) {
	if len(key)+len(env) > l1MaxEntry {
		c.metrics.oversize.Inc()

		return
	}

	c.fillMu.RLock()
	defer c.fillMu.RUnlock()

	if c.epoch.Load() == epoch {
		c.local.Set(key[:], env)
	}
}

func (c *Cache[T]) invalidateLocal(key Key) {
	c.fillMu.Lock()
	defer c.fillMu.Unlock()

	c.epoch.Add(1)
	c.local.Del(key[:])
	c.metrics.invalidatedKey.Inc()
}

// reset empties L1: invalidations may have been missed.
func (c *Cache[T]) reset() {
	c.fillMu.Lock()
	defer c.fillMu.Unlock()

	c.epoch.Add(1)
	c.local.Reset()
	c.metrics.invalidatedAll.Inc()
}

// refresh starts the background refresh of a stale key unless one is already
// running for it or the limit is reached. It is on the read path of every
// stale hit, so the busy case costs a map lookup and nothing else.
func (c *Cache[T]) refresh(ctx context.Context, key Key) {
	c.refreshMu.Lock()

	_, busy := c.refreshing[key]
	if busy || c.closed {
		c.refreshMu.Unlock()

		return
	}

	if len(c.refreshing) >= c.cfg.RefreshConcurrency {
		c.refreshMu.Unlock()
		c.metrics.refreshDropped.Inc()

		return
	}

	c.refreshing[key] = struct{}{}
	c.wg.Add(1) // under the lock that Close takes before it waits

	c.refreshMu.Unlock()

	go c.runRefresh(ctx, key) // detached on purpose: see runRefresh
}

// runRefresh is one background refresh: detached from the request that
// noticed the stale value (keeping its trace), bounded by RefreshTimeout and
// by the cache's lifetime.
func (c *Cache[T]) runRefresh(ctx context.Context, key Key) {
	defer c.wg.Done()
	defer func() {
		c.refreshMu.Lock()
		delete(c.refreshing, key)
		c.refreshMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.RefreshTimeout)
	defer cancel()

	defer context.AfterFunc(c.life, cancel)()

	// The same flight group as fetch: a refresh and a synchronous lookup of
	// one key (after an invalidation, say) are still a single load.
	_, err, _ := c.flight.Do(string(key[:]), func() (any, error) { return c.resolve(ctx, key, true) })
	if err != nil && !errors.Is(err, ErrNotFound) {
		c.metrics.refreshError.Inc()
		c.warn(ctx, "tiercache: background refresh failed, the stale value stays", "cache", c.cfg.Name, "err", err)

		return
	}

	c.metrics.refreshOK.Inc()
}

// subscribe applies the invalidations other processes publish. Pub/Sub is at
// most once, so every (re-)established subscription empties L1: whatever was
// published while it was down is unknown.
func (c *Cache[T]) subscribe() {
	retry := time.NewTimer(time.Hour)
	defer retry.Stop()

	for {
		err := c.remote.Subscribe(c.life, c.channel, c.onInvalidation, c.reset)
		if c.life.Err() != nil {
			return
		}

		c.warn(c.life, "tiercache: invalidation subscription ended, retrying", "cache", c.cfg.Name, "err", err)

		retry.Reset(time.Second)

		select {
		case <-retry.C:
		case <-c.life.Done():
			return
		}
	}
}

func (c *Cache[T]) onInvalidation(message string) {
	var key Key
	if len(message) != len(key) {
		return // not ours: the channel carries raw keys only
	}

	copy(key[:], message)
	c.invalidateLocal(key)
}

func (c *Cache[T]) remoteKey(key Key) string { return c.prefix + string(key[:]) }

// remoteGet reads L2 unless it is absent or its breaker is open; any failure
// is a miss.
func (c *Cache[T]) remoteGet(ctx context.Context, remoteKey string) (string, bool) {
	if c.remote == nil {
		return "", false
	}

	ticket, allowed := c.breaker.Allow()
	if !allowed {
		c.metrics.bypassed.Inc()

		return "", false
	}

	value, found, err := c.remote.Get(ctx, remoteKey)
	c.breaker.Record(ticket, outcome(err))

	return value, found && err == nil
}

// remoteStore writes a loaded value to L2 — over the value the lookup started
// from when there was one, into the empty key otherwise — and reports whether
// L1 may keep it too: yes when L2 took it or could not be asked, no when L2
// refused it.
func (c *Cache[T]) remoteStore(ctx context.Context, remoteKey, old, env string, ttl time.Duration, replace bool) bool {
	if c.remote == nil {
		return true
	}

	ticket, allowed := c.breaker.Allow()
	if !allowed {
		return true
	}

	var (
		stored bool
		err    error
	)

	if replace {
		stored, err = c.remote.Replace(ctx, remoteKey, old, env, ttl)
	} else {
		stored, err = c.remote.SetNX(ctx, remoteKey, env, ttl)
	}

	c.breaker.Record(ticket, outcome(err))

	return stored || err != nil
}

// outcome is what an L2 call tells the breaker: a caller that went away says
// nothing about L2; any other error — a timeout included — is its failure.
func outcome(err error) resilience.Outcome {
	switch {
	case err == nil:
		return resilience.OutcomeSuccess
	case errors.Is(err, context.Canceled):
		return resilience.OutcomeIgnored
	default:
		return resilience.OutcomeFailure
	}
}

// warn logs at most one warning a second per cache: an outage must be seen,
// and must not turn every read into a log line.
func (c *Cache[T]) warn(ctx context.Context, msg string, args ...any) {
	now := time.Now().Unix()
	if last := c.lastWarn.Load(); last == now || !c.lastWarn.CompareAndSwap(last, now) {
		return
	}

	c.log.WarnContext(ctx, msg, args...)
}
