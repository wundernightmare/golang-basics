package tiercache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

// ErrNotFound is what a [Loader] returns for a key the source of truth does
// not have, and what [Cache.Get] returns then. It is not cached.
var ErrNotFound = errors.New("tiercache: not found")

// ErrClosed is what [Cache.Get] and [Cache.Invalidate] return once
// [Cache.Close] has been called.
var ErrClosed = errors.New("tiercache: the cache is closed")

// ErrPanic is what [Cache.Get] returns to every caller of a lookup in which
// the [Loader], the [Codec] or the [Remote] panicked: the panic happened in
// the goroutine of the shared lookup, where no recover of the caller's would
// have caught it. The error carries the panic's value; its stack is logged.
var ErrPanic = errors.New("tiercache: panic in a lookup")

// MaxKeyLen is the longest key, in bytes. A UUID as text is 36; in UTF-8 the
// limit is 512 Latin characters, 256 Cyrillic ones, 170 CJK ones, 128 emoji.
const MaxKeyLen = 512

// ErrKey is what [Cache.Get] and [Cache.Invalidate] return for a key that is
// empty or longer than [MaxKeyLen].
var ErrKey = errors.New("tiercache: a key is 1 to 512 bytes")

// KeyOf is the key for an identifier that is a secret (an API key, a token)
// or may be longer than [MaxKeyLen]: its SHA-256, 32 bytes.
//
// A key is any string of 1 to [MaxKeyLen] bytes — an id, a UUID, a
// "tenant:42" — and travels as it is: it is the loader's argument, a part of
// the L2 key ("tc:<name>:<key>") and the message on the invalidation
// channel. So a key is visible to whoever can read Valkey (MONITOR, the slow
// log, a dump): never make one out of a secret, hash the secret.
func KeyOf(s string) string {
	sum := sha256.Sum256([]byte(s))

	return string(sum[:])
}

func validKey(key string) bool { return key != "" && len(key) <= MaxKeyLen }

// Cache is a two-level read cache: a fixed-size L1 in the process
// in front of a shared L2 ([Remote]), in front of the source of truth
// ([Loader]). See the package documentation for the read rules. It is safe
// for concurrent use; build one with [New] and [Cache.Close] it.
type Cache[T any] struct {
	cfg     Config
	local   Store
	remote  Remote // nil: L1 and the loader only
	codec   Codec[T]
	load    Loader[T]
	log     *slog.Logger
	metrics *metrics
	breaker *resilience.Breaker // keeps a failing L2 from costing every miss a command timeout
	// flight shares one lookup between the callers that ask for a key while
	// it runs. An invalidation makes the key's lookup unjoinable (and a
	// reset, every lookup, by replacing the group): whoever asks afterwards
	// starts a new one, because the one in flight may hold the row the
	// write replaced. Those already waiting still get its answer.
	flight  atomic.Pointer[singleflight.Group]
	buffers sync.Pool // *[]byte

	prefix  string
	channel string

	// longestMs is the furthest ahead, in milliseconds, a hard deadline can
	// be when this process sets it: HardTTL with all of its jitter. One
	// further away was set by another clock — a process whose clock runs
	// ahead, or this one before its clock was stepped back — and says
	// nothing about the value's age: it is not trusted (see trusted).
	longestMs int64

	// fillMu orders L1 fills against invalidations: a fill holds it shared
	// and checks that its key's epoch has not moved since its read began; an
	// invalidation moves the epoch and removes the entry under the exclusive
	// lock. Without it a value read just before an invalidation could be
	// written to L1 just after it and live there for a full TTL.
	//
	// An epoch is shared by the keys that hash to one of epochShards, so an
	// invalidation costs the lookups in flight of its own key and of one key
	// in epochShards of the others their L1 fill; resets is the epoch of
	// every key at once.
	fillMu sync.RWMutex
	seed   maphash.Seed
	epochs [epochShards]atomic.Uint64
	resets atomic.Uint64

	// A lookup or a refresh is added to wg under refreshMu, and Close sets
	// closed under it before it waits: none starts behind Close's back.
	// closed is atomic for Get alone, which reads it without the lock.
	refreshMu  sync.Mutex
	refreshing map[string]struct{}
	closed     atomic.Bool

	lastWarn atomic.Int64 // Unix seconds of the last rate-limited warning

	life context.Context //nolint:containedctx // the cache's own lifetime: cancelled by Close to stop its goroutines
	stop context.CancelFunc
	wg   sync.WaitGroup
}

const epochShards = 256

// epoch is what a lookup remembers of the invalidations when it begins.
type epoch struct{ key, all uint64 }

func (c *Cache[T]) shard(key string) uint64 { return maphash.String(c.seed, key) % epochShards }

func (c *Cache[T]) epochOf(key string) epoch {
	return epoch{key: c.epochs[c.shard(key)].Load(), all: c.resets.Load()}
}

// New builds a cache on the L1 store makes (offheap.New, onheap.New — see
// [Store]). remote may be nil (see [Remote]); log may be nil. With
// a remote and invalidation enabled it starts the subscription goroutine,
// which [Cache.Close] stops. The remote stays the caller's to close.
func New[T any](cfg Config, store StoreFactory, codec Codec[T], load Loader[T], remote Remote, log *slog.Logger) (*Cache[T], error) {
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if store == nil || codec == nil || load == nil {
		return nil, errors.New("tiercache: store, codec and loader are required")
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	local := store(cfg.MaxBytes)
	if local == nil {
		return nil, errors.New("tiercache: the store factory returned no store")
	}

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
		seed:       maphash.MakeSeed(),
		longestMs:  int64((cfg.HardTTL + cfg.HardTTL/10 + time.Millisecond - 1) / time.Millisecond), // rounded up: see resilience.Jitter
		refreshing: make(map[string]struct{}, cfg.RefreshConcurrency),
		life:       life,
		stop:       stop,
	}

	cache.flight.Store(new(singleflight.Group))

	// Not through warn: the breaker opens once per OpenTimeout at most, and
	// the line must not be lost to another warning of the same second.
	cache.breaker = resilience.NewBreaker(cfg.Breaker, time.Now, func(_, to resilience.State) {
		if to == resilience.StateOpen {
			log.WarnContext(life, "tiercache: L2 is failing, skipping it", "cache", cfg.Name, "for", cfg.Breaker.OpenTimeout.String())
		}
	})

	if remote != nil && !cfg.DisableInvalidation {
		cache.wg.Go(cache.subscribe)
	}

	return cache, nil
}

// Close stops the subscription, cancels the lookups and background refreshes
// in flight and waits for them — for as long as a [Loader] that ignores its
// context takes — then hands L1's memory back for reuse by other caches of
// the process. Afterwards [Cache.Get] and [Cache.Invalidate] return
// [ErrClosed]; a second Close does nothing.
func (c *Cache[T]) Close() {
	c.refreshMu.Lock()
	already := c.closed.Swap(true)
	c.refreshMu.Unlock()

	if already {
		return
	}

	c.stop()
	c.wg.Wait()
	c.local.Reset()
}

// begin counts one more lookup for Close to wait for, unless the cache is
// closed; the caller ends it with wg.Done.
func (c *Cache[T]) begin() bool {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	if c.closed.Load() {
		return false
	}

	c.wg.Add(1)

	return true
}

// Get returns the value for key.
//
//   - Younger than SoftTTL in L1: returned as is. This path does no I/O, takes
//     no exclusive lock and allocates only what the codec's Decode does.
//   - Between SoftTTL and HardTTL: returned as is, and refreshed in the
//     background — at most one refresh per key, RefreshConcurrency in all.
//   - Past HardTTL or absent: ignored. The value comes from L2 or, failing
//     that, from the loader; concurrent callers for one key share one such
//     lookup, each honouring its own ctx while it waits. The lookup runs
//     until the deadline of the caller that started it; a caller that joined
//     and has time left when that deadline ends the lookup starts another.
//
// An unreachable L2 never fails a read: the loader answers, L1 keeps what it
// has. When the loader fails too, a value past HardTTL is served for
// StaleIfError more; beyond that the loader's error is returned. A panic in
// the loader, the codec or the remote is returned as [ErrPanic].
func (c *Cache[T]) Get(ctx context.Context, key string) (T, error) {
	if !validKey(key) || c.closed.Load() {
		var zero T

		c.metrics.failed.Inc()

		if validKey(key) {
			return zero, ErrClosed
		}

		return zero, ErrKey
	}

	nowMs := time.Now().UnixMilli()

	buf := c.buffers.Get().(*[]byte) //nolint:forcetypeassert // the pool only ever holds *[]byte
	env, found := c.local.Get((*buf)[:0], key)
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
	if cap(*buf) <= 2*c.local.MaxEntry() { // twice: append may over-allocate for the largest entry L1 holds
		c.buffers.Put(buf)
	}
}

// usable decodes an L1 entry that is within its hard deadline; anything else
// — expired, not an envelope, a payload the codec refuses — is a miss.
func (c *Cache[T]) usable(env []byte, nowMs int64) (value T, stale, ok bool) {
	softMs, hardMs, payload, valid := parseEnvelope(env)
	if !valid || nowMs >= hardMs || !c.trusted(hardMs, nowMs) {
		return value, false, false
	}

	value, err := c.codec.Decode(payload)

	return value, nowMs >= softMs, err == nil
}

// trusted reports whether a hard deadline is one this process could have set
// at nowMs or earlier. The deadlines in an envelope are wall-clock instants,
// and an envelope in L2 was written by another process: without this, a
// writer whose clock runs ahead by an hour gives every reader a value that
// stays fresh for an hour, in their L1, whatever HardTTL says. An untrusted
// envelope is a miss — the source answers and the entry is replaced.
//
// The other direction needs no rule: a deadline from a clock that runs
// behind is merely nearer, and the value is reloaded sooner.
func (c *Cache[T]) trusted(hardMs, nowMs int64) bool {
	return hardMs-nowMs <= c.longestMs
}

// Invalidate drops key everywhere after a write to the source of truth has
// committed: from this process's L1 at once, from L2 by a tombstone that
// also refuses fills racing the write (see libs/valkey), and from every other
// process's L1 through the invalidation channel.
//
// The local part cannot fail, and holds whatever the rest does: once
// Invalidate has returned, this process does not read the value from before
// it again. An error means L2 or the channel could not be reached: other
// processes then keep the old value until its TTL (or until their
// subscription reconnects, which empties their L1) — log it.
func (c *Cache[T]) Invalidate(ctx context.Context, key string) error {
	switch {
	case !validKey(key):
		return ErrKey
	case c.closed.Load():
		return ErrClosed
	}

	c.metrics.invalidatedKey.Inc()
	c.drop(key)

	if c.remote == nil {
		return nil
	}

	// The breaker is neither consulted nor told: an invalidation is worth
	// one attempt even while reads are skipping L2.
	tombErr := c.remote.Tombstone(ctx, c.remoteKey(key), c.cfg.TombstoneTTL)

	// Once more: a reader of this process that asked L2 while the tombstone
	// was on its way found the old value there, and the first drop was
	// before its fill. From here on L2 answers with the tombstone.
	c.drop(key)

	var pubErr error
	if !c.cfg.DisableInvalidation {
		pubErr = c.remote.Publish(ctx, c.channel, key)
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
// callers. The shared lookup runs detached from the first caller's
// cancellation, keeping its deadline and trace: a caller that leaves does
// not fail the others. Its deadline can: the lookup then ends with a context
// error that is not the joiners', and one of them with time left asks again
// — as the first caller of the next lookup, bounded by its own deadline.
func (c *Cache[T]) fetch(ctx context.Context, key string) (T, error) {
	var zero T

	for {
		// Written by the lookup's goroutine before it sends the result,
		// read here only after receiving it.
		led := false

		shared := c.flight.Load().DoChan(key, func() (any, error) {
			led = true

			return c.lookup(ctx, key)
		})

		select {
		case res := <-shared:
			ended := errors.Is(res.Err, context.DeadlineExceeded) || errors.Is(res.Err, context.Canceled)
			if ended && !led && ctx.Err() == nil {
				continue
			}

			return c.answer(res)
		case <-ctx.Done():
			c.metrics.failed.Inc()

			return zero, ctx.Err()
		}
	}
}

// lookup is the body of one shared lookup, in the goroutine singleflight
// gives it: bounded by the deadline of the caller that started it and by the
// cache's lifetime, and waited for by Close.
func (c *Cache[T]) lookup(ctx context.Context, key string) (any, error) {
	if !c.begin() {
		return nil, ErrClosed
	}
	defer c.wg.Done()

	deadline, bounded := ctx.Deadline()

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	defer context.AfterFunc(c.life, cancel)()

	if bounded {
		var expire context.CancelFunc

		ctx, expire = context.WithDeadline(ctx, deadline)
		defer expire()
	}

	return c.guarded(ctx, key, false)
}

// guarded is resolve with a panic of the loader, the codec or the remote
// turned into the lookup's error. It runs in a goroutine of the cache's own —
// singleflight's, or a refresh's — where a panic would end the process
// whatever the callers recover.
func (c *Cache[T]) guarded(ctx context.Context, key string, refreshing bool) (res result[T], err error) {
	defer func() {
		if cause := recover(); cause != nil {
			c.log.ErrorContext(ctx, "tiercache: panic in a lookup", "cache", c.cfg.Name, "panic", cause, "stack", string(debug.Stack()))

			err = fmt.Errorf("%w: %v", ErrPanic, cause)
		}
	}()

	return c.resolve(ctx, key, refreshing)
}

// answer turns the result of a shared lookup into one caller's return
// values, and counts the read.
func (c *Cache[T]) answer(res singleflight.Result) (T, error) {
	if res.Err != nil {
		var zero T

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
}

// resolve looks key up in L2 and then in the source of truth, and fills the
// levels above whatever answered. refreshing is the background variant: a
// stale L2 value is not an answer for it (that is what it is replacing), and
// it never falls back to an expired one.
func (c *Cache[T]) resolve(ctx context.Context, key string, refreshing bool) (result[T], error) {
	var none result[T]

	began := c.epochOf(key)
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

	nowMs := time.Now().UnixMilli()
	heldOK = heldOK && c.trusted(heldHard, nowMs) // from a clock ahead of this one: as good as absent

	if heldOK && nowMs < heldHard && (!refreshing || nowMs < heldSoftMs) {
		if value, err := c.codec.Decode(heldPayload); err == nil {
			c.fill(began, key, heldEnv)

			return result[T]{value: value, source: sourceL2}, nil
		}

		heldOK = false // corrupt payload: as good as absent
	}

	value, err := c.load(ctx, key)

	switch {
	case errors.Is(err, ErrNotFound):
		// Gone from the source: stop serving what L1 and L2 still have. Not
		// an invalidation — nothing was written, and no lookup in flight
		// holds a row this one makes older.
		c.local.Delete(key)

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
	hardTTL := resilience.Jitter(c.cfg.HardTTL)

	// Without a soft TTL of its own a value is never stale: the two
	// deadlines are one instant, not two draws of the jitter.
	softTTL := hardTTL
	if c.cfg.SoftTTL < c.cfg.HardTTL {
		softTTL = min(resilience.Jitter(c.cfg.SoftTTL), hardTTL)
	}

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
	// L2 keeps the entry for StaleIfError past its hard deadline: that is
	// where staleFallback finds it when the source fails.
	if c.remoteStore(ctx, remoteKey, held, string(env), hardTTL+c.cfg.StaleIfError, heldFound) {
		c.fill(began, key, env)
	}

	return result[T]{value: value, source: sourceLoad}, nil
}

// staleFallback finds a value past its hard deadline but within StaleIfError
// of it: the one just read from L2, else the one L1 still physically holds —
// either only with a deadline this process could have set (see trusted).
func (c *Cache[T]) staleFallback(key string, heldPayload []byte, heldHardMs int64, heldOK bool) (T, bool) {
	var zero T

	if c.cfg.StaleIfError <= 0 {
		return zero, false
	}

	nowMs := time.Now().UnixMilli()
	limitMs := nowMs - c.cfg.StaleIfError.Milliseconds()

	if heldOK && heldHardMs > limitMs {
		if value, err := c.codec.Decode(heldPayload); err == nil {
			return value, true
		}
	}

	env, found := c.local.Get(nil, key)
	if !found {
		return zero, false
	}

	if _, hardMs, payload, ok := parseEnvelope(env); ok && hardMs > limitMs && c.trusted(hardMs, nowMs) {
		if value, err := c.codec.Decode(payload); err == nil {
			return value, true
		}
	}

	return zero, false
}

// fill writes an envelope to L1 unless an invalidation arrived since began.
// One too large for L1 is not offered to it, and takes the key's previous
// envelope out: the value is served from L2 from now on, not an older one
// from L1 until its deadline.
func (c *Cache[T]) fill(began epoch, key string, env []byte) {
	if len(key)+len(env) > c.local.MaxEntry() {
		c.metrics.oversize.Inc()
		c.local.Delete(key)

		return
	}

	c.fillMu.RLock()
	defer c.fillMu.RUnlock()

	if c.epochOf(key) == began {
		c.local.Set(key, env)
	}
}

// drop takes key out of L1 and keeps out what the lookups now in flight for
// it have read; whoever asks next starts a lookup of their own.
func (c *Cache[T]) drop(key string) {
	c.fillMu.Lock()
	defer c.fillMu.Unlock()

	c.epochs[c.shard(key)].Add(1)
	c.local.Delete(key)
	c.flight.Load().Forget(key)
}

// reset empties L1: invalidations may have been missed.
func (c *Cache[T]) reset() {
	c.fillMu.Lock()
	defer c.fillMu.Unlock()

	c.resets.Add(1)
	c.local.Reset()
	c.flight.Store(new(singleflight.Group))
	c.metrics.invalidatedAll.Inc()
}

// refresh starts the background refresh of a stale key unless one is already
// running for it or the limit is reached. It is on the read path of every
// stale hit, so the busy case costs a map lookup and nothing else.
func (c *Cache[T]) refresh(ctx context.Context, key string) {
	c.refreshMu.Lock()

	_, busy := c.refreshing[key]
	if busy || c.closed.Load() {
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
func (c *Cache[T]) runRefresh(ctx context.Context, key string) {
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
	_, err, _ := c.flight.Load().Do(key, func() (any, error) { return c.guarded(ctx, key, true) })
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
//
// Subscribe reconnects by itself (libs/valkey) and returns only when the
// subscription has ended for good; the loop then starts another, after a
// pause, so that a remote which fails at once is not called in a spin.
func (c *Cache[T]) subscribe() {
	retry := time.NewTimer(time.Hour) // one timer for every pause; Reset arms it, the hour is never waited for
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
	if !validKey(message) {
		return // not ours: the channel carries keys only
	}

	c.metrics.invalidatedKey.Inc()
	c.drop(message)
}

func (c *Cache[T]) remoteKey(key string) string { return c.prefix + key }

// remoteGet reads L2 unless it is absent or its breaker is open; any failure
// is a miss.
func (c *Cache[T]) remoteGet(ctx context.Context, remoteKey string) (string, bool) {
	if c.remote == nil {
		return "", false
	}

	var (
		value string
		found bool
		err   error
	)

	skipped := c.breaker.Do(func() resilience.Outcome {
		value, found, err = c.remote.Get(ctx, remoteKey)

		return outcome(ctx, err)
	})
	if skipped != nil {
		c.metrics.bypassed.Inc()

		return "", false
	}

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

	var (
		stored bool
		err    error
	)

	skipped := c.breaker.Do(func() resilience.Outcome {
		if replace {
			stored, err = c.remote.Replace(ctx, remoteKey, old, env, ttl)
		} else {
			stored, err = c.remote.SetNX(ctx, remoteKey, env, ttl)
		}

		return outcome(ctx, err)
	})
	if skipped != nil {
		return true
	}

	return stored || err != nil
}

// outcome is what an L2 call tells the breaker. A call made on a context
// that is done — the lookup's caller left, or its deadline passed — says
// nothing about L2; an error while the lookup still had time is L2's
// failure, its own timeout (the remote's per-call deadline) included.
func outcome(ctx context.Context, err error) resilience.Outcome {
	if err == nil {
		return resilience.OutcomeSuccess
	}

	if ctx.Err() != nil {
		return resilience.OutcomeIgnored
	}

	return resilience.OutcomeFailure
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
