package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

// tombstoneValue marks a key as "recently invalidated". [Cache.Get] reports it
// as a miss, and [Cache.SetNX] (so [Aside]) cannot overwrite it; it is not
// valid JSON, so no cached value can collide with it.
const tombstoneValue = "\x00valkey:tombstone"

// Tombstone invalidates key for ttl: the current value (if any) is replaced by
// a marker that reads as a miss and that cache fills ([Aside], [Cache.SetNX])
// cannot overwrite until it expires. Call it after the write to the source of
// truth has committed.
//
// Why not a plain DEL: a reader that loaded the old row just before the
// commit writes it back just after the DEL, and the stale value lives for a
// full TTL — worse, a deleted row is resurrected. With a tombstone that
// write-back is refused (it is a SET NX). The window left is a reader that
// sits between its load and its write-back for longer than ttl, so ttl
// should be a few times the slowest load (seconds, not milliseconds). Reads
// during ttl go to the source of truth; the first one after it re-fills.
func (c *Cache) Tombstone(ctx context.Context, key string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("valkey: tombstone %q: ttl must be positive", key)
	}

	if err := c.Set(ctx, key, tombstoneValue, ttl); err != nil {
		return fmt.Errorf("valkey: tombstone: %w", err)
	}

	return nil
}

// ErrLoadPanic is the error of an [Aside] call whose loader panicked.
var ErrLoadPanic = errors.New("valkey: loader panicked")

// Aside is the cache-aside (lazy-loading) pattern as a generic helper: it
// returns the JSON-decoded value at key on a hit, otherwise loads it and
// fills the cache. The boolean reports a hit.
//
//   - Concurrent misses on one key share a single load (singleflight): a hot
//     key expiring does not turn into N identical queries. Each caller still
//     honours its own context while it waits; the shared load runs detached
//     from the first caller's cancellation (keeping its deadline and trace),
//     so one impatient client cannot fail the others. That deadline is the
//     first caller's alone: a caller that joined the load and still has time
//     when it runs out loads again, once, under its own context. Waiters
//     receive the same value — treat a shared pointer/slice/map T as
//     read-only. One key is one T: a caller that joins a load of another
//     type gets an error.
//   - The fill is a SET NX with ttl ±10% ([resilience.Jitter]): it never
//     overwrites a tombstone ([Cache.Tombstone]), so a load that raced an
//     invalidation cannot resurrect stale data.
//   - A corrupt entry, a failed lookup or a failed fill is a miss / a warning:
//     the source of truth (load) always wins, so caching never turns a
//     readable value into an error. A corrupt entry is replaced by the loaded
//     value (compare-and-set against the bytes that were read, so still never
//     over a tombstone). After a failed lookup the fill is skipped: a cache
//     that does not answer costs the caller one OpTimeout, not two. The
//     warnings of a failing cache are limited to one per ten seconds each,
//     with the number left out; cache_command_errors_total has the rate.
//   - load's own error is returned as is. A panic in load is recovered,
//     logged with its stack, and returned to every waiter as an error
//     wrapping [ErrLoadPanic]: it would otherwise surface on a goroutine no
//     recover of the caller's reaches, and end the process.
//   - A caller whose context is over when the lookup fails gets its
//     context's error: nothing is loaded and nothing is logged as a cache
//     failure.
//   - key is written whole into the log lines and the error texts of Aside:
//     it must not carry a secret or personal data.
func Aside[T any](ctx context.Context, cache *Cache, key string, ttl time.Duration, load func(ctx context.Context) (T, error)) (T, bool, error) {
	var zero T

	v, hit, fill, err := lookup[T](ctx, cache, key)
	if hit || err != nil {
		return v, hit, err
	}

	for {
		// Written by the load's goroutine before it sends the result, read
		// here only after receiving it.
		led := false

		result := cache.flight.DoChan(key, func() (any, error) {
			led = true

			return loadAndFill(ctx, cache, key, ttl, load, fill)
		})
		select {
		case res := <-result:
			// The load this caller joined ran out of its starter's time,
			// not of this caller's.
			if !led && errors.Is(res.Err, context.DeadlineExceeded) && ctx.Err() == nil {
				continue
			}

			if res.Err != nil {
				return zero, false, res.Err
			}

			v, ok := res.Val.(T)
			if !ok {
				return zero, false, fmt.Errorf("valkey: aside %q: a concurrent load of this key returned %T, not %T", key, res.Val, zero)
			}

			return v, false, nil
		case <-ctx.Done():
			return zero, false, ctx.Err()
		}
	}
}

// fillFunc writes a loaded value to the cache; nil means "do not".
type fillFunc func(ctx context.Context, key, value string, ttl time.Duration)

// lookup is the read half of [Aside]: the hit, or how a load is to fill the
// cache afterwards.
func lookup[T any](ctx context.Context, cache *Cache, key string) (v T, hit bool, fill fillFunc, err error) {
	raw, found, err := cache.Get(ctx, key)

	switch {
	case err != nil && ctx.Err() != nil:
		return v, false, nil, ctx.Err()
	case err != nil:
		cache.lookupWarn.log(ctx, cache.log, "valkey: lookup failed, loading from source", "key", key, "err", err)

		return v, false, nil, nil
	case !found:
		return v, false, cache.fillNew, nil
	}

	if json.Unmarshal([]byte(raw), &v) == nil {
		return v, true, nil, nil
	}

	cache.log.WarnContext(ctx, "valkey: discarding corrupt cache entry", "key", key)

	var zero T

	return zero, false, func(ctx context.Context, key, value string, ttl time.Duration) {
		cache.fillOver(ctx, key, raw, value, ttl)
	}, nil
}

// loadAndFill is the shared half of [Aside], run once per flight: detached
// from the starter's cancellation, under the starter's deadline.
func loadAndFill[T any](ctx context.Context, cache *Cache, key string, ttl time.Duration, load func(ctx context.Context) (T, error), fill fillFunc) (T, error) {
	lctx := context.WithoutCancel(ctx)
	if dl, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc

		lctx, cancel = context.WithDeadline(lctx, dl)
		defer cancel()
	}

	v, err := guarded(lctx, cache, key, load)
	if err != nil || fill == nil {
		return v, err
	}

	if b, err := json.Marshal(v); err != nil {
		cache.log.WarnContext(lctx, "valkey: cannot encode value for the cache", "key", key, "err", err)
	} else {
		fill(lctx, key, string(b), resilience.Jitter(ttl))
	}

	return v, nil
}

// guarded runs load and turns its panic into an error.
func guarded[T any](ctx context.Context, cache *Cache, key string, load func(ctx context.Context) (T, error)) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			cache.log.ErrorContext(ctx, "valkey: loader panicked", "key", key, "panic", r, "stack", string(debug.Stack()))

			err = fmt.Errorf("valkey: aside %q: %w: %v", key, ErrLoadPanic, r)
		}
	}()

	return load(ctx)
}

// fillNew is the fill after a miss.
func (c *Cache) fillNew(ctx context.Context, key, value string, ttl time.Duration) {
	if _, err := c.SetNX(ctx, key, value, ttl); err != nil {
		c.fillWarn.log(ctx, c.log, "valkey: cache write failed", "key", key, "err", err)
	}
}

// fillOver is the fill over an entry that did not decode: it replaces
// exactly the bytes that were read.
func (c *Cache) fillOver(ctx context.Context, key, corrupt, value string, ttl time.Duration) {
	if _, err := c.Replace(ctx, key, corrupt, value, ttl); err != nil {
		c.fillWarn.log(ctx, c.log, "valkey: cache write failed", "key", key, "err", err)
	}
}
