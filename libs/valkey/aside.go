package valkey

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2" // nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- TTL jitter is not security-sensitive; a CSPRNG (crypto/rand) is unnecessary here.
	"time"
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

// Jitter returns ttl spread uniformly over ±10%, so keys filled together (a
// warm-up, a burst of misses after a deploy) do not all expire together and
// stampede the source of truth at once.
func Jitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	spread := int64(ttl) / 10
	if spread == 0 {
		return ttl
	}
	return ttl + time.Duration(rand.Int64N(2*spread+1)-spread) //nolint:gosec // jitter, not security
}

// Aside is the cache-aside (lazy-loading) pattern as a generic helper: it
// returns the JSON-decoded value at key on a hit, otherwise loads it and
// fills the cache. The boolean reports a hit.
//
//   - Concurrent misses on one key share a single load (singleflight): a hot
//     key expiring does not turn into N identical queries. Each caller still
//     honours its own context while it waits; the shared load runs detached
//     from the first caller's cancellation (keeping its deadline and trace),
//     so one impatient client cannot fail the others. Waiters receive the
//     same value — treat a shared pointer/slice/map T as read-only.
//   - The fill is a SET NX with ttl ±10% (see [Jitter]): it never overwrites
//     a tombstone ([Cache.Tombstone]), so a load that raced an invalidation
//     cannot resurrect stale data.
//   - A corrupt entry, a failed lookup or a failed fill is a miss / a warning:
//     the source of truth (load) always wins, so caching never turns a
//     readable value into an error. load's own error is returned as is.
func Aside[T any](ctx context.Context, c *Cache, key string, ttl time.Duration, load func(ctx context.Context) (T, error)) (T, bool, error) {
	var zero T
	raw, ok, err := c.Get(ctx, key)
	if err != nil {
		c.log.WarnContext(ctx, "valkey: lookup failed, loading from source", "key", key, "err", err)
	}
	if ok {
		var v T
		if json.Unmarshal([]byte(raw), &v) == nil {
			return v, true, nil
		}
		c.log.WarnContext(ctx, "valkey: discarding corrupt cache entry", "key", key)
	}

	ch := c.flight.DoChan(key, func() (any, error) {
		lctx := context.WithoutCancel(ctx)
		if dl, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			lctx, cancel = context.WithDeadline(lctx, dl)
			defer cancel()
		}
		v, err := load(lctx)
		if err != nil {
			return v, err
		}
		if b, err := json.Marshal(v); err != nil {
			c.log.WarnContext(lctx, "valkey: cannot encode value for the cache", "key", key, "err", err)
		} else if _, err := c.SetNX(lctx, key, string(b), Jitter(ttl)); err != nil {
			c.log.WarnContext(lctx, "valkey: cache write failed", "key", key, "err", err)
		}
		return v, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return zero, false, res.Err
		}
		return res.Val.(T), false, nil //nolint:forcetypeassert // the closure above only ever returns T
	case <-ctx.Done():
		return zero, false, ctx.Err()
	}
}
