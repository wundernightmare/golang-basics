// Package valkey is the shared cache layer for golang-basics services — the
// Valkey (Redis-compatible) analogue of libs/pgx. It wraps the official
// [github.com/valkey-io/valkey-go] client with environment/YAML-driven config,
// a readiness check that plugs into libs/httpx, per-command metrics and
// tracing, and the cache-aside helpers services would otherwise re-implement:
//
//   - [Aside]: get-or-load with singleflight (one load per key however many
//     concurrent misses), a ±10% TTL jitter, and a fill that never overwrites
//     an invalidation;
//   - [Cache.Tombstone]: delete-safe invalidation for writes.
//
// # The invalidation scheme
//
// A cache-aside read is "GET; on a miss, load from the database; SET". The
// classic bug is the reader that loads the row just before a writer commits
// and writes it back just after the writer's DEL: the stale row — or a
// deleted one — lives in the cache for a full TTL. This package closes that
// window with two rules:
//
//  1. fills are SET NX (only when the key is absent), and
//  2. writers, after their transaction commits, replace the key with a
//     short-lived tombstone ([Cache.Tombstone]) instead of deleting it.
//
// A racing reader's write-back then finds the tombstone and is refused; reads
// during the tombstone's TTL go to the database, and the first one after it
// re-fills. The only remaining window is a reader stalled between its load
// and its write-back for longer than the tombstone TTL, so that TTL should be
// a few times the slowest load (seconds). If the tombstone write itself fails
// (the cache is down), staleness is bounded by the entry's TTL — log it.
//
// Valkey is the BSD-licensed fork of Redis; valkey-go is its first-party Go
// client (RESP3, automatic pipelining, opt-in client-side caching) and is the
// reliable, actively-maintained choice over the now relicensed redis clients.
//
// A service typically does:
//
//	c, err := valkey.New(cfg.Valkey, logger)
//	if err != nil { … }
//	defer c.Close()
//	srv.Health.Register("valkey", c.ReadyCheck(), httpx.Optional())
//	srv.Metrics.Registry.MustRegister(c.Collectors()...)
//
//	task, hit, err := valkey.Aside(ctx, c, "task:"+id, time.Minute,
//		func(ctx context.Context) (Task, error) { return store.Get(ctx, id) })
//
//	// after the UPDATE / DELETE has committed:
//	_ = c.Tombstone(ctx, "task:"+id, 5*time.Second)
package valkey
