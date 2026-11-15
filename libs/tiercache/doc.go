// Package tiercache is a two-level read cache for values that are read far
// more often than they change: a fixed-size L1 inside the process in front
// of a shared L2 (Valkey, through libs/valkey), in front of the source of
// truth (a [Loader]).
//
// It adds little of its own: the store, the client, singleflight, the TTL
// jitter and the tombstone scheme are the tested pieces it is assembled from.
// What it owns is the envelope and the read rules.
//
// # Reading
//
// Both levels hold the same bytes — an envelope with two deadlines in front
// of the codec's payload — so L1 needs no TTL support and nothing is ever
// evicted for age: an expired entry is ignored on read and overwritten by
// the ring buffer in due course.
//
//	age < SoftTTL            served from L1                     (no I/O, no allocation of ours)
//	SoftTTL ≤ age < HardTTL  served from L1, refreshed behind   (stale-while-revalidate)
//	HardTTL ≤ age            ignored: L2, then the loader       (one lookup per key)
//	  … and the source fails served for StaleIfError more       (off by default)
//
// SoftTTL == HardTTL is plain cache-aside.
//
// # Running without L2
//
// L1 is the always-on level. A nil [Remote], an unreachable or a slow one
// never fails a read: every L2 call has its own deadline, a circuit breaker
// skips L2 after consecutive failures, and the loader answers instead. What
// is lost is sharing between processes and invalidation — bounded by the TTL.
//
// # Invalidation
//
// [Cache.Invalidate] is for writers, after their transaction commits: it
// drops the key from this process's L1, tombstones it in L2 (which refuses
// the fills of readers that loaded the old row — see libs/valkey) and
// publishes it, so every other process drops it from its L1. Pub/Sub is at
// most once: every time the subscription is re-established, L1 is emptied.
// A read that begins after an invalidation was applied here never joins a
// lookup that began before it: once Invalidate has returned, this process
// reads its own write.
// Without [Cache.Invalidate] the cache is still correct up to HardTTL.
//
// # Keys
//
// A key is a string of 1 to [MaxKeyLen] bytes and travels as it is: to the
// loader, into the L2 key, onto the invalidation channel. A secret is never
// a key — [KeyOf] hashes it.
//
// # Memory
//
// L1 is a [Store], built by the [StoreFactory] given to [New] with the
// capacity of the configuration ([Config.MaxBytes]), fixed from then on.
// Two are provided, and the service names one. Package offheap
// (github.com/VictoriaMetrics/fastcache) is off the Go heap, so neither the
// garbage collector nor the Go memory limit counts it; sized in steps of
// 32 MiB; never returned to the OS (budget the full capacity as resident);
// entries of 64 KiB and more are not held. Package onheap
// (github.com/coocood/freecache) is on the heap at exactly the size asked
// for (512 KiB at least), counted by the memory limit, with entries of up to
// 1/1024 of the capacity. Either keeps an index of 16 bytes per entry on the
// heap on top. An entry too large for L1 is served from L2 and counted.
// Several logical caches can share one budget by sharing one cache and
// telling their keys apart by a prefix.
//
// A service may bring a store of its own: [Store] is the contract, and
// package storetest the suite that checks a store against it.
//
// A service typically does:
//
//	creds, err := tiercache.New(cfg.Credentials, offheap.New, credential.Codec{}, store.Credential, remote, logger)
//	if err != nil { … }
//	defer creds.Close()
//	srv.Metrics.Registry.MustRegister(creds.Collectors()...)
//
//	c, err := creds.Get(ctx, tiercache.KeyOf(apiKey))
//
//	// after the UPDATE / DELETE has committed:
//	if err := creds.Invalidate(ctx, tiercache.KeyOf(apiKey)); err != nil { log.WarnContext(ctx, …) }
package tiercache
