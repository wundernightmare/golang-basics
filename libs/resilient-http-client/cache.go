package resilient

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// CachedResponse is the minimal response shape stored in the cache: status code
// and body only. Headers are deliberately omitted so sensitive values such as
// Set-Cookie are never cached.
//
// Body is owned by the cache once stored; callers must treat a CachedResponse
// returned from a cache hit as read-only and must not mutate Body.
type CachedResponse struct {
	// Status is the HTTP status code (e.g. 200).
	Status int
	// Body is the full response body.
	Body []byte
}

// CacheAdapter is a pluggable read-through cache backend used by
// [Client.SendCached], [Client.SendCoalesced] and [Client.SendWithFallback].
//
// Implementations must be safe for concurrent use. Get reports a miss (and any
// internal error) by returning ok == false — the client then falls through to
// the upstream, so cache failures never surface as request failures.
type CacheAdapter interface {
	// Get returns the cached response for key, or ok == false on a miss.
	Get(ctx context.Context, key string) (resp CachedResponse, ok bool)
	// Set stores resp under key with the given time-to-live.
	Set(ctx context.Context, key string, resp CachedResponse, ttl time.Duration)
}

// InMemoryCache is an in-process [CacheAdapter] with LRU eviction and per-entry
// TTL. It is safe for concurrent use.
type InMemoryCache struct {
	mu         sync.Mutex
	ll         *list.List // front = most-recently used
	items      map[string]*list.Element
	maxEntries int
	defaultTTL time.Duration
}

type cacheEntry struct {
	key       string
	resp      CachedResponse
	expiresAt time.Time
}

// NewInMemoryCache builds a cache holding at most maxEntries entries (LRU
// eviction beyond that). defaultTTL is used when [InMemoryCache.Set] is called
// with a non-positive ttl.
func NewInMemoryCache(maxEntries int, defaultTTL time.Duration) *InMemoryCache {
	if maxEntries < 1 {
		maxEntries = 1
	}
	return &InMemoryCache{
		ll:         list.New(),
		items:      make(map[string]*list.Element, maxEntries),
		maxEntries: maxEntries,
		defaultTTL: defaultTTL,
	}
}

// Get returns the cached response for key. Expired entries are evicted on access
// and reported as a miss.
func (c *InMemoryCache) Get(_ context.Context, key string) (CachedResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return CachedResponse{}, false
	}
	ent := el.Value.(*cacheEntry)
	if time.Now().After(ent.expiresAt) {
		c.removeElement(el)
		return CachedResponse{}, false
	}
	c.ll.MoveToFront(el)
	return ent.resp, true
}

// Set stores resp under key. The body is copied so the cache owns its data
// independently of the caller's buffer.
func (c *InMemoryCache) Set(_ context.Context, key string, resp CachedResponse, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	stored := CachedResponse{Status: resp.Status, Body: append([]byte(nil), resp.Body...)}
	exp := time.Now().Add(ttl)

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		ent := el.Value.(*cacheEntry)
		ent.resp = stored
		ent.expiresAt = exp
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&cacheEntry{key: key, resp: stored, expiresAt: exp})
	c.items[key] = el
	if c.ll.Len() > c.maxEntries {
		if back := c.ll.Back(); back != nil {
			c.removeElement(back)
		}
	}
}

// Len reports the current number of cached entries (including not-yet-evicted
// expired ones). Intended for tests and diagnostics.
func (c *InMemoryCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// removeElement drops el from both the list and the index. Caller holds c.mu.
func (c *InMemoryCache) removeElement(el *list.Element) {
	c.ll.Remove(el)
	delete(c.items, el.Value.(*cacheEntry).key)
}
