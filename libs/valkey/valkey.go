package valkey

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

// Cache wraps a valkey-go client with the conveniences a service needs: string
// and JSON get/set with TTL, a readiness probe and cache-aside loading. It is
// safe for concurrent use; construct one with [New] and share it.
type Cache struct {
	client valkeygo.Client
	log    *slog.Logger
}

// New builds a client from cfg, verifies connectivity with a single PING (so a
// misconfigured cache fails fast at boot) and returns a ready [Cache]. The
// caller owns it and must call [Cache.Close].
func New(cfg Config, log *slog.Logger) (*Cache, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	opt, err := cfg.clientOption()
	if err != nil {
		return nil, err
	}
	client, err := valkeygo.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("valkey: connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()
	if err := client.Do(pingCtx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("valkey: initial ping: %w", err)
	}

	log.Info("valkey cache ready", "addr", cfg.Addr, "db", cfg.DB)
	return &Cache{client: client, log: log}, nil
}

// Client exposes the underlying valkey-go client for commands this wrapper does
// not surface.
func (c *Cache) Client() valkeygo.Client { return c.client }

// Get returns the value at key. The boolean is false on a cache miss (a missing
// key is not an error); a non-nil error means the lookup itself failed.
func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.client.Do(ctx, c.client.B().Get().Key(key).Build()).ToString()
	if valkeygo.IsValkeyNil(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("valkey: get %q: %w", key, err)
	}
	return v, true, nil
}

// Set writes value at key. A positive ttl sets an expiry; a zero ttl stores the
// key without one.
func (c *Cache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	var cmd valkeygo.Completed
	if ttl > 0 {
		cmd = c.client.B().Set().Key(key).Value(value).Ex(ttl).Build()
	} else {
		cmd = c.client.B().Set().Key(key).Value(value).Build()
	}
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey: set %q: %w", key, err)
	}
	return nil
}

// Del removes one or more keys, ignoring those that do not exist.
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.client.Do(ctx, c.client.B().Del().Key(keys...).Build()).Error(); err != nil {
		return fmt.Errorf("valkey: del: %w", err)
	}
	return nil
}

// ReadyCheck returns a readiness probe (a libs/httpx CheckFunc) that PINGs the
// cache with a short timeout.
func (c *Cache) ReadyCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := c.client.Do(ctx, c.client.B().Ping().Build()).Error(); err != nil {
			return fmt.Errorf("valkey unreachable: %w", err)
		}
		return nil
	}
}

// Close releases the client's connection pool.
func (c *Cache) Close() { c.client.Close() }

// Aside is the cache-aside (lazy-loading) pattern as a generic helper: it
// returns the JSON-decoded value at key on a hit, otherwise calls load, stores
// the result under key with ttl, and returns it. A corrupt cache entry or a
// failed write is treated as a miss — the source of truth (load) always wins,
// so caching never turns a readable value into an error.
func Aside[T any](ctx context.Context, c *Cache, key string, ttl time.Duration, load func(ctx context.Context) (T, error)) (T, bool, error) {
	var zero T
	if raw, ok, err := c.Get(ctx, key); err == nil && ok {
		var v T
		if json.Unmarshal([]byte(raw), &v) == nil {
			return v, true, nil // cache hit
		}
		c.log.Warn("valkey: discarding corrupt cache entry", "key", key)
	}

	v, err := load(ctx)
	if err != nil {
		return zero, false, err
	}
	if b, err := json.Marshal(v); err == nil {
		if err := c.Set(ctx, key, string(b), ttl); err != nil {
			c.log.Warn("valkey: cache write failed", "key", key, "err", err)
		}
	}
	return v, false, nil // cache miss, loaded fresh
}
