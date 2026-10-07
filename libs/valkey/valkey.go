package valkey

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeyotel"
	"golang.org/x/sync/singleflight"
)

// Defaults applied when a hand-built Config leaves a field zero.
const (
	defaultOpTimeout   = 500 * time.Millisecond
	defaultDialTimeout = 5 * time.Second
)

// Cache wraps a valkey-go client with the conveniences a service needs: string
// get/set with TTL, a readiness probe, cache-aside loading ([Aside]) and
// delete-safe invalidation ([Cache.Tombstone]). It is safe for concurrent
// use; construct one with [New] and share it.
//
// Every command runs under an OpenTelemetry client span (child of the span in
// the calling context) via valkeyotel, is bounded by Config.OpTimeout, and is
// measured — register [Cache.Collectors] to export the metrics.
type Cache struct {
	client    valkeygo.Client
	log       *slog.Logger
	metrics   *metrics
	opTimeout time.Duration
	flight    singleflight.Group
}

// New builds a client from cfg, verifies connectivity with a single PING (so a
// misconfigured cache fails fast at boot) and returns a ready [Cache]. The
// caller owns it and must call [Cache.Close].
func New(cfg Config, log *slog.Logger) (*Cache, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = defaultOpTimeout
	}
	opt, err := cfg.clientOption()
	if err != nil {
		return nil, err
	}
	// valkeyotel wraps the client so each command becomes a span; with the
	// no-op provider installed by default this costs one interface call.
	client, err := valkeyotel.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("valkey: connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()
	if err := client.Do(pingCtx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		return nil, fmt.Errorf("valkey: initial ping: %w", err)
	}

	// The resolved address (the URL's, when one is set), not the discrete field.
	log.Info("valkey cache ready", "addr", opt.InitAddress, "db", opt.SelectDB, "op_timeout", cfg.OpTimeout.String())
	return &Cache{client: client, log: log, metrics: newMetrics(), opTimeout: cfg.OpTimeout}, nil
}

// do runs one command under the per-command deadline and records its latency
// and (a nil reply — a missing key — is an answer, not an error) its failure.
func (c *Cache) do(ctx context.Context, op string, cmd valkeygo.Completed) valkeygo.ValkeyResult {
	ctx, cancel := context.WithTimeout(ctx, c.opTimeout)
	defer cancel()
	start := time.Now()
	res := c.client.Do(ctx, cmd)
	c.metrics.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if err := res.Error(); err != nil && !valkeygo.IsValkeyNil(err) {
		c.metrics.errors.WithLabelValues(op).Inc()
	}
	return res
}

// Client exposes the underlying valkey-go client for commands this wrapper does
// not surface. Commands sent through it are traced but not measured.
func (c *Cache) Client() valkeygo.Client { return c.client }

// Get returns the value at key. The boolean is false on a cache miss (a missing
// key — or a tombstone, see [Cache.Tombstone] — is not an error); a non-nil
// error means the lookup itself failed.
func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.do(ctx, "get", c.client.B().Get().Key(key).Build()).ToString()
	switch {
	case valkeygo.IsValkeyNil(err):
		c.metrics.lookups.WithLabelValues("miss").Inc()
		return "", false, nil
	case err != nil:
		c.metrics.lookups.WithLabelValues("error").Inc()
		return "", false, fmt.Errorf("valkey: get %q: %w", key, err)
	case v == tombstoneValue:
		c.metrics.lookups.WithLabelValues("miss").Inc()
		return "", false, nil
	}
	c.metrics.lookups.WithLabelValues("hit").Inc()
	return v, true, nil
}

// Set writes value at key unconditionally. A positive ttl sets an expiry; a
// zero ttl stores the key without one. Prefer [Cache.SetNX] (or [Aside]) to
// fill a cache from the source of truth: an unconditional write can overwrite
// a tombstone and resurrect stale data.
func (c *Cache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	var cmd valkeygo.Completed
	if ttl > 0 {
		cmd = c.client.B().Set().Key(key).Value(value).Px(ttl).Build()
	} else {
		cmd = c.client.B().Set().Key(key).Value(value).Build()
	}
	if err := c.do(ctx, "set", cmd).Error(); err != nil {
		return fmt.Errorf("valkey: set %q: %w", key, err)
	}
	return nil
}

// SetNX writes value at key only when the key does not exist — neither a value
// nor a tombstone — and reports whether it did. This is how a cache is filled
// from the source of truth: a concurrent invalidation always wins over a
// fill that loaded before it. A positive ttl is required.
func (c *Cache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("valkey: setnx %q: ttl must be positive", key)
	}
	err := c.do(ctx, "set", c.client.B().Set().Key(key).Value(value).Nx().Px(ttl).Build()).Error()
	switch {
	case valkeygo.IsValkeyNil(err):
		return false, nil // the key exists: not written
	case err != nil:
		return false, fmt.Errorf("valkey: setnx %q: %w", key, err)
	}
	return true, nil
}

// Del removes one or more keys, ignoring those that do not exist.
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.do(ctx, "del", c.client.B().Del().Key(keys...).Build()).Error(); err != nil {
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
