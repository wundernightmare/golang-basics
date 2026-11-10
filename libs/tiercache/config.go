package tiercache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Config tunes one cache. It carries `env` and `yaml` tags so a service can
// embed it in its own config (with an envPrefix per cache); zero values of
// the optional fields mean the defaults below.
//
//	TIERCACHE_NAME                 [name]                 metric label, L2 key prefix and channel   (required)
//	TIERCACHE_MAX_BYTES            [max_bytes]            L1 capacity in bytes                      (default 32 MiB)
//	TIERCACHE_SOFT_TTL             [soft_ttl]             age after which a value is refreshed      (default: HARD_TTL)
//	TIERCACHE_HARD_TTL             [hard_ttl]             age after which a value is never served   (required)
//	TIERCACHE_STALE_IF_ERROR       [stale_if_error]       grace past HARD_TTL when the source fails (default 0: off)
//	TIERCACHE_REFRESH_TIMEOUT      [refresh_timeout]      deadline of one background refresh        (default 5s)
//	TIERCACHE_REFRESH_CONCURRENCY  [refresh_concurrency]  background refreshes in flight            (default 16)
//	TIERCACHE_TOMBSTONE_TTL        [tombstone_ttl]        how long an invalidation blocks L2 fills  (default 5s)
//	TIERCACHE_BREAKER_THRESHOLD    [breaker_threshold]    consecutive L2 failures that open it      (default 5)
//	TIERCACHE_BREAKER_COOLDOWN     [breaker_cooldown]     how long L2 is skipped once open          (default 5s)
//	TIERCACHE_DISABLE_INVALIDATION [disable_invalidation] no Pub/Sub subscription                   (default false)
type Config struct {
	// Name identifies the cache: the `cache` metric label, the L2 key prefix
	// ("tc:<name>:") and the invalidation channel ("tc:<name>:inv"). Put the
	// value's format version in it ("credentials:v1") so that a change of
	// codec starts from an empty keyspace.
	Name string `env:"TIERCACHE_NAME" yaml:"name"`
	// MaxBytes is the L1 capacity. The off-heap store grows in steps of
	// 32 MiB and never below one step: 16 MiB and 33 MiB mean 32 and 64.
	MaxBytes int `env:"TIERCACHE_MAX_BYTES" yaml:"max_bytes"`
	// SoftTTL: past it a value is still served, and refreshed in the
	// background (stale-while-revalidate). Equal to HardTTL (the default)
	// there is no such window: plain cache-aside.
	SoftTTL time.Duration `env:"TIERCACHE_SOFT_TTL" yaml:"soft_ttl"`
	// HardTTL: past it a value is ignored on read and loaded synchronously.
	HardTTL time.Duration `env:"TIERCACHE_HARD_TTL" yaml:"hard_ttl"`
	// StaleIfError lets a value past HardTTL be served for this much longer,
	// and only when the source of truth failed. Zero never serves one.
	StaleIfError time.Duration `env:"TIERCACHE_STALE_IF_ERROR" yaml:"stale_if_error"`
	// RefreshTimeout bounds one background refresh (L2 and the loader).
	RefreshTimeout time.Duration `env:"TIERCACHE_REFRESH_TIMEOUT" yaml:"refresh_timeout"`
	// RefreshConcurrency bounds the background refreshes in flight; a stale
	// read past the limit is served without starting one.
	RefreshConcurrency int `env:"TIERCACHE_REFRESH_CONCURRENCY" yaml:"refresh_concurrency"`
	// TombstoneTTL is how long [Cache.Invalidate] keeps L2 fills of the key
	// refused: a few times the slowest load.
	TombstoneTTL time.Duration `env:"TIERCACHE_TOMBSTONE_TTL" yaml:"tombstone_ttl"`
	// BreakerThreshold consecutive L2 failures skip L2 for BreakerCooldown.
	BreakerThreshold int           `env:"TIERCACHE_BREAKER_THRESHOLD" yaml:"breaker_threshold"`
	BreakerCooldown  time.Duration `env:"TIERCACHE_BREAKER_COOLDOWN" yaml:"breaker_cooldown"`
	// DisableInvalidation turns the Pub/Sub subscription off: entries then
	// leave L1 by TTL only.
	DisableInvalidation bool `env:"TIERCACHE_DISABLE_INVALIDATION" yaml:"disable_invalidation"`
}

// l1Step is the granularity (and the minimum) of the off-heap store.
const l1Step = 32 << 20

func (c *Config) applyDefaults() {
	if c.MaxBytes <= 0 {
		c.MaxBytes = l1Step
	}

	if c.SoftTTL <= 0 {
		c.SoftTTL = c.HardTTL
	}

	if c.RefreshTimeout <= 0 {
		c.RefreshTimeout = 5 * time.Second
	}

	if c.RefreshConcurrency <= 0 {
		c.RefreshConcurrency = 16
	}

	if c.TombstoneTTL <= 0 {
		c.TombstoneTTL = 5 * time.Second
	}

	if c.BreakerThreshold <= 0 {
		c.BreakerThreshold = 5
	}

	if c.BreakerCooldown <= 0 {
		c.BreakerCooldown = 5 * time.Second
	}
}

// Validate reports the first setting a cache cannot run with.
func (c Config) Validate() error {
	switch {
	case c.Name == "":
		return errors.New("tiercache: Name is required")
	case c.HardTTL <= 0:
		return errors.New("tiercache: HardTTL must be positive")
	case c.SoftTTL > c.HardTTL:
		return fmt.Errorf("tiercache: SoftTTL (%s) must not exceed HardTTL (%s)", c.SoftTTL, c.HardTTL)
	case c.StaleIfError < 0:
		return errors.New("tiercache: StaleIfError must not be negative")
	}

	return nil
}

// Remote is the second level: what [Cache] needs from a shared cache server.
// *valkey.Cache (libs/valkey) implements it; every method is expected to
// carry its own deadline, as that one does (VALKEY_OP_TIMEOUT).
//
// Pass a nil Remote — the untyped nil, not a nil *valkey.Cache — to run on
// L1 and the loader alone.
type Remote interface {
	// Get returns the value at key; a missing key is not an error.
	Get(ctx context.Context, key string) (value string, found bool, err error)
	// SetNX stores value only when key holds nothing (not even a tombstone).
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (stored bool, err error)
	// Replace stores value only when key still holds exactly old.
	Replace(ctx context.Context, key, old, value string, ttl time.Duration) (stored bool, err error)
	// Tombstone makes key read as missing and refuse SetNX / Replace for ttl.
	Tombstone(ctx context.Context, key string, ttl time.Duration) error
	// Publish sends message to the current subscribers of channel.
	Publish(ctx context.Context, channel, message string) error
	// Subscribe blocks delivering channel's messages until ctx is done;
	// onSubscribed runs each time the subscription is (re-)established.
	Subscribe(ctx context.Context, channel string, onMessage func(message string), onSubscribed func()) error
}

// Codec turns a value into the payload both levels store, and back. Append
// writes into the caller's buffer, so encoding allocates nothing; Decode
// must not keep src (the buffer is reused) — a decoded string is a copy.
type Codec[T any] interface {
	Append(dst []byte, value T) ([]byte, error)
	Decode(src []byte) (T, error)
}

// Loader reads a value from the source of truth. It returns [ErrNotFound]
// (wrapped or not) when there is none; any other error counts as a failure of
// the source, which is what [Config.StaleIfError] is for.
type Loader[T any] func(ctx context.Context, key Key) (T, error)
