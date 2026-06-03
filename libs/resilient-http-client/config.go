package resilient

import (
	"bytes"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the complete declarative configuration for a [Client].
//
// It is normally loaded from YAML via [LoadConfig], but can equally be built in
// code. Every field has a sensible default applied by [Config.withDefaults] (run
// automatically by [LoadConfig] and [New]), so callers only set what differs.
//
// YAML example:
//
//	pool_max_idle_per_host: 100
//	pool_idle_timeout: 90s
//	tcp_keepalive: 30s
//	default_timeout: 5s
//	user_agent: "my-service/1.0"
//	dns_cache_enabled: true
//	dns_min_ttl: 10s
//	dns_max_ttl: 5m
//	outbound_targets:
//	  - name: "meta_events"
//	    selector: "://graph.facebook.com/{pixel_id}/events"
//	    rate_limit: 5000
//	    timeout: 2s
//	    cb_threshold: 0.5
//	    cb_min_requests: 10
//	    retry_max_attempts: 3
//	    adaptive_concurrency_enabled: true
type Config struct {
	// PoolMaxIdlePerHost caps idle keep-alive connections kept per host.
	PoolMaxIdlePerHost int `yaml:"pool_max_idle_per_host"`
	// PoolIdleTimeout closes an idle connection after this long (0 = no limit).
	PoolIdleTimeout time.Duration `yaml:"pool_idle_timeout"`
	// TCPKeepAlive is the TCP keep-alive probe interval (0 disables keep-alive).
	TCPKeepAlive time.Duration `yaml:"tcp_keepalive"`
	// DefaultTimeout is the per-request timeout applied when neither the request
	// nor the target overrides it.
	DefaultTimeout time.Duration `yaml:"default_timeout"`

	// UserAgent, when set, is sent as the User-Agent header on every request
	// (a per-request override still wins). Empty leaves Go's default.
	UserAgent string `yaml:"user_agent"`

	// DNSCacheEnabled turns on the TTL-aware in-process DNS cache, eliminating
	// repeated OS resolver lookups for stable endpoints.
	DNSCacheEnabled bool `yaml:"dns_cache_enabled"`
	// DNSMinTTL / DNSMaxTTL clamp how long a resolved host is cached.
	DNSMinTTL time.Duration `yaml:"dns_min_ttl"`
	DNSMaxTTL time.Duration `yaml:"dns_max_ttl"`

	// OutboundTargets declares one policy set per logical target. The Name field
	// is the key used in [Request.Target].
	OutboundTargets []TargetConfig `yaml:"outbound_targets"`
}

// TargetConfig holds the per-target policy knobs. A request whose [Request.Target]
// does not match any Name here transparently gets a fallback policy built from
// the zero value + defaults (see [Config.withDefaults]).
type TargetConfig struct {
	// Name is the logical identifier, e.g. "meta_events". It is both the lookup
	// key and a metric label.
	Name string `yaml:"name"`
	// Selector is a human-readable URL template used only as the template_url
	// metric label — never for routing.
	Selector string `yaml:"selector"`

	// RateLimit is the sustained requests/second allowed for this target.
	RateLimit int `yaml:"rate_limit"`
	// Timeout overrides Config.DefaultTimeout for this target (0 = inherit).
	Timeout time.Duration `yaml:"timeout"`

	// --- Circuit breaker ---

	// CBThreshold is the failure ratio in the window that trips the breaker
	// (0.0..1.0).
	CBThreshold float64 `yaml:"cb_threshold"`
	// CBMinRequests is the minimum number of requests in the window before the
	// ratio is evaluated (guards against tripping on cold traffic).
	CBMinRequests uint32 `yaml:"cb_min_requests"`
	// CBWindow is the sliding measurement window size.
	CBWindow time.Duration `yaml:"cb_window"`
	// CBHalfOpenTimeout is how long the breaker stays open before admitting one
	// probe.
	CBHalfOpenTimeout time.Duration `yaml:"cb_half_open_timeout"`

	// --- Retry (jittered exponential backoff) ---

	// RetryMaxAttempts is the maximum total attempts for [Client.SendWithRetry].
	// 0 means no automatic retries. Only transient errors are retried.
	RetryMaxAttempts int `yaml:"retry_max_attempts"`
	// RetryBase is the base delay for full-jitter backoff.
	RetryBase time.Duration `yaml:"retry_base"`
	// RetryCap is the backoff ceiling.
	RetryCap time.Duration `yaml:"retry_cap"`

	// --- Adaptive concurrency (AIMD) ---

	// AdaptiveConcurrencyEnabled turns on the AIMD in-flight limiter.
	AdaptiveConcurrencyEnabled bool `yaml:"adaptive_concurrency_enabled"`
	// AdaptiveConcurrencyInitial / Min / Max bound the AIMD limit.
	AdaptiveConcurrencyInitial int `yaml:"adaptive_concurrency_initial"`
	AdaptiveConcurrencyMin     int `yaml:"adaptive_concurrency_min"`
	AdaptiveConcurrencyMax     int `yaml:"adaptive_concurrency_max"`
}

// Defaults, mirroring the Rust crate's serde defaults.
const (
	defPoolMaxIdlePerHost = 100
	defPoolIdleTimeout    = 90 * time.Second
	defTCPKeepAlive       = 30 * time.Second
	defDefaultTimeout     = 5 * time.Second
	defDNSMinTTL          = 10 * time.Second
	defDNSMaxTTL          = 300 * time.Second

	defRateLimit         = 1000
	defCBThreshold       = 0.5
	defCBMinRequests     = 10
	defCBWindow          = 10 * time.Second
	defCBHalfOpenTimeout = 30 * time.Second
	defRetryBase         = 100 * time.Millisecond
	defRetryCap          = 30 * time.Second
	defAdaptiveInitial   = 100
	defAdaptiveMin       = 1
	defAdaptiveMax       = 1000
)

// DefaultConfig returns a fully-defaulted [Config] with no targets declared.
func DefaultConfig() Config {
	var c Config
	c.withDefaults()
	return c
}

// LoadConfig parses a YAML document into a fully-defaulted [Config].
//
// Unknown fields are rejected so typos surface immediately rather than being
// silently ignored.
func LoadConfig(data []byte) (Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("resilient: parse config: %w", err)
	}
	c.withDefaults()
	return c, nil
}

// withDefaults fills every zero-valued field with its default, in place.
//
// Zero is treated as "unset" for all knobs except RetryMaxAttempts, where 0
// legitimately means "no retries" and is preserved.
func (c *Config) withDefaults() {
	if c.PoolMaxIdlePerHost == 0 {
		c.PoolMaxIdlePerHost = defPoolMaxIdlePerHost
	}
	if c.PoolIdleTimeout == 0 {
		c.PoolIdleTimeout = defPoolIdleTimeout
	}
	if c.TCPKeepAlive == 0 {
		c.TCPKeepAlive = defTCPKeepAlive
	}
	if c.DefaultTimeout == 0 {
		c.DefaultTimeout = defDefaultTimeout
	}
	if c.DNSMinTTL == 0 {
		c.DNSMinTTL = defDNSMinTTL
	}
	if c.DNSMaxTTL == 0 {
		c.DNSMaxTTL = defDNSMaxTTL
	}
	for i := range c.OutboundTargets {
		c.OutboundTargets[i].withDefaults()
	}
}

func (t *TargetConfig) withDefaults() {
	if t.RateLimit == 0 {
		t.RateLimit = defRateLimit
	}
	if t.CBThreshold == 0 {
		t.CBThreshold = defCBThreshold
	}
	if t.CBMinRequests == 0 {
		t.CBMinRequests = defCBMinRequests
	}
	if t.CBWindow == 0 {
		t.CBWindow = defCBWindow
	}
	if t.CBHalfOpenTimeout == 0 {
		t.CBHalfOpenTimeout = defCBHalfOpenTimeout
	}
	if t.RetryBase == 0 {
		t.RetryBase = defRetryBase
	}
	if t.RetryCap == 0 {
		t.RetryCap = defRetryCap
	}
	if t.AdaptiveConcurrencyInitial == 0 {
		t.AdaptiveConcurrencyInitial = defAdaptiveInitial
	}
	if t.AdaptiveConcurrencyMin == 0 {
		t.AdaptiveConcurrencyMin = defAdaptiveMin
	}
	if t.AdaptiveConcurrencyMax == 0 {
		t.AdaptiveConcurrencyMax = defAdaptiveMax
	}
}

// fallbackTarget builds a defaulted policy config for an undeclared target.
func fallbackTarget(name string) TargetConfig {
	t := TargetConfig{Name: name}
	t.withDefaults()
	return t
}

// resolveTimeout returns the effective per-request timeout for this target,
// falling back to def when the target does not override it.
func (t *TargetConfig) resolveTimeout(def time.Duration) time.Duration {
	if t.Timeout > 0 {
		return t.Timeout
	}
	return def
}
