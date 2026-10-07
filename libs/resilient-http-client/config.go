package resilient

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the declarative configuration of a [Client].
//
// Zero means what each field's comment says — usually "disabled" — and is
// never silently replaced by a default. Start from [DefaultConfig] /
// [DefaultTarget] (or use [LoadConfig], which overlays the YAML on those
// defaults) and change what differs.
//
// YAML example (every key optional except a target's name):
//
//	max_idle_conns_per_host: 32
//	targets:
//	  - name: billing
//	    timeout: 2s
//	    rate_limit: 50.5
//	    rate_burst: 10
//	    max_concurrent: 64
//	    retry_max_attempts: 3
type Config struct {
	// MaxIdleConns caps idle keep-alive connections across all hosts
	// (default 100; 0 = no limit, as in net/http).
	MaxIdleConns int `yaml:"max_idle_conns"`
	// MaxIdleConnsPerHost caps idle keep-alive connections per host
	// (default 32; 0 = net/http's default of 2).
	MaxIdleConnsPerHost int `yaml:"max_idle_conns_per_host"`
	// MaxConnsPerHost caps dialing + active + idle connections per host
	// (default 0 = no limit).
	MaxConnsPerHost int `yaml:"max_conns_per_host"`
	// IdleConnTimeout closes a connection idle this long (default 90s;
	// 0 = never).
	IdleConnTimeout time.Duration `yaml:"idle_conn_timeout"`
	// ResponseHeaderTimeout bounds the wait for response headers after the
	// request is written (default 0 = none; the per-attempt timeout still
	// applies).
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
	// HTTP2PingInterval sends an HTTP/2 PING on a connection that has been
	// silent this long, so a dead connection is detected before a request is
	// stuck on it (default 30s; 0 = no health check).
	HTTP2PingInterval time.Duration `yaml:"http2_ping_interval"`
	// HTTP2PingTimeout closes the connection when a PING is not answered in
	// time (default 15s; 0 = net/http's default of 15s).
	HTTP2PingTimeout time.Duration `yaml:"http2_ping_timeout"`

	// MaxRedirects is how many same-host redirects are followed (default 10;
	// 0 = none: a 3xx response is returned to the caller as is).
	MaxRedirects int `yaml:"max_redirects"`
	// AllowCrossHostRedirects follows a redirect to another host (or an
	// https→http downgrade). Off by default: the per-target policy is keyed
	// on the host the caller chose, and a redirect must not escape it.
	AllowCrossHostRedirects bool `yaml:"allow_cross_host_redirects"`

	// UserAgent is sent when the request has no User-Agent of its own
	// (default "" = Go's default).
	UserAgent string `yaml:"user_agent"`

	// Targets declares one policy set per logical target. A request names its
	// target; an undeclared name is an error, never a lazily created policy.
	Targets []TargetConfig `yaml:"targets"`
}

// TargetConfig is the policy of one logical target (a dependency).
type TargetConfig struct {
	// Name identifies the target in [Client.Send] and in metric labels.
	// Required, unique.
	Name string `yaml:"name"`

	// Timeout bounds one attempt, including the wait for the rate limiter and
	// the bulkhead (default 5s; 0 = none — only the caller's context).
	Timeout time.Duration `yaml:"timeout"`

	// RateLimit is the sustained requests per second, fractions allowed
	// (default 0 = no rate limit).
	RateLimit float64 `yaml:"rate_limit"`
	// RateBurst is the token-bucket depth (0 = max(1, ceil(RateLimit))).
	RateBurst int `yaml:"rate_burst"`

	// MaxConcurrent caps in-flight requests to the target — a bulkhead
	// (default 0 = no cap).
	MaxConcurrent int `yaml:"max_concurrent"`
	// MaxConcurrentWait is how long a request waits for a bulkhead slot
	// (default 0 = do not wait: reject at once when full).
	MaxConcurrentWait time.Duration `yaml:"max_concurrent_wait"`

	// BreakerFailureRatio trips the circuit breaker when failures / requests
	// in the window reach it, in (0, 1] (default 0.5; 0 = no breaker).
	BreakerFailureRatio float64 `yaml:"breaker_failure_ratio"`
	// BreakerMinRequests is the least requests in the window before the ratio
	// is evaluated (default 20; must be ≥ 1 with a breaker).
	BreakerMinRequests int `yaml:"breaker_min_requests"`
	// BreakerWindow is the sliding window the ratio is measured over
	// (default 10s; must be > 0 with a breaker).
	BreakerWindow time.Duration `yaml:"breaker_window"`
	// BreakerOpenTimeout is how long the breaker stays open before admitting
	// probes (default 30s; must be > 0 with a breaker).
	BreakerOpenTimeout time.Duration `yaml:"breaker_open_timeout"`
	// BreakerHalfOpenProbes is how many probes half-open admits; all must
	// succeed to close it, any failure re-opens it (default 1; must be ≥ 1
	// with a breaker).
	BreakerHalfOpenProbes int `yaml:"breaker_half_open_probes"`

	// RetryMaxAttempts is the total attempts [Client.SendWithRetry] makes,
	// the first included (default 3; 0 or 1 = no retries).
	RetryMaxAttempts int `yaml:"retry_max_attempts"`
	// RetryBaseDelay is the full-jitter backoff base (default 100ms).
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	// RetryMaxDelay caps one backoff delay (default 5s; must be ≥
	// RetryBaseDelay). A Retry-After longer than this ends the retries.
	RetryMaxDelay time.Duration `yaml:"retry_max_delay"`
	// RetryBudgetRatio caps retries at this fraction of requests over
	// RetryBudgetWindow, in [0, 1] (default 0.2).
	RetryBudgetRatio float64 `yaml:"retry_budget_ratio"`
	// RetryBudgetMinRetries is the retries always allowed per window on top
	// of the ratio, so a quiet target can still retry (default 10).
	RetryBudgetMinRetries int `yaml:"retry_budget_min_retries"`
	// RetryBudgetWindow is the sliding window of the retry budget
	// (default 10s; 0 = no budget: retries are only bounded by attempts).
	RetryBudgetWindow time.Duration `yaml:"retry_budget_window"`
}

// DefaultConfig returns the defaults documented on [Config], with the given
// targets (each built with [DefaultTarget]).
func DefaultConfig(targets ...string) Config {
	c := Config{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
		HTTP2PingInterval:   30 * time.Second,
		HTTP2PingTimeout:    15 * time.Second,
		MaxRedirects:        10,
	}
	for _, name := range targets {
		c.Targets = append(c.Targets, DefaultTarget(name))
	}
	return c
}

// DefaultTarget returns the defaults documented on [TargetConfig].
func DefaultTarget(name string) TargetConfig {
	return TargetConfig{
		Name:                  name,
		Timeout:               5 * time.Second,
		BreakerFailureRatio:   0.5,
		BreakerMinRequests:    20,
		BreakerWindow:         10 * time.Second,
		BreakerOpenTimeout:    30 * time.Second,
		BreakerHalfOpenProbes: 1,
		RetryMaxAttempts:      3,
		RetryBaseDelay:        100 * time.Millisecond,
		RetryMaxDelay:         5 * time.Second,
		RetryBudgetRatio:      0.2,
		RetryBudgetMinRetries: 10,
		RetryBudgetWindow:     10 * time.Second,
	}
}

// LoadConfig parses YAML over [DefaultConfig] (and each target over
// [DefaultTarget]), rejects unknown keys and validates the result.
func LoadConfig(data []byte) (Config, error) {
	c := DefaultConfig()
	if err := strictDecode(data, &c); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("resilient: parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// UnmarshalYAML decodes a target over [DefaultTarget], so omitted keys keep
// their defaults. yaml.Node.Decode forgets KnownFields, hence the re-encode.
func (t *TargetConfig) UnmarshalYAML(n *yaml.Node) error {
	type plain TargetConfig
	raw, err := yaml.Marshal(n)
	if err != nil {
		return err
	}
	v := plain(DefaultTarget(""))
	if err := strictDecode(raw, &v); err != nil {
		return fmt.Errorf("target at line %d: %w", n.Line, err)
	}
	*t = TargetConfig(v)
	return nil
}

func strictDecode(data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	return dec.Decode(v)
}
