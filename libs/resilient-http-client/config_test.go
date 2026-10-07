package resilient

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_OverlaysDefaults(t *testing.T) {
	cfg, err := LoadConfig([]byte(`
max_idle_conns_per_host: 8
http2_ping_interval: 0s
targets:
  - name: billing
    timeout: 2s
    rate_limit: 2.5
    breaker_failure_ratio: 0
  - name: search
`))
	require.NoError(t, err)

	want := DefaultConfig("billing", "search")
	want.MaxIdleConnsPerHost = 8
	want.HTTP2PingInterval = 0
	want.Targets[0].Timeout = 2 * time.Second
	want.Targets[0].RateLimit = 2.5
	want.Targets[0].BreakerFailureRatio = 0
	assert.Equal(t, want, cfg, "omitted keys keep their defaults; an explicit 0 disables")
}

func TestLoadConfig_EmptyIsDefault(t *testing.T) {
	cfg, err := LoadConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, DefaultConfig(), cfg)
}

func TestLoadConfig_RejectsUnknownKeysAndInvalid(t *testing.T) {
	for name, doc := range map[string]string{
		"top-level typo": "max_idle_conn: 3",
		"target typo":    "targets:\n  - name: a\n    time_out: 1s",
		"bad duration":   "targets:\n  - name: a\n    timeout: soon",
		"not yaml":       "targets: [",
		"invalid":        "targets:\n  - name: a\n  - name: a",
	} {
		_, err := LoadConfig([]byte(doc))
		assert.Error(t, err, name)
	}
}

func TestValidate(t *testing.T) {
	require.NoError(t, (&Config{}).Validate(), "the zero config is valid: everything disabled")
	def := DefaultConfig("a", "b")
	require.NoError(t, def.Validate())

	for _, tc := range []struct {
		name string
		mut  func(*Config)
		msg  string
	}{
		{"empty name", func(c *Config) { c.Targets[0].Name = "" }, "name is empty"},
		{"duplicate", func(c *Config) { c.Targets[1].Name = "a" }, "declared twice"},
		{"pool", func(c *Config) { c.MaxConnsPerHost = -1 }, "pool sizes"},
		{"pool idle", func(c *Config) { c.MaxIdleConns = -1 }, "pool sizes"},
		{"pool per host", func(c *Config) { c.MaxIdleConnsPerHost = -1 }, "pool sizes"},
		{"idle timeout", func(c *Config) { c.IdleConnTimeout = -1 }, "transport timeouts"},
		{"header timeout", func(c *Config) { c.ResponseHeaderTimeout = -1 }, "transport timeouts"},
		{"ping", func(c *Config) { c.HTTP2PingInterval = -1 }, "transport timeouts"},
		{"ping timeout", func(c *Config) { c.HTTP2PingTimeout = -1 }, "transport timeouts"},
		{"redirects", func(c *Config) { c.MaxRedirects = -1 }, "max_redirects"},
		{"timeout", func(c *Config) { c.Targets[0].Timeout = -1 }, "timeouts must not"},
		{"bulkhead wait", func(c *Config) { c.Targets[0].MaxConcurrentWait = -1 }, "timeouts must not"},
		{"rate", func(c *Config) { c.Targets[0].RateLimit = -0.5 }, "rate_limit"},
		{"burst", func(c *Config) { c.Targets[0].RateBurst = -1 }, "rate_limit"},
		{"bulkhead", func(c *Config) { c.Targets[0].MaxConcurrent = -1 }, "max_concurrent"},
		{"ratio low", func(c *Config) { c.Targets[0].BreakerFailureRatio = -0.1 }, "breaker_failure_ratio"},
		{"ratio high", func(c *Config) { c.Targets[0].BreakerFailureRatio = 1.01 }, "breaker_failure_ratio"},
		{"min requests", func(c *Config) { c.Targets[0].BreakerMinRequests = 0 }, "breaker_min_requests"},
		{"window", func(c *Config) { c.Targets[0].BreakerWindow = 0 }, "breaker_window"},
		{"open timeout", func(c *Config) { c.Targets[0].BreakerOpenTimeout = 0 }, "breaker_window"},
		{"probes", func(c *Config) { c.Targets[0].BreakerHalfOpenProbes = 0 }, "probes"},
		{"attempts", func(c *Config) { c.Targets[0].RetryMaxAttempts = -1 }, "retry_max_attempts"},
		{"base delay", func(c *Config) { c.Targets[0].RetryBaseDelay = -1 }, "retry delays"},
		{"max < base", func(c *Config) { c.Targets[0].RetryMaxDelay = c.Targets[0].RetryBaseDelay - 1 }, "retry delays"},
		{"budget ratio low", func(c *Config) { c.Targets[0].RetryBudgetRatio = -0.1 }, "retry_budget_ratio"},
		{"budget ratio high", func(c *Config) { c.Targets[0].RetryBudgetRatio = 1.1 }, "retry_budget_ratio"},
		{"budget min", func(c *Config) { c.Targets[0].RetryBudgetMinRetries = -1 }, "retry budget"},
		{"budget window", func(c *Config) { c.Targets[0].RetryBudgetWindow = -1 }, "retry budget"},
	} {
		c := DefaultConfig("a", "b")
		tc.mut(&c)
		err := c.Validate()
		if assert.Error(t, err, tc.name) {
			assert.Contains(t, err.Error(), tc.msg, tc.name)
		}
	}

	// Bounds are inclusive where the doc says so.
	c := DefaultConfig("a")
	c.Targets[0].BreakerFailureRatio = 1
	c.Targets[0].RetryBudgetRatio = 1
	c.Targets[0].RetryMaxDelay = c.Targets[0].RetryBaseDelay
	c.Targets[0].BreakerMinRequests = 1
	c.Targets[0].BreakerHalfOpenProbes = 1
	require.NoError(t, c.Validate())

	// Breaker knobs are free when the breaker is off.
	c = DefaultConfig("a")
	c.Targets[0].BreakerFailureRatio = 0
	c.Targets[0].BreakerMinRequests, c.Targets[0].BreakerWindow, c.Targets[0].BreakerHalfOpenProbes = 0, 0, 0
	require.NoError(t, c.Validate())

	// Every problem is reported, not just the first.
	c = DefaultConfig("a")
	c.MaxRedirects = -1
	c.Targets[0].RateLimit = -1
	err := c.Validate()
	require.Error(t, err)
	assert.Equal(t, 2, strings.Count(err.Error(), "\n")+1)
}
