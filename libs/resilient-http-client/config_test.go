package resilient

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/wundernightmare/golang-basics/libs/resilience"
)

func TestLoadConfig_OverlaysDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig([]byte(`
max_idle_conns_per_host: 8
http2_ping_interval: 0s
targets:
  - name: billing
    timeout: 2s
    rate_limit: 2.5
    breaker:
      failure_ratio: 0
  - name: search
`))
	require.NoError(t, err)

	want := DefaultConfig("billing", "search")
	want.MaxIdleConnsPerHost = 8
	want.HTTP2PingInterval = 0
	want.Targets[0].Timeout = 2 * time.Second
	want.Targets[0].RateLimit = 2.5
	want.Targets[0].Breaker.FailureRatio = 0
	assert.Equal(t, want, cfg, "omitted keys keep their defaults; an explicit 0 disables")
}

func TestLoadConfig_EmptyIsDefault(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, DefaultConfig(), cfg)
}

// The documented defaults, as literals: the field comments of Config and
// TargetConfig, and the README, promise these numbers.
func TestDefaults(t *testing.T) {
	t.Parallel()

	assert.Equal(t, Config{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
		HTTP2PingInterval:   30 * time.Second,
		HTTP2PingTimeout:    15 * time.Second,
		MaxRedirects:        10,
		Targets: []TargetConfig{{
			Name:    "a",
			Timeout: 5 * time.Second,
			Breaker: resilience.BreakerConfig{
				FailureRatio:   0.5,
				MinRequests:    20,
				Window:         10 * time.Second,
				OpenTimeout:    30 * time.Second,
				HalfOpenProbes: 1,
			},
			RetryMaxAttempts:      3,
			RetryBaseDelay:        100 * time.Millisecond,
			RetryMaxDelay:         5 * time.Second,
			RetryBudgetRatio:      0.2,
			RetryBudgetMinRetries: 10,
			RetryBudgetWindow:     10 * time.Second,
		}},
	}, DefaultConfig("a"))
}

// Every transport key of Config reaches the transport New builds.
func TestNewTransport_AppliesTheConfig(t *testing.T) {
	t.Parallel()

	transport := newTransport(&Config{
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   2,
		MaxConnsPerHost:       3,
		IdleConnTimeout:       4 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		HTTP2PingInterval:     6 * time.Second,
		HTTP2PingTimeout:      7 * time.Second,
	})

	for _, tc := range []struct {
		key       string
		got, want any
	}{
		{"max_idle_conns", transport.MaxIdleConns, 1},
		{"max_idle_conns_per_host", transport.MaxIdleConnsPerHost, 2},
		{"max_conns_per_host", transport.MaxConnsPerHost, 3},
		{"idle_conn_timeout", transport.IdleConnTimeout, 4 * time.Second},
		{"response_header_timeout", transport.ResponseHeaderTimeout, 5 * time.Second},
		{"http2_ping_interval", transport.HTTP2.SendPingTimeout, 6 * time.Second},
		{"http2_ping_timeout", transport.HTTP2.PingTimeout, 7 * time.Second},
		{"HTTP/2 is attempted", transport.ForceAttemptHTTP2, true},
		{"the proxy comes from the environment", transport.Proxy != nil, true},
	} {
		assert.Equal(t, tc.want, tc.got, tc.key)
	}

	assert.Nil(t, newTransport(&Config{}).HTTP2, "http2_ping_interval 0: no health check")
}

func TestLoadConfig_RejectsUnknownKeysAndInvalid(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
		{"rate NaN", func(c *Config) { c.Targets[0].RateLimit = math.NaN() }, "rate_limit"},
		{"rate Inf", func(c *Config) { c.Targets[0].RateLimit = math.Inf(1) }, "rate_limit"},
		{"burst", func(c *Config) { c.Targets[0].RateBurst = -1 }, "rate_limit"},
		{"bulkhead", func(c *Config) { c.Targets[0].MaxConcurrent = -1 }, "max_concurrent"},
		{"ratio low", func(c *Config) { c.Targets[0].Breaker.FailureRatio = -0.1 }, "breaker: failure_ratio"},
		{"ratio high", func(c *Config) { c.Targets[0].Breaker.FailureRatio = 1.01 }, "breaker: failure_ratio"},
		{"ratio NaN", func(c *Config) { c.Targets[0].Breaker.FailureRatio = math.NaN() }, "breaker: failure_ratio"},
		{"min requests", func(c *Config) { c.Targets[0].Breaker.MinRequests = 0 }, "breaker: min_requests"},
		{"window", func(c *Config) { c.Targets[0].Breaker.Window = 0 }, "breaker: window"},
		{"open timeout", func(c *Config) { c.Targets[0].Breaker.OpenTimeout = 0 }, "breaker: window"},
		{"probes", func(c *Config) { c.Targets[0].Breaker.HalfOpenProbes = 0 }, "probes"},
		{"attempts", func(c *Config) { c.Targets[0].RetryMaxAttempts = -1 }, "retry_max_attempts"},
		{"base delay", func(c *Config) { c.Targets[0].RetryBaseDelay = -1 }, "retry delays"},
		{"max < base", func(c *Config) { c.Targets[0].RetryMaxDelay = c.Targets[0].RetryBaseDelay - 1 }, "retry delays"},
		{"budget ratio low", func(c *Config) { c.Targets[0].RetryBudgetRatio = -0.1 }, "retry_budget_ratio"},
		{"budget ratio high", func(c *Config) { c.Targets[0].RetryBudgetRatio = 1.1 }, "retry_budget_ratio"},
		{"budget ratio NaN", func(c *Config) { c.Targets[0].RetryBudgetRatio = math.NaN() }, "retry_budget_ratio"},
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

	// Zero means disabled, never invalid: a target may set nothing but its name.
	zero := Config{Targets: []TargetConfig{{Name: "a"}}}
	require.NoError(t, zero.Validate())

	// Bounds are inclusive where the doc says so.
	defaults := DefaultConfig("a")
	defaults.Targets[0].Breaker.FailureRatio = 1
	defaults.Targets[0].RetryBudgetRatio = 1
	defaults.Targets[0].RetryMaxDelay = defaults.Targets[0].RetryBaseDelay
	defaults.Targets[0].Breaker.MinRequests = 1
	defaults.Targets[0].Breaker.HalfOpenProbes = 1
	require.NoError(t, defaults.Validate())

	// Breaker knobs are free when the breaker is off.
	defaults = DefaultConfig("a")
	defaults.Targets[0].Breaker.FailureRatio = 0
	defaults.Targets[0].Breaker.MinRequests, defaults.Targets[0].Breaker.Window, defaults.Targets[0].Breaker.HalfOpenProbes = 0, 0, 0
	require.NoError(t, defaults.Validate())

	// Every problem is reported, not just the first.
	defaults = DefaultConfig("a")
	defaults.MaxRedirects = -1
	defaults.Targets[0].RateLimit = -1
	defaults.Targets[0].Breaker.MinRequests, defaults.Targets[0].Breaker.HalfOpenProbes = 0, 0
	err := defaults.Validate()
	require.Error(t, err)
	assert.Equal(t, 4, strings.Count(err.Error(), "\n")+1)
	assert.Equal(t, 2, strings.Count(err.Error(), `target "a": breaker: `), "each on a line that names its target")
}

// viaNode decodes itself the short way: from the node it is handed.
type viaNode struct {
	Known string `yaml:"known"`
}

func (v *viaNode) UnmarshalYAML(n *yaml.Node) error {
	type plain viaNode

	return n.Decode((*plain)(v))
}

// The defect TargetConfig.UnmarshalYAML works around, asserted: a strict
// decoder refuses an unknown key of a struct it decodes itself, and takes
// the same key in silence once the struct decodes its own node. This test
// fails when an upgrade of the library fixes that — the re-encode in both
// outbound clients, the markers and the row in docs/UPSTREAM-WORKAROUNDS.md
// go then.
func TestUpstreamYAMLNodeDecodeIgnoresKnownFields(t *testing.T) {
	t.Parallel()

	const doc = "inner:\n  known: a\n  misspelt: b\n"

	var direct struct {
		Inner struct {
			Known string `yaml:"known"`
		} `yaml:"inner"`
	}

	require.ErrorContains(t, strictDecode([]byte(doc), &direct), "misspelt", "the control: KnownFields works on a plain struct")

	var through struct {
		Inner viaNode `yaml:"inner"`
	}

	require.NoError(t, strictDecode([]byte(doc), &through), "fixed upstream? then remove the workaround")
	assert.Equal(t, "a", through.Inner.Known)
}
