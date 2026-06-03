package resilient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig_FillsDefaults(t *testing.T) {
	c := DefaultConfig()
	assert.Equal(t, defPoolMaxIdlePerHost, c.PoolMaxIdlePerHost)
	assert.Equal(t, defPoolIdleTimeout, c.PoolIdleTimeout)
	assert.Equal(t, defTCPKeepAlive, c.TCPKeepAlive)
	assert.Equal(t, defDefaultTimeout, c.DefaultTimeout)
	assert.Equal(t, defDNSMinTTL, c.DNSMinTTL)
	assert.Equal(t, defDNSMaxTTL, c.DNSMaxTTL)
}

func TestLoadConfig_AppliesTargetDefaults(t *testing.T) {
	yaml := `
default_timeout: 3s
outbound_targets:
  - name: "san_api"
    selector: "://san.example.com/v1"
    rate_limit: 500
`
	c, err := LoadConfig([]byte(yaml))
	require.NoError(t, err)
	require.Len(t, c.OutboundTargets, 1)

	tc := c.OutboundTargets[0]
	assert.Equal(t, "san_api", tc.Name)
	assert.Equal(t, 500, tc.RateLimit)
	// Unset target knobs take their defaults.
	assert.Equal(t, defCBThreshold, tc.CBThreshold)
	assert.Equal(t, uint32(defCBMinRequests), tc.CBMinRequests)
	assert.Equal(t, defCBWindow, tc.CBWindow)
	assert.Equal(t, defRetryBase, tc.RetryBase)
	// retry_max_attempts defaults to 0 (no retries) — must be preserved.
	assert.Equal(t, 0, tc.RetryMaxAttempts)
	assert.Equal(t, 3*time.Second, c.DefaultTimeout)
}

func TestLoadConfig_RejectsUnknownFields(t *testing.T) {
	_, err := LoadConfig([]byte("bogus_field: 1\n"))
	assert.Error(t, err)
}

func TestLoadConfig_DurationsParse(t *testing.T) {
	c, err := LoadConfig([]byte("pool_idle_timeout: 45s\ntcp_keepalive: 15s\n"))
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, c.PoolIdleTimeout)
	assert.Equal(t, 15*time.Second, c.TCPKeepAlive)
}

func TestResolveTimeout(t *testing.T) {
	tc := TargetConfig{}
	assert.Equal(t, 5*time.Second, tc.resolveTimeout(5*time.Second))
	tc.Timeout = 1200 * time.Millisecond
	assert.Equal(t, 1200*time.Millisecond, tc.resolveTimeout(5*time.Second))
}

func TestFallbackTarget_Defaulted(t *testing.T) {
	tc := fallbackTarget("unknown")
	assert.Equal(t, "unknown", tc.Name)
	assert.Equal(t, defRateLimit, tc.RateLimit)
	assert.Equal(t, defCBThreshold, tc.CBThreshold)
}
