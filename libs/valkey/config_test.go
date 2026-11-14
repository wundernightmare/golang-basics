package valkey_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

func TestConnectionSettingsDefaultFromTheEnvironment(t *testing.T) {
	t.Parallel()

	cfg, err := valkey.LoadConfigFrom("TASKS_", map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, 4, cfg.PipelineConns)
	assert.Equal(t, 32, cfg.PoolSize)
	assert.Equal(t, 0, cfg.PoolMinIdle)
	assert.Equal(t, 5*time.Minute, cfg.PoolIdleTimeout)
	assert.Equal(t, 10*time.Second, cfg.ConnWriteTimeout)
	assert.Equal(t, time.Hour, cfg.ConnLifetime)

	cfg, err = valkey.LoadConfigFrom("TASKS_", map[string]string{
		"TASKS_VALKEY_PIPELINE_CONNS":     "8",
		"TASKS_VALKEY_POOL_SIZE":          "6",
		"TASKS_VALKEY_POOL_MIN_IDLE":      "2",
		"TASKS_VALKEY_POOL_IDLE_TIMEOUT":  "0",
		"TASKS_VALKEY_CONN_WRITE_TIMEOUT": "3s",
		"TASKS_VALKEY_CONN_LIFETIME":      "0",
	})
	require.NoError(t, err)
	assert.Equal(t, valkey.Config{
		DialTimeout: 5 * time.Second, OpTimeout: 500 * time.Millisecond,
		PipelineConns: 8, PoolSize: 6, PoolMinIdle: 2, ConnWriteTimeout: 3 * time.Second,
	}, cfg, "an explicit 0 turns the idle cleanup and the recycling off")
}

// What reaches the library. The count → power translation is the part that
// is easy to get wrong: the library reads 0 as "choose by GOMAXPROCS" and
// only a negative value as one connection.
func TestConfigTranslatesToClientOptions(t *testing.T) {
	t.Parallel()

	for conns, multiplex := range map[int]int{1: -1, 2: 1, 4: 2, 8: 3, 256: 8} {
		opt, err := valkey.Config{Addr: "cache:6379", PipelineConns: conns}.ClientOption()
		require.NoError(t, err, "pipeline_conns=%d", conns)
		assert.Equal(t, multiplex, opt.PipelineMultiplex, "pipeline_conns=%d", conns)
	}

	cfg := valkey.Config{
		Addr: "cache:6379", DB: 3, DialTimeout: 2 * time.Second,
		PoolSize: 6, PoolMinIdle: 2, PoolIdleTimeout: time.Minute,
		ConnWriteTimeout: 3 * time.Second, ConnLifetime: 20 * time.Minute,
	}
	opt, err := cfg.ClientOption()
	require.NoError(t, err)
	assert.Equal(t, []string{"cache:6379"}, opt.InitAddress)
	assert.Equal(t, 3, opt.SelectDB)
	assert.Equal(t, 2*time.Second, opt.Dialer.Timeout)
	assert.Equal(t, 6, opt.BlockingPoolSize)
	assert.Equal(t, 2, opt.BlockingPoolMinSize)
	assert.Equal(t, time.Minute, opt.BlockingPoolCleanup)
	assert.Equal(t, 3*time.Second, opt.ConnWriteTimeout)
	assert.Equal(t, 20*time.Minute, opt.ConnLifetime)

	// The URL form gets the same settings, and they win over the URL's own
	// parameters: one place decides the timeouts.
	cfg.Addr, cfg.URL = "", "valkey://:pw@other:6380/5?write_timeout=1m&dial_timeout=1m"
	opt, err = cfg.ClientOption()
	require.NoError(t, err)
	assert.Equal(t, []string{"other:6380"}, opt.InitAddress)
	assert.Equal(t, "pw", opt.Password)
	assert.Equal(t, 5, opt.SelectDB)
	assert.Equal(t, 2*time.Second, opt.Dialer.Timeout)
	assert.Equal(t, 3*time.Second, opt.ConnWriteTimeout)
	assert.Equal(t, 6, opt.BlockingPoolSize)
	assert.Equal(t, 20*time.Minute, opt.ConnLifetime)
}

// A hand-built Config with only an address is bounded all the same: the
// library's own defaults (1024 dedicated connections) never apply.
func TestHandBuiltConfigIsBounded(t *testing.T) {
	t.Parallel()

	opt, err := valkey.Config{Addr: "cache:6379"}.ClientOption()
	require.NoError(t, err)
	assert.Equal(t, 2, opt.PipelineMultiplex, "4 connections")
	assert.Equal(t, 32, opt.BlockingPoolSize)
	assert.Equal(t, 10*time.Second, opt.ConnWriteTimeout)
	assert.Equal(t, 5*time.Second, opt.Dialer.Timeout)
	assert.Zero(t, opt.ConnLifetime, "zero is 'never', as written")
	assert.Zero(t, opt.BlockingPoolCleanup)
}

func TestInvalidConnectionSettingsAreRejected(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		cfg   valkey.Config
		field string
	}{
		"not a power of two":     {valkey.Config{PipelineConns: 3}, "pipeline_conns"},
		"above the library max":  {valkey.Config{PipelineConns: 512}, "pipeline_conns"},
		"negative conns":         {valkey.Config{PipelineConns: -1}, "pipeline_conns"},
		"negative pool":          {valkey.Config{PoolSize: -1}, "pool_size"},
		"min idle above size":    {valkey.Config{PoolSize: 2, PoolMinIdle: 3}, "pool_min_idle"},
		"negative min idle":      {valkey.Config{PoolMinIdle: -1}, "pool_min_idle"},
		"negative idle timeout":  {valkey.Config{PoolIdleTimeout: -time.Second}, "pool_idle_timeout"},
		"negative write timeout": {valkey.Config{ConnWriteTimeout: -time.Second}, "conn_write_timeout"},
		"negative lifetime":      {valkey.Config{ConnLifetime: -time.Second}, "conn_lifetime"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// 192.0.2.1 is TEST-NET-1: were the config dialled rather than
			// rejected, this would take the dial timeout, not microseconds.
			tc.cfg.Addr = "192.0.2.1:6379"
			start := time.Now()
			_, err := valkey.New(tc.cfg, nil)
			require.ErrorIs(t, err, valkey.ErrConfig)
			require.ErrorContains(t, err, tc.field)
			assert.Less(t, time.Since(start), time.Second, "rejected before any dial")
		})
	}

	// The boundaries that must pass.
	for _, cfg := range []valkey.Config{
		{PipelineConns: 1, PoolSize: 1, PoolMinIdle: 1},
		{PipelineConns: 256},
		{PoolSize: 2, PoolMinIdle: 2},
	} {
		cfg.Addr = "cache:6379"
		_, err := cfg.ClientOption()
		require.NoError(t, err, "%+v", cfg)
	}
}
