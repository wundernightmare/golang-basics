package valkey_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

// The process-environment path of LoadConfig (LoadConfigFrom with nil);
// everything else reads a map through LoadConfigFrom. Serial: t.Setenv
// cannot run in a parallel test.
func TestLoadConfig_ReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv("TASKS_VALKEY_ADDR", "cache.from-env:6380")

	cfg, err := valkey.LoadConfig("TASKS_")
	require.NoError(t, err)
	assert.Equal(t, "cache.from-env:6380", cfg.Addr, "LoadConfig reads the process environment")
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := valkey.LoadConfigFrom("TASKS_", map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, cfg.Addr, "no default address: unset means no Valkey")
	assert.False(t, cfg.Configured())
	assert.Equal(t, 0, cfg.DB)
	assert.Equal(t, 500*time.Millisecond, cfg.OpTimeout)
	assert.Equal(t, valkey.CompareAndSetIFEQ, cfg.CompareAndSet)

	cfg, err = valkey.LoadConfigFrom("TASKS_", map[string]string{
		"TASKS_VALKEY_ADDR":            "cache:6380",
		"TASKS_VALKEY_DB":              "3",
		"TASKS_VALKEY_COMPARE_AND_SET": "lua",
	})
	require.NoError(t, err)
	assert.Equal(t, "cache:6380", cfg.Addr)
	assert.Equal(t, 3, cfg.DB)
	assert.Equal(t, valkey.CompareAndSetLua, cfg.CompareAndSet)
	assert.True(t, cfg.Configured())
}

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
	assert.Equal(t, valkey.CompareAndSetIFEQ, cfg.CompareAndSet)

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
		DialTimeout: 5 * time.Second, OpTimeout: 500 * time.Millisecond, CompareAndSet: valkey.CompareAndSetIFEQ,
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
	assert.True(t, opt.DisableRetry, "the library's retry races with its own connection writer")

	// The URL form gets the same settings, and they win over the URL's own
	// parameters: one place decides the timeouts.
	cfg.Addr, cfg.URL = "", secret.New("valkey://:pw@other:6380/5?write_timeout=1m&dial_timeout=1m")
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

func TestClusterAndSentinelURLsTranslateToClientOptions(t *testing.T) {
	t.Parallel()

	opt, err := valkey.Config{URL: secret.New("valkey://one:6379?addr=two:6379&addr=three:6379")}.ClientOption()
	require.NoError(t, err)
	assert.Equal(t, []string{"one:6379", "two:6379", "three:6379"}, opt.InitAddress)
	assert.Empty(t, opt.Sentinel.MasterSet)

	opt, err = valkey.Config{URL: secret.New("valkeys://one:26379?addr=two:26379&master_set=primary")}.ClientOption()
	require.NoError(t, err)
	assert.Equal(t, []string{"one:26379", "two:26379"}, opt.InitAddress)
	assert.Equal(t, "primary", opt.Sentinel.MasterSet)
	assert.Equal(t, 5*time.Second, opt.Sentinel.TopologyRefreshInterval)
	assert.Equal(t, 5*time.Second, opt.Sentinel.Dialer.Timeout)
	assert.Empty(t, opt.TLSConfig.ServerName, "the primary's name is not the sentinel seed's")
	assert.NotNil(t, opt.Sentinel.TLSConfig)
	assert.NotSame(t, opt.TLSConfig, opt.Sentinel.TLSConfig)
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
		"not a power of two":         {valkey.Config{PipelineConns: 3}, "pipeline_conns"},
		"above the library max":      {valkey.Config{PipelineConns: 512}, "pipeline_conns"},
		"negative conns":             {valkey.Config{PipelineConns: -1}, "pipeline_conns"},
		"negative pool":              {valkey.Config{PoolSize: -1}, "pool_size"},
		"min idle above size":        {valkey.Config{PoolSize: 2, PoolMinIdle: 3}, "pool_min_idle"},
		"negative min idle":          {valkey.Config{PoolMinIdle: -1}, "pool_min_idle"},
		"negative idle timeout":      {valkey.Config{PoolIdleTimeout: -time.Second}, "pool_idle_timeout"},
		"negative write timeout":     {valkey.Config{ConnWriteTimeout: -time.Second}, "conn_write_timeout"},
		"negative lifetime":          {valkey.Config{ConnLifetime: -time.Second}, "conn_lifetime"},
		"negative op timeout":        {valkey.Config{OpTimeout: -time.Second}, "op_timeout"},
		"unknown compare-and-set":    {valkey.Config{CompareAndSet: "watch"}, "compare_and_set"},
		"negative dial timeout":      {valkey.Config{DialTimeout: -time.Second}, "dial_timeout"},
		"negative db":                {valkey.Config{DB: -5}, "db"},
		"ca file without tls":        {valkey.Config{CAFile: "ca.pem"}, "ca_file"},
		"ca file that is absent":     {valkey.Config{TLS: true, CAFile: "no-such-ca.pem"}, "ca_file"},
		"no address":                 {valkey.Config{Addr: "-"}, "VALKEY_URL or VALKEY_ADDR"},
		"url of another scheme":      {valkey.Config{URL: secret.New("http://cache:6379")}, "url"},
		"url with a bad db":          {valkey.Config{URL: secret.New("valkey://cache:6379/zz")}, "url"},
		"url of a cluster with a db": {valkey.Config{URL: secret.New("valkey://cache:6379/1?addr=other:6379")}, "only db 0"},
		"cert without its key":       {valkey.Config{TLS: true, CertFile: "client.pem"}, "cert_file and key_file"},
		"key without its cert":       {valkey.Config{TLS: true, KeyFile: "client.key"}, "cert_file and key_file"},
		"cert pair without tls":      {valkey.Config{CertFile: "client.pem", KeyFile: "client.key"}, "TLS is off"},
		"cert pair that is absent":   {valkey.Config{TLS: true, CertFile: "no-such.pem", KeyFile: "no-such.key"}, "cert_file"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// 192.0.2.1 is TEST-NET-1: were the config dialled rather than
			// rejected, this would take the dial timeout, not microseconds.
			if tc.cfg.Addr == "" {
				tc.cfg.Addr = "192.0.2.1:6379"
			} else {
				tc.cfg.Addr = ""
			}

			require.ErrorIs(t, tc.cfg.Validate(), valkey.ErrConfig)

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

// The loader validates what it read, when there is an address to validate
// for; its errors are config errors like the constructor's.
func TestLoadConfigRejectsWhatNewWould(t *testing.T) {
	t.Parallel()

	for name, environ := range map[string]map[string]string{
		"a value New rejects":  {"TASKS_VALKEY_ADDR": "cache:6379", "TASKS_VALKEY_PIPELINE_CONNS": "3"},
		"a value env rejects":  {"TASKS_VALKEY_ADDR": "cache:6379", "TASKS_VALKEY_OP_TIMEOUT": "soon"},
		"a url that is no url": {"TASKS_VALKEY_URL": "cache:6379"},
	} {
		_, err := valkey.LoadConfigFrom("TASKS_", environ)
		require.ErrorIs(t, err, valkey.ErrConfig, name)
	}

	cfg, err := valkey.LoadConfigFrom("TASKS_", map[string]string{"TASKS_VALKEY_PIPELINE_CONNS": "3"})
	require.NoError(t, err, "without an address there is no cache to validate for")
	assert.False(t, cfg.Configured())
}

// A URL that does not parse is, as a rule, a password with an unescaped
// character: nothing of the URL may reach the error.
func TestUnparsableURLDoesNotLeakThePassword(t *testing.T) {
	t.Parallel()

	for _, url := range []string{
		"valkey://:Sup3r%S3cret@cache:6379/0",  // a bare percent sign
		"valkey://:Sup3rS3cret\n@cache:6379/0", // a newline, as read from a file
		"valkey://:Sup3r#S3cret@cache:6379/0",  // the rest becomes a fragment
		"valkey://:Sup3rS3cret@cache:port/0",
		"valkey://:12/Sup3rS3cret@cache:6379", // the rest becomes the path
	} {
		_, err := valkey.New(valkey.Config{URL: secret.New(url)}, nil)
		require.ErrorIs(t, err, valkey.ErrConfig, url)
		assert.NotContains(t, err.Error(), "Sup3r", url)
		assert.NotContains(t, err.Error(), "S3cret", url)
	}
}

// TLS, the CA file and the user reach the library from the discrete keys,
// and TLS and the CA file from next to a URL as well.
func TestTLSAndUserTranslateToClientOptions(t *testing.T) {
	t.Parallel()

	cfg, err := valkey.LoadConfigFrom("TASKS_", map[string]string{
		"TASKS_VALKEY_ADDR": "cache:6379", "TASKS_VALKEY_USERNAME": "tasks", "TASKS_VALKEY_TLS": "true",
	})
	require.NoError(t, err)

	opt, err := cfg.ClientOption()
	require.NoError(t, err)
	assert.Equal(t, "tasks", opt.Username)
	require.NotNil(t, opt.TLSConfig)
	assert.Nil(t, opt.TLSConfig.RootCAs, "no CA file: the system roots")

	opt, err = valkey.Config{Addr: "cache:6379"}.ClientOption()
	require.NoError(t, err)
	assert.Nil(t, opt.TLSConfig, "TLS is off unless asked for")

	opt, err = valkey.Config{URL: secret.New("valkey://cache:6379"), TLS: true}.ClientOption()
	require.NoError(t, err)
	assert.NotNil(t, opt.TLSConfig, "the flag turns TLS on for a plain-scheme URL")
}

// A pipeline long enough for a dedicated connection waits for a slot like a
// blocking command; one command shorter, it is pipelined and never waits.
func TestLongPipelineWaitsForADedicatedConnection(t *testing.T) {
	t.Parallel()

	inner := &countingClient{}
	client := valkey.NewBounded(inner, 1)

	_, giveBack := client.Dedicate() // the only slot
	defer giveBack()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	long := make([]valkeygo.Completed, valkeygo.DefaultBlockingPipeline)
	for _, res := range client.DoMulti(ctx, long...) {
		require.ErrorIs(t, res.Error(), context.DeadlineExceeded)
	}

	assert.Zero(t, inner.multis, "the library never saw the pipeline that had no slot")

	client.DoMulti(t.Context(), long[1:]...)
	assert.Equal(t, 1, inner.multis)
}

// countingClient is the library's client as far as bounded calls it here.
type countingClient struct {
	valkeygo.Client

	multis int
}

func (c *countingClient) Dedicate() (valkeygo.DedicatedClient, func()) { return nil, func() {} }

func (c *countingClient) DoMulti(context.Context, ...valkeygo.Completed) []valkeygo.ValkeyResult {
	c.multis++

	return nil
}

// A streaming call would hold a connection of the library's second pool,
// outside PoolSize, for as long as the caller keeps the stream: the client
// refuses it without reaching the library (the nil client below would panic).
func TestStreamingCallsAreRefused(t *testing.T) {
	t.Parallel()

	client := valkey.NewBounded(nil, 1)

	one := client.DoStream(t.Context(), valkeygo.Completed{})
	require.ErrorIs(t, one.Error(), valkey.ErrStreamUnsupported)
	require.False(t, one.HasNext())

	n, err := one.WriteTo(io.Discard)
	require.ErrorIs(t, err, valkey.ErrStreamUnsupported)
	require.Zero(t, n)

	many := client.DoMultiStream(t.Context(), valkeygo.Completed{}, valkeygo.Completed{})
	require.ErrorIs(t, many.Error(), valkey.ErrStreamUnsupported)
	require.False(t, many.HasNext())
}

func TestResubscribeDelayDoublesToOneSecond(t *testing.T) {
	t.Parallel()

	for attempt, want := range []time.Duration{
		50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, time.Second, time.Second,
	} {
		assert.Equal(t, want, valkey.ResubscribeDelay(attempt), "attempt %d", attempt)
	}

	assert.Equal(t, time.Second, valkey.ResubscribeDelay(1_000), "no overflow however long the server stays down")
}

// The URL carries the password, so printing the Config must not show it.
func TestPrintingAConfigDoesNotShowTheURL(t *testing.T) {
	t.Parallel()

	cfg := valkey.Config{URL: secret.New("valkey://app:Sup3rS3cret@cache:6379/0")}

	var logged strings.Builder
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("config", "valkey", cfg)

	for form, printed := range map[string]string{
		"%v": fmt.Sprintf("%v", cfg), "%+v": fmt.Sprintf("%+v", cfg), "%#v": fmt.Sprintf("%#v", cfg), "slog": logged.String(),
	} {
		assert.NotContains(t, printed, "Sup3rS3cret", form)
	}
}
