package valkey

import (
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/caarlos0/env/v11"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/tracehubmmp/golang-basics/libs/secret"
)

// Config is the Valkey client configuration, populated from the environment
// with a per-service prefix (e.g. "TASKS_"), or from YAML when a service
// embeds it in its own config (field `Valkey valkey.Config` tagged yaml:"valkey", no
// envPrefix: the keys already carry VALKEY_).
//
// Two ways to point at a server, in precedence order (one is required; there
// is no default address, so a service can treat Valkey as optional — on when
// an address is set):
//
//  1. A connection URL in <PREFIX>VALKEY_URL (e.g. "valkey://:pw@host:6379/0"
//     or the redis:// scheme). When set, the discrete fields are ignored.
//  2. The discrete Addr/Password/DB fields.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_VALKEY_URL           [url]           connection URL; overrides discrete fields  (default "")
//	TASKS_VALKEY_ADDR          [addr]          host:port                                  (default "")
//	TASKS_VALKEY_PASSWORD      [password]      auth password                              (default "")
//	TASKS_VALKEY_DB            [db]            logical database number                    (default 0)
//	TASKS_VALKEY_DIAL_TIMEOUT  [dial_timeout]  connection dial timeout                    (default "5s")
//	TASKS_VALKEY_OP_TIMEOUT    [op_timeout]    per-command deadline                       (default "500ms")
//
// Connections (see the fields for what each bounds):
//
//	TASKS_VALKEY_PIPELINE_CONNS     [pipeline_conns]     shared pipelined connections, a power of two   (default 4)
//	TASKS_VALKEY_POOL_SIZE          [pool_size]          dedicated connections, at most                 (default 32)
//	TASKS_VALKEY_POOL_MIN_IDLE      [pool_min_idle]      idle dedicated connections kept open           (default 0)
//	TASKS_VALKEY_POOL_IDLE_TIMEOUT  [pool_idle_timeout]  close an idle dedicated connection after; 0 = never  (default "5m")
//	TASKS_VALKEY_CONN_WRITE_TIMEOUT [conn_write_timeout] a connection silent for this long is dead      (default "10s")
//	TASKS_VALKEY_CONN_LIFETIME      [conn_lifetime]      recycle a pipelined connection after; 0 = never (default "1h")
type Config struct {
	URL         string        `env:"VALKEY_URL" yaml:"url"`
	Addr        string        `env:"VALKEY_ADDR" yaml:"addr"`
	Password    secret.String `env:"VALKEY_PASSWORD" yaml:"password"`
	DB          int           `env:"VALKEY_DB" envDefault:"0" yaml:"db"`
	DialTimeout time.Duration `env:"VALKEY_DIAL_TIMEOUT" envDefault:"5s" yaml:"dial_timeout"`
	// OpTimeout bounds every command (get/set/del). Without it a cache that
	// refuses connections makes valkey-go retry forever, and a request whose
	// context has no deadline hangs with it — found by the chaos suite. A
	// cache is optional: past this, the caller treats it as a miss.
	OpTimeout time.Duration `env:"VALKEY_OP_TIMEOUT" envDefault:"500ms" yaml:"op_timeout"`

	// valkey-go does not hand out one connection per command. Ordinary
	// commands (everything [Cache] itself sends, a subscription included) are
	// pipelined over a fixed set of PipelineConns shared connections, opened
	// at boot and kept: that number is the client's steady footprint on the
	// server, whatever the request rate. It must be a power of two up to 256
	// (the library indexes them by a bit mask). One connection carries
	// thousands of concurrent commands; raise it for CPU-bound contention on
	// the write path, not for "more load".
	PipelineConns int `env:"VALKEY_PIPELINE_CONNS" envDefault:"4" yaml:"pipeline_conns"`
	// PoolSize bounds the dedicated connections, taken one per call by what
	// cannot share a pipeline: a blocking command (BLPOP, XREAD BLOCK), a
	// pipeline of 2000 commands or more, Dedicated / Dedicate — all reached
	// through [Cache.Client]. At the bound a caller waits for a connection
	// until its context ends, so size it to the number of concurrent blocking
	// callers. The worst case on the server is PipelineConns + PoolSize. The
	// library's own default is 1024 — in effect no bound, and enough to
	// exhaust a server's maxclients from a few replicas — and its bound
	// leaks under overload; this package enforces its own (see bounded.go).
	// The streaming calls (DoStream, DoMultiStream) would take connections
	// of a second library pool outside this bound, so the client refuses
	// them (ErrStreamUnsupported).
	PoolSize int `env:"VALKEY_POOL_SIZE" envDefault:"32" yaml:"pool_size"`
	// PoolMinIdle is how many idle dedicated connections survive
	// PoolIdleTimeout; PoolIdleTimeout closes the rest (0: an opened
	// connection is kept until Close).
	PoolMinIdle     int           `env:"VALKEY_POOL_MIN_IDLE" envDefault:"0" yaml:"pool_min_idle"`
	PoolIdleTimeout time.Duration `env:"VALKEY_POOL_IDLE_TIMEOUT" envDefault:"5m" yaml:"pool_idle_timeout"`
	// ConnWriteTimeout is the read/write deadline of every connection and the
	// interval of the client's background PING: a peer that went silent (a
	// dropped route, a paused server) is detected and the connection replaced
	// after about this long. OpTimeout bounds what a caller waits; this bounds
	// how long the client keeps using a dead connection.
	ConnWriteTimeout time.Duration `env:"VALKEY_CONN_WRITE_TIMEOUT" envDefault:"10s" yaml:"conn_write_timeout"`
	// ConnLifetime recycles a pipelined connection at this age, so the client
	// follows a DNS change or a rebalanced proxy without a restart (0: never).
	// A command caught by the swap is re-sent on the new connection by the
	// library, not failed. Dedicated connections are not recycled while in use.
	ConnLifetime time.Duration `env:"VALKEY_CONN_LIFETIME" envDefault:"1h" yaml:"conn_lifetime"`
}

// Defaults for a hand-built Config that leaves a field zero. PoolMinIdle,
// PoolIdleTimeout and ConnLifetime have a meaningful zero ("none", "never")
// and are taken as written; [LoadConfig] and YAML default them as documented.
const (
	defaultOpTimeout        = 500 * time.Millisecond
	defaultDialTimeout      = 5 * time.Second
	defaultPipelineConns    = 4
	defaultPoolSize         = 32
	defaultConnWriteTimeout = 10 * time.Second
	maxPipelineConns        = 1 << valkeygo.MaxPipelineMultiplex
)

// ErrConfig marks a Config that cannot be used; [New] returns it wrapped,
// naming the field.
var ErrConfig = errors.New("valkey: invalid config")

// withDefaults fills the zero fields that have no meaningful zero.
func (c Config) withDefaults() Config {
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}

	if c.OpTimeout <= 0 {
		c.OpTimeout = defaultOpTimeout
	}

	if c.PipelineConns == 0 {
		c.PipelineConns = defaultPipelineConns
	}

	if c.PoolSize == 0 {
		c.PoolSize = defaultPoolSize
	}

	if c.ConnWriteTimeout == 0 {
		c.ConnWriteTimeout = defaultConnWriteTimeout
	}

	return c
}

// validate rejects what the library would silently reinterpret: a negative
// pool size means "1024" to it, a multiplex above its limit is an error only
// at connect, and a negative duration means "off".
func (c Config) validate() error {
	switch {
	case c.PipelineConns < 1 || c.PipelineConns > maxPipelineConns || bits.OnesCount(uint(c.PipelineConns)) != 1:
		return fmt.Errorf("%w: pipeline_conns must be a power of two from 1 to %d, got %d", ErrConfig, maxPipelineConns, c.PipelineConns)
	case c.PoolSize < 1:
		return fmt.Errorf("%w: pool_size must be positive, got %d", ErrConfig, c.PoolSize)
	case c.PoolMinIdle < 0 || c.PoolMinIdle > c.PoolSize:
		return fmt.Errorf("%w: pool_min_idle must be between 0 and pool_size (%d), got %d", ErrConfig, c.PoolSize, c.PoolMinIdle)
	case c.PoolIdleTimeout < 0:
		return fmt.Errorf("%w: pool_idle_timeout must not be negative, got %s", ErrConfig, c.PoolIdleTimeout)
	case c.ConnWriteTimeout < 0:
		return fmt.Errorf("%w: conn_write_timeout must be positive, got %s", ErrConfig, c.ConnWriteTimeout)
	case c.ConnLifetime < 0:
		return fmt.Errorf("%w: conn_lifetime must not be negative, got %s", ErrConfig, c.ConnLifetime)
	}

	return nil
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field but the address is defaulted.
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("valkey: parse config (prefix %q): %w", prefix, err)
	}

	return cfg, nil
}

// Configured reports whether an address is set (VALKEY_URL or VALKEY_ADDR):
// the switch for a service that runs without a cache when there is none.
func (c Config) Configured() bool { return c.URL != "" || c.Addr != "" }

// clientOption translates the Config into a valkey-go ClientOption, preferring
// the URL form when present. The connection settings apply to both forms and
// win over a URL's own dial_timeout / write_timeout parameters.
func (c Config) clientOption() (valkeygo.ClientOption, error) {
	if !c.Configured() {
		return valkeygo.ClientOption{}, errors.New("valkey: VALKEY_URL or VALKEY_ADDR must be set")
	}

	c = c.withDefaults()
	if err := c.validate(); err != nil {
		return valkeygo.ClientOption{}, err
	}

	opt := valkeygo.ClientOption{
		InitAddress: []string{c.Addr},
		Password:    c.Password.Expose(),
		SelectDB:    c.DB,
	}

	if c.URL != "" {
		var err error
		if opt, err = valkeygo.ParseURL(c.URL); err != nil {
			return valkeygo.ClientOption{}, fmt.Errorf("valkey: parse url: %w", err)
		}
	}

	opt.Dialer.Timeout = c.DialTimeout
	// The library takes the count as a power: 0 is "pick by GOMAXPROCS" and
	// only a negative value means one connection.
	opt.PipelineMultiplex = bits.TrailingZeros(uint(c.PipelineConns))
	if c.PipelineConns == 1 {
		opt.PipelineMultiplex = -1
	}

	opt.BlockingPoolSize = c.PoolSize
	opt.BlockingPipeline = blockingPipeline
	opt.BlockingPoolMinSize = c.PoolMinIdle
	opt.BlockingPoolCleanup = c.PoolIdleTimeout
	opt.ConnWriteTimeout = c.ConnWriteTimeout
	opt.ConnLifetime = c.ConnLifetime

	return opt, nil
}
