package valkey

import (
	"crypto/tls"
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
//     or the redis:// scheme; valkeys:// and rediss:// are TLS). When set,
//     the discrete fields are ignored.
//  2. The discrete Addr/Username/Password/DB fields.
//
// TLS, CAFile and the client certificate apply to both. One server only: a URL
// that names several (addr=) or a sentinel (master_set=) is a config error.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_VALKEY_URL           [url]           connection URL; overrides discrete fields  (default "")
//	TASKS_VALKEY_ADDR          [addr]          host:port                                  (default "")
//	TASKS_VALKEY_USERNAME      [username]      ACL user                                   (default "": the server's default user)
//	TASKS_VALKEY_PASSWORD      [password]      auth password                              (default "")
//	TASKS_VALKEY_DB            [db]            logical database number                    (default 0)
//	TASKS_VALKEY_TLS           [tls]           connect over TLS                           (default false)
//	TASKS_VALKEY_CA_FILE       [ca_file]       PEM bundle that signs the server's cert    (default "": system roots)
//	TASKS_VALKEY_CERT_FILE     [cert_file]     client certificate (PEM), mTLS             (default "")
//	TASKS_VALKEY_KEY_FILE      [key_file]      its private key (PEM)                      (default "")
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
	// URL holds the password, hence the type: no formatter or logger prints it.
	URL      secret.String `env:"VALKEY_URL" yaml:"url"`
	Addr     string        `env:"VALKEY_ADDR" yaml:"addr"`
	Username string        `env:"VALKEY_USERNAME" yaml:"username"`
	Password secret.String `env:"VALKEY_PASSWORD" yaml:"password"`
	DB       int           `env:"VALKEY_DB" envDefault:"0" yaml:"db"`
	// TLS turns TLS on, for the URL form too: a valkey:// URL is then dialled
	// as valkeys:// would be. The server's name is verified against the host
	// of the address.
	TLS bool `env:"VALKEY_TLS" envDefault:"false" yaml:"tls"`
	// CAFile is the trust store of the TLS connection — it replaces the
	// system roots, it does not add to them. Set without TLS (the flag or a
	// TLS scheme in the URL) it is a config error, not an ignored key.
	CAFile string `env:"VALKEY_CA_FILE" yaml:"ca_file"`
	// CertFile and KeyFile are the certificate (PEM, the chain after it) and
	// the private key the client presents to a server that asks for one
	// (mTLS). Both or neither, and only with TLS on. They are read when the
	// client is built: a rotated pair reaches the server after a restart.
	CertFile    string        `env:"VALKEY_CERT_FILE" secret:"false" yaml:"cert_file"`
	KeyFile     string        `env:"VALKEY_KEY_FILE" secret:"false" yaml:"key_file"`
	DialTimeout time.Duration `env:"VALKEY_DIAL_TIMEOUT" envDefault:"5s" yaml:"dial_timeout"`
	// OpTimeout bounds every command [Cache] sends through its own methods
	// (the readiness PING and a subscription aside, see [Cache]). Without it a cache that
	// does not answer holds a request whose context has no deadline for as
	// long as the connection takes to notice — found by the chaos suite. A
	// cache is optional: past this, the caller treats it as a miss. Commands
	// are not retried by the client (see clientOption): one attempt, then
	// the error.
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

// ErrConfig marks a Config that cannot be used; [Config.Validate],
// [LoadConfigFrom] and [New] return it wrapped, naming the field.
var ErrConfig = errors.New("valkey: invalid config")

// withDefaults fills the zero fields that have no meaningful zero.
func (c Config) withDefaults() Config {
	if c.DialTimeout == 0 {
		c.DialTimeout = defaultDialTimeout
	}

	if c.OpTimeout == 0 {
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

// Validate reports why [New] would reject the Config without dialling, or
// nil: everything New checks before it connects is checked here, the URL and
// the TLS files (which are read) included. A Config without an address is
// rejected like any other — a service that treats the cache as optional asks
// [Config.Configured] first.
func (c Config) Validate() error {
	_, err := c.clientOption()

	return err
}

// validate rejects what the library would silently reinterpret: a negative
// pool size means "1024" to it, a multiplex above its limit is an error only
// at connect, and a negative duration means "off".
func (c Config) validate() error {
	if c.DB < 0 {
		return fmt.Errorf("%w: db must not be negative, got %d", ErrConfig, c.DB)
	}

	if c.DialTimeout < 0 {
		return fmt.Errorf("%w: dial_timeout must be positive, got %s", ErrConfig, c.DialTimeout)
	}

	if c.OpTimeout < 0 {
		return fmt.Errorf("%w: op_timeout must be positive, got %s", ErrConfig, c.OpTimeout)
	}

	if c.PipelineConns < 1 || c.PipelineConns > maxPipelineConns || bits.OnesCount(uint(c.PipelineConns)) != 1 {
		return fmt.Errorf("%w: pipeline_conns must be a power of two from 1 to %d, got %d", ErrConfig, maxPipelineConns, c.PipelineConns)
	}

	if c.PoolSize < 1 {
		return fmt.Errorf("%w: pool_size must be positive, got %d", ErrConfig, c.PoolSize)
	}

	if c.PoolMinIdle < 0 || c.PoolMinIdle > c.PoolSize {
		return fmt.Errorf("%w: pool_min_idle must be between 0 and pool_size (%d), got %d", ErrConfig, c.PoolSize, c.PoolMinIdle)
	}

	if c.PoolIdleTimeout < 0 {
		return fmt.Errorf("%w: pool_idle_timeout must not be negative, got %s", ErrConfig, c.PoolIdleTimeout)
	}

	if c.ConnWriteTimeout < 0 {
		return fmt.Errorf("%w: conn_write_timeout must be positive, got %s", ErrConfig, c.ConnWriteTimeout)
	}

	if c.ConnLifetime < 0 {
		return fmt.Errorf("%w: conn_lifetime must not be negative, got %s", ErrConfig, c.ConnLifetime)
	}

	return nil
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field but the address is defaulted. A Config
// with an address is validated ([Config.Validate]); one without is returned
// as read — it means "no Valkey" ([Config.Configured]).
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("%w: parse (prefix %q): %w", ErrConfig, prefix, err)
	}

	if cfg.Configured() {
		if err := cfg.Validate(); err != nil {
			return Config{}, fmt.Errorf("%w (prefix %q)", err, prefix)
		}
	}

	return cfg, nil
}

// Configured reports whether an address is set (VALKEY_URL or VALKEY_ADDR):
// the switch for a service that runs without a cache when there is none.
func (c Config) Configured() bool { return !c.URL.IsZero() || c.Addr != "" }

// clientOption translates the Config into a valkey-go ClientOption, preferring
// the URL form when present. The connection settings apply to both forms and
// win over a URL's own dial_timeout / write_timeout parameters.
func (c Config) clientOption() (valkeygo.ClientOption, error) {
	if !c.Configured() {
		return valkeygo.ClientOption{}, fmt.Errorf("%w: VALKEY_URL or VALKEY_ADDR must be set", ErrConfig)
	}

	c = c.withDefaults()
	if err := c.validate(); err != nil {
		return valkeygo.ClientOption{}, err
	}

	opt := valkeygo.ClientOption{
		InitAddress: []string{c.Addr},
		Username:    c.Username,
		Password:    c.Password.Expose(),
		SelectDB:    c.DB,
	}

	if !c.URL.IsZero() {
		var err error
		// The parser's message is withheld whole: net/url quotes the URL in
		// it, and what fails to parse is, as a rule, a password with an
		// unescaped character.
		if opt, err = valkeygo.ParseURL(c.URL.Expose()); err != nil {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: url is not usable: want valkey[s]://[user:password@]host:port[/db] "+
				"with the special characters of the password percent-encoded (the parser's message is withheld: it quotes the URL)", ErrConfig)
		}

		// The library builds another kind of client from either — a cluster
		// client from several addresses, a sentinel one from master_set —
		// and nothing of this package is tested against those.
		if len(opt.InitAddress) != 1 || opt.Sentinel.MasterSet != "" {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: url must name one server: several addresses (addr=, a cluster) "+
				"and master_set (a sentinel) are not supported", ErrConfig)
		}
	}

	if c.TLS && opt.TLSConfig == nil {
		// No ServerName: the dialer takes it from the address.
		opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	if c.CAFile != "" {
		if opt.TLSConfig == nil {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: ca_file is set but TLS is off: set tls or use a valkeys:// URL", ErrConfig)
		}

		pool, err := secret.CertPool(c.CAFile)
		if err != nil {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: ca_file: %w", ErrConfig, err)
		}

		opt.TLSConfig.RootCAs = pool
	}

	if c.CertFile != "" || c.KeyFile != "" {
		if c.CertFile == "" || c.KeyFile == "" {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: cert_file and key_file must be set together", ErrConfig)
		}

		if opt.TLSConfig == nil {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: cert_file is set but TLS is off: set tls or use a valkeys:// URL", ErrConfig)
		}

		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return valkeygo.ClientOption{}, fmt.Errorf("%w: cert_file / key_file: %w", ErrConfig, err)
		}

		opt.TLSConfig.Certificates = []tls.Certificate{pair}
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
	// UPSTREAM-WORKAROUND(valkey-go ≤ 1.0.79): the library's own retry of
	// read-only commands is off (docs/UPSTREAM-WORKAROUNDS.md). After a
	// connection error it sends the same command object again on a new
	// connection and, on success, recycles it — while the writer of the old
	// connection may still be reading it (singleClient.Do: a data race, seen
	// as garbage written to a dying socket at best).
	// TestReadsUnderConnectionFaultsDoNotRace reproduces it when this line is
	// removed. Nothing is lost that the callers rely on: a failed read is a
	// miss for Aside and for tiercache, bounded by OpTimeout either way.
	opt.DisableRetry = true
	opt.ConnWriteTimeout = c.ConnWriteTimeout
	opt.ConnLifetime = c.ConnLifetime

	return opt, nil
}
