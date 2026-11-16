package pgx

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tracehubmmp/golang-basics/libs/secret"
)

// Config is the PostgreSQL pool configuration, populated from the environment
// with a per-service prefix (e.g. "TASKS_") so several binaries can coexist in
// one process tree without key collisions, or from YAML when a service embeds
// it in its own config (see httpx.LoadYAML):
//
//	type Config struct {
//		httpx.Config `yaml:",inline"`
//		Postgres pgx.Config `yaml:"postgres"` // env keys stay TASKS_DATABASE_URL, TASKS_DB_*
//	}
//
// No envPrefix on the embedding field: the keys below already carry their
// DB_ / DATABASE_ namespace, so the documented names work unchanged.
//
// Two ways to point at a database, in precedence order:
//
//  1. A full DSN/URL in <PREFIX>DATABASE_URL (e.g. "postgres://app:app@db:5432/app?sslmode=disable").
//     When set, the discrete Host/Port/User/… fields are ignored.
//  2. The discrete fields below, assembled into a DSN.
//
// Pool sizing, timeouts and the session parameters always apply on top of
// whichever source is used.
//
// Keys (with the "TASKS_" prefix as an example; YAML key in brackets):
//
//	TASKS_DATABASE_URL          [url]                 full DSN; overrides the discrete fields   (default "")
//	TASKS_DB_HOST               [host]                host                                      (default "localhost")
//	TASKS_DB_PORT               [port]                port                                      (default 5432)
//	TASKS_DB_USER               [user]                user; required without DATABASE_URL       (default "")
//	TASKS_DB_PASSWORD           [password]            password; without DATABASE_URL either it
//	                                                  or DB_PASSWORD_FILE is required           (default "")
//	TASKS_DB_PASSWORD_FILE      [password_file]       file holding the password (a mounted secret);
//	                                                  read for every new connection, wins over
//	                                                  DB_PASSWORD and the URL's password        (default "")
//	TASKS_DB_NAME               [name]                database name                             (default "app")
//	TASKS_DB_SSLMODE            [sslmode]             disable|require|verify-full|…             (default "disable")
//	TASKS_DB_CA_FILE            [ca_file]             PEM bundle the server's certificate is
//	                                                  verified against (libpq's sslrootcert);
//	                                                  needs sslmode require, verify-ca or
//	                                                  verify-full, and no DATABASE_URL          (default "": system roots)
//	TASKS_DB_MAX_CONNS          [max_conns]           pool max connections; 0 in a hand-built
//	                                                  Config is pgxpool's own default           (default 10)
//	TASKS_DB_MIN_CONNS          [min_conns]           pool min (warm) connections               (default 0)
//	TASKS_DB_MAX_CONN_LIFETIME  [max_conn_lifetime]   recycle a conn after this age, plus up to
//	                                                  a tenth of it at random                   (default "1h")
//	TASKS_DB_MAX_CONN_IDLE      [max_conn_idle]       close an idle conn after this             (default "30m")
//	TASKS_DB_DIAL_TIMEOUT       [dial_timeout]        opening one connection (dial, TLS,
//	                                                  authentication), and the boot ping        (default "5s")
//	TASKS_DB_STATEMENT_TIMEOUT  [statement_timeout]   server-side cap on one statement          (default "30s")
//	TASKS_DB_LOCK_TIMEOUT       [lock_timeout]        cap on waiting for a row/table lock       (default "5s")
//	TASKS_DB_IDLE_IN_TX_TIMEOUT [idle_in_transaction_session_timeout]
//	                                                  kill a session idle inside a transaction  (default "60s")
//	TASKS_DB_OP_TIMEOUT         [op_timeout]          client-side deadline of one call through
//	                                                  DB.Conn and of an InTx begin / commit;
//	                                                  0 = none                                  (default "35s")
//
// The three session timeouts are sent as run-time parameters on every
// connection (see [New]); a zero value leaves the server's setting alone.
// They are the database's own guard rails: a runaway query, a migration
// queued behind a long lock, a handler that forgot to commit — each is ended
// by Postgres instead of holding a connection (and its locks) forever.
//
// They are enforced by the server, so they bound nothing the server does not
// see: a call waiting for a free connection of an exhausted pool, or for an
// answer that never arrives over a dead network path. OpTimeout is the
// client-side bound for those (see [DB.Conn]).
//
// There is no default user or password: a config that names neither
// DATABASE_URL nor credentials is refused by [Config.Validate], not connected
// as a well-known account. Authentication that needs no password (trust,
// peer, a .pgpass file) is spelled as a DATABASE_URL. The default sslmode is
// "disable": TLS to the database is opted into with DB_SSLMODE.
//
// Behind a connection pooler (PgBouncer) startup parameters do not reach the
// server: a stock PgBouncer refuses the connection, and one told to ignore
// them (ignore_startup_parameters) drops them silently — [New] then fails
// with [ErrGuardRailOff]. There, set the timeouts on the role or database
// (ALTER ROLE … SET statement_timeout = '30s') and leave these three at 0.
// In transaction pooling pgx's statement cache also needs PgBouncer's
// max_prepared_statements > 0, or default_query_exec_mode=exec in the URL.
// README "Behind PgBouncer" has the whole table; integration/pgbouncer_test.go
// proves it.
type Config struct {
	// URL carries the password, hence the type: printing the Config does not
	// print it.
	URL secret.String `env:"DATABASE_URL" yaml:"url"`

	Host     string        `env:"DB_HOST" envDefault:"localhost" yaml:"host"`
	Port     int           `env:"DB_PORT" envDefault:"5432" yaml:"port"`
	User     string        `env:"DB_USER" yaml:"user"`
	Password secret.String `env:"DB_PASSWORD" yaml:"password"`
	// PasswordFile names a file with the password, so the secret stays out of
	// the environment (and /admin/config): a Kubernetes/Docker secret mount.
	PasswordFile string `env:"DB_PASSWORD_FILE" yaml:"password_file"`
	Name         string `env:"DB_NAME" envDefault:"app" yaml:"name"`
	SSLMode      string `env:"DB_SSLMODE" envDefault:"disable" yaml:"sslmode"`
	// CAFile is the trust store for the server's certificate: only the
	// certificates in it are trusted, the system roots are not consulted. It
	// is read when the pool is built, so a rotated bundle needs a restart.
	// With sslmode=require it turns the chain check on, as libpq's
	// sslrootcert does (require then behaves as verify-ca).
	CAFile string `env:"DB_CA_FILE" yaml:"ca_file"`

	MaxConns int32 `env:"DB_MAX_CONNS" envDefault:"10" yaml:"max_conns"`
	MinConns int32 `env:"DB_MIN_CONNS" envDefault:"0" yaml:"min_conns"`
	// MaxConnLifetime is the age a connection is recycled at. Each connection
	// gets up to a tenth of it on top, at random (lifetimeJitterDivisor), so
	// that connections opened together — at boot, after a failover — do not
	// all reconnect in the same instant an hour later.
	MaxConnLifetime time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h" yaml:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `env:"DB_MAX_CONN_IDLE" envDefault:"30m" yaml:"max_conn_idle"`
	DialTimeout     time.Duration `env:"DB_DIAL_TIMEOUT" envDefault:"5s" yaml:"dial_timeout"`

	StatementTimeout                time.Duration `env:"DB_STATEMENT_TIMEOUT" envDefault:"30s" yaml:"statement_timeout"`
	LockTimeout                     time.Duration `env:"DB_LOCK_TIMEOUT" envDefault:"5s" yaml:"lock_timeout"`
	IdleInTransactionSessionTimeout time.Duration `env:"DB_IDLE_IN_TX_TIMEOUT" envDefault:"60s" yaml:"idle_in_transaction_session_timeout"`

	// OpTimeout, when positive, is the deadline [DB.Conn] and [DB.InTx] put
	// on each call they make; zero is no deadline. It has to be above
	// StatementTimeout ([Config.Validate] refuses it otherwise): the server's
	// cancellation then answers first, with an error that names the cause and
	// a connection that stays usable, while a client-side deadline that fires
	// mid-statement closes the connection.
	//
	// The loaded default is 35s: the default StatementTimeout (30s) plus the
	// default DialTimeout (5s) — a call that first has to open a connection
	// may spend that long before its statement starts. A hand-built Config
	// has no default: its zero is no deadline.
	OpTimeout time.Duration `env:"DB_OP_TIMEOUT" envDefault:"35s" yaml:"op_timeout"`
}

// ErrConfig marks a Config that cannot be used; [Config.Validate],
// [LoadConfigFrom] and [New] return it wrapped, naming the key.
var ErrConfig = errors.New("pgx: invalid config")

// defaultDialTimeout stands in when a hand-built Config leaves DialTimeout
// at zero (a zero timeout would expire the boot ping immediately).
const defaultDialTimeout = 5 * time.Second

// lifetimeJitterDivisor is the share of MaxConnLifetime a connection's own
// lifetime is stretched by at most: a tenth.
const lifetimeJitterDivisor = 10

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). The result has passed [Config.Validate], so an
// environment that names neither DATABASE_URL nor DB_USER and a password is
// an error, and so is one that still sets a key of [Config.RetiredKeys].
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	if err := refuseRetiredKeys(prefix, environ); err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("%w: parse (prefix %q): %w", ErrConfig, prefix, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("%w (prefix %q)", err, prefix)
	}

	return cfg, nil
}

// RetiredKeys names the environment keys this config once read, each with
// what to set instead. [LoadConfig] refuses an environment that sets one
// under its prefix, and so does httpx.LoadYAML for a service that has this
// config as a section — it reads the keys of the struct, and cannot see one
// that is gone. Were DB_CONNECT_TIMEOUT merely unknown, a deployment that
// tuned it would run on the default without a word.
func (Config) RetiredKeys() map[string]string {
	return map[string]string{"DB_CONNECT_TIMEOUT": "the key is DB_DIAL_TIMEOUT"}
}

// refuseRetiredKeys fails when the environment (environ, or the process
// environment when it is nil) sets a key of [Config.RetiredKeys] under
// prefix, whatever its value.
func refuseRetiredKeys(prefix string, environ map[string]string) error {
	for name, instead := range (Config{}).RetiredKeys() {
		key := prefix + name

		_, set := environ[key]
		if environ == nil {
			_, set = os.LookupEnv(key)
		}

		if set {
			return fmt.Errorf("%w: %s is no longer read: %s", ErrConfig, key, instead)
		}
	}

	return nil
}

// DSN returns the connection string this config resolves to: the explicit URL
// when set, otherwise one assembled from the discrete fields. The password is
// included (it is a connection string, not a log line) — never log the result.
func (c Config) DSN() string {
	if !c.URL.IsZero() {
		return c.URL.Expose()
	}

	query := url.Values{"sslmode": {c.SSLMode}}
	if c.CAFile != "" {
		query.Set("sslrootcert", c.CAFile)
	}

	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password.Expose()),
		// An IPv6 literal needs brackets in a URL; one that already has them
		// is taken as it is.
		Host:     net.JoinHostPort(strings.TrimSuffix(strings.TrimPrefix(c.Host, "["), "]"), strconv.Itoa(c.Port)),
		Path:     "/" + c.Name,
		RawQuery: query.Encode(),
	}

	return dsn.String()
}

// runtimeParams returns the session parameters the config sets, in the units
// Postgres expects (milliseconds). A zero duration is left out: the server's
// (or the DSN's) value stands.
func (c Config) runtimeParams() map[string]string {
	out := map[string]string{}

	for name, d := range map[string]time.Duration{
		"statement_timeout":                   c.StatementTimeout,
		"lock_timeout":                        c.LockTimeout,
		"idle_in_transaction_session_timeout": c.IdleInTransactionSessionTimeout,
	} {
		if d > 0 {
			// Never round a positive timeout down to "0", which means "off".
			out[name] = strconv.FormatInt(max(d.Milliseconds(), 1), 10)
		}
	}

	return out
}

// Validate checks what can be checked without connecting — everything [New]
// would refuse on the config alone: no DATABASE_URL and no user or no
// password, a DSN that does not parse (host, port, sslmode, TLS files),
// negative sizes and durations, MinConns above MaxConns, an OpTimeout that is
// not above StatementTimeout, a DB_PASSWORD_FILE that is missing, unreadable
// or empty, a DB_CA_FILE that is not a sound PEM bundle or that the sslmode
// would not use. So a broken config is an error at boot and in
// --check-config, not when the pool first dials. The error wraps [ErrConfig]
// and never contains the password.
func (c Config) Validate() error {
	_, err := c.poolConfig()

	return err
}

// verifyingSSLModes is the sslmode values under which the server's
// certificate chain is checked once a CA file is given.
var verifyingSSLModes = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table (Go has no const maps)
	"require": true, "verify-ca": true, "verify-full": true,
}

// credentials checks what only the discrete fields can get wrong: with a
// DATABASE_URL they are not what the pool connects with.
func (c Config) credentials() error {
	if !c.URL.IsZero() {
		if c.CAFile != "" {
			return errors.New("DB_CA_FILE is set with DATABASE_URL, which it does not apply to: put sslrootcert into the URL")
		}

		return nil
	}

	if c.User == "" {
		return errors.New("DB_USER is empty and there is no DATABASE_URL: there is no default user")
	}

	if c.Password.IsZero() && c.PasswordFile == "" {
		return errors.New("DB_PASSWORD and DB_PASSWORD_FILE are empty and there is no DATABASE_URL: there is no default password")
	}

	if c.CAFile != "" {
		if !verifyingSSLModes[c.SSLMode] {
			return fmt.Errorf("DB_CA_FILE is set but DB_SSLMODE=%s does not verify the server: use require, verify-ca or verify-full", c.SSLMode)
		}

		if _, err := secret.CertPool(c.CAFile); err != nil {
			return fmt.Errorf("DB_CA_FILE: %w", err)
		}
	}

	return nil
}

// poolConfig resolves c into what pgxpool is built from; its error wraps
// [ErrConfig].
func (c Config) poolConfig() (*pgxpool.Config, error) {
	poolCfg, err := c.resolve()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}

	return poolCfg, nil
}

func (c Config) resolve() (*pgxpool.Config, error) {
	for _, d := range []struct {
		key   string
		value time.Duration
	}{
		{"DB_MAX_CONN_LIFETIME", c.MaxConnLifetime},
		{"DB_MAX_CONN_IDLE", c.MaxConnIdleTime},
		{"DB_DIAL_TIMEOUT", c.DialTimeout},
		{"DB_STATEMENT_TIMEOUT", c.StatementTimeout},
		{"DB_LOCK_TIMEOUT", c.LockTimeout},
		{"DB_IDLE_IN_TX_TIMEOUT", c.IdleInTransactionSessionTimeout},
		{"DB_OP_TIMEOUT", c.OpTimeout},
	} {
		if d.value < 0 {
			return nil, fmt.Errorf("%s: %s is negative", d.key, d.value)
		}
	}

	if c.MaxConns < 0 || c.MinConns < 0 {
		return nil, fmt.Errorf("DB_MAX_CONNS %d / DB_MIN_CONNS %d: negative", c.MaxConns, c.MinConns)
	}

	if c.OpTimeout > 0 && c.OpTimeout <= c.StatementTimeout {
		return nil, fmt.Errorf("DB_OP_TIMEOUT %s is not above DB_STATEMENT_TIMEOUT %s: the client would give up on a statement the server is still allowed to run",
			c.OpTimeout, c.StatementTimeout)
	}

	if err := c.credentials(); err != nil {
		return nil, err
	}

	poolCfg, err := pgxpool.ParseConfig(c.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	// Only override pgxpool's own defaults when a value is actually set. A zero
	// is "leave the default", never "set to zero" — pgxpool reads a zero
	// MaxConnLifetime/IdleTime as "expires immediately", which would churn the
	// pool into the "too many failed attempts acquiring connection" failure.
	if c.MaxConns > 0 {
		poolCfg.MaxConns = c.MaxConns
	}

	if c.MinConns > 0 {
		poolCfg.MinConns = c.MinConns
	}

	if poolCfg.MinConns > poolCfg.MaxConns {
		return nil, fmt.Errorf("DB_MIN_CONNS %d is above DB_MAX_CONNS %d", poolCfg.MinConns, poolCfg.MaxConns)
	}

	if c.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = c.MaxConnLifetime
	}

	// A jitter the DSN names (pool_max_conn_lifetime_jitter) stands.
	if poolCfg.MaxConnLifetimeJitter == 0 {
		poolCfg.MaxConnLifetimeJitter = poolCfg.MaxConnLifetime / lifetimeJitterDivisor
	}

	if c.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = c.MaxConnIdleTime
	}

	if c.PasswordFile != "" {
		if _, err := secret.ReadFile(c.PasswordFile); err != nil {
			return nil, fmt.Errorf("DB_PASSWORD_FILE: %w", err)
		}
		// Read again for every connection, over the URL's password and
		// DB_PASSWORD: a rotated secret mount reaches the connections opened
		// after it without a restart.
		poolCfg.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error {
			pw, err := secret.ReadFile(c.PasswordFile)
			if err != nil {
				return fmt.Errorf("pgx: DB_PASSWORD_FILE: %w", err)
			}

			cc.Password = pw.Expose()

			return nil
		}
	}

	poolCfg.ConnConfig.ConnectTimeout = c.DialTimeout
	if poolCfg.ConnConfig.ConnectTimeout == 0 {
		poolCfg.ConnConfig.ConnectTimeout = defaultDialTimeout
	}

	maps.Copy(poolCfg.ConnConfig.RuntimeParams, c.runtimeParams())

	return poolCfg, nil
}
