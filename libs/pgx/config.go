package pgx

import (
	"context"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wundernightmare/golang-basics/libs/secret"
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
//	TASKS_DB_USER               [user]                user                                      (default "app")
//	TASKS_DB_PASSWORD           [password]            password                                  (default "app")
//	TASKS_DB_PASSWORD_FILE      [password_file]       file holding the password (a mounted secret);
//	                                                  read for every new connection, wins over
//	                                                  DB_PASSWORD and the URL's password        (default "")
//	TASKS_DB_NAME               [name]                database name                             (default "app")
//	TASKS_DB_SSLMODE            [sslmode]             disable|require|verify-full|…             (default "disable")
//	TASKS_DB_MAX_CONNS          [max_conns]           pool max connections; 0 in a hand-built
//	                                                  Config is pgxpool's own default           (default 10)
//	TASKS_DB_MIN_CONNS          [min_conns]           pool min (warm) connections               (default 0)
//	TASKS_DB_MAX_CONN_LIFETIME  [max_conn_lifetime]   recycle a conn after this age             (default "1h")
//	TASKS_DB_MAX_CONN_IDLE      [max_conn_idle]       close an idle conn after this             (default "30m")
//	TASKS_DB_CONNECT_TIMEOUT    [connect_timeout]     per-connection dial timeout, boot ping    (default "5s")
//	TASKS_DB_STATEMENT_TIMEOUT  [statement_timeout]   server-side cap on one statement          (default "30s")
//	TASKS_DB_LOCK_TIMEOUT       [lock_timeout]        cap on waiting for a row/table lock       (default "5s")
//	TASKS_DB_IDLE_IN_TX_TIMEOUT [idle_in_transaction_session_timeout]
//	                                                  kill a session idle inside a transaction  (default "60s")
//	TASKS_DB_OP_TIMEOUT         [op_timeout]          client-side deadline of one call through
//	                                                  DB.Conn and of an InTx begin / commit;
//	                                                  0 = none                                  (default "0")
//
// The three session timeouts are sent as run-time parameters on every
// connection (see [New]); a zero value leaves the server's setting alone.
// They are the database's own guard rails: a runaway query, a migration
// queued behind a long lock, a handler that forgot to commit — each is ended
// by Postgres instead of holding a connection (and its locks) forever.
//
// They are enforced by the server, so they bound nothing the server does not
// see: a call waiting for a free connection of an exhausted pool, or for an
// answer that never arrives over a dead network path, lasts as long as its
// context does. OpTimeout is the client-side bound for those (see [DB.Conn]).
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
	User     string        `env:"DB_USER" envDefault:"app" yaml:"user"`
	Password secret.String `env:"DB_PASSWORD" envDefault:"app" yaml:"password"`
	// PasswordFile names a file with the password, so the secret stays out of
	// the environment (and /admin/config): a Kubernetes/Docker secret mount.
	PasswordFile string `env:"DB_PASSWORD_FILE" yaml:"password_file"`
	Name         string `env:"DB_NAME" envDefault:"app" yaml:"name"`
	SSLMode      string `env:"DB_SSLMODE" envDefault:"disable" yaml:"sslmode"`

	MaxConns        int32         `env:"DB_MAX_CONNS" envDefault:"10" yaml:"max_conns"`
	MinConns        int32         `env:"DB_MIN_CONNS" envDefault:"0" yaml:"min_conns"`
	MaxConnLifetime time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h" yaml:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `env:"DB_MAX_CONN_IDLE" envDefault:"30m" yaml:"max_conn_idle"`
	ConnectTimeout  time.Duration `env:"DB_CONNECT_TIMEOUT" envDefault:"5s" yaml:"connect_timeout"`

	StatementTimeout                time.Duration `env:"DB_STATEMENT_TIMEOUT" envDefault:"30s" yaml:"statement_timeout"`
	LockTimeout                     time.Duration `env:"DB_LOCK_TIMEOUT" envDefault:"5s" yaml:"lock_timeout"`
	IdleInTransactionSessionTimeout time.Duration `env:"DB_IDLE_IN_TX_TIMEOUT" envDefault:"60s" yaml:"idle_in_transaction_session_timeout"`

	// OpTimeout, when positive, is the deadline [DB.Conn] and [DB.InTx] put
	// on each call they make. Keep it above StatementTimeout: the server's
	// cancellation then answers first, with an error that names the cause and
	// a connection that stays usable.
	OpTimeout time.Duration `env:"DB_OP_TIMEOUT" envDefault:"0" yaml:"op_timeout"`
}

// defaultConnectTimeout stands in when a hand-built Config leaves
// ConnectTimeout at zero (a zero timeout would expire the boot ping
// immediately).
const defaultConnectTimeout = 5 * time.Second

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field is defaulted, so callers can rely on a
// usable config even when nothing is set. The result has passed
// [Config.Validate].
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
		return Config{}, fmt.Errorf("pgx: parse config (prefix %q): %w", prefix, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("pgx: config (prefix %q): %w", prefix, err)
	}

	return cfg, nil
}

// DSN returns the connection string this config resolves to: the explicit URL
// when set, otherwise one assembled from the discrete fields. The password is
// included (it is a connection string, not a log line) — never log the result.
func (c Config) DSN() string {
	if !c.URL.IsZero() {
		return c.URL.Expose()
	}

	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password.Expose()),
		// An IPv6 literal needs brackets in a URL; one that already has them
		// is taken as it is.
		Host: net.JoinHostPort(strings.TrimSuffix(strings.TrimPrefix(c.Host, "["), "]"), strconv.Itoa(c.Port)),
		Path: "/" + c.Name,
	}
	q := url.Values{}
	q.Set("sslmode", c.SSLMode)
	u.RawQuery = q.Encode()

	return u.String()
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
// would refuse on the config alone: a DSN that does not parse (host, port,
// sslmode, TLS files), negative sizes and durations, MinConns above MaxConns,
// and a DB_PASSWORD_FILE that is missing, unreadable or empty. So a broken
// config is an error at boot and in --check-config, not when the pool first
// dials. The error never contains the password.
func (c Config) Validate() error {
	_, err := c.poolConfig()

	return err
}

// poolConfig resolves c into what pgxpool is built from.
func (c Config) poolConfig() (*pgxpool.Config, error) {
	for _, d := range []struct {
		key   string
		value time.Duration
	}{
		{"DB_MAX_CONN_LIFETIME", c.MaxConnLifetime},
		{"DB_MAX_CONN_IDLE", c.MaxConnIdleTime},
		{"DB_CONNECT_TIMEOUT", c.ConnectTimeout},
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

	poolCfg.ConnConfig.ConnectTimeout = c.ConnectTimeout
	if poolCfg.ConnConfig.ConnectTimeout == 0 {
		poolCfg.ConnConfig.ConnectTimeout = defaultConnectTimeout
	}

	maps.Copy(poolCfg.ConnConfig.RuntimeParams, c.runtimeParams())

	return poolCfg, nil
}
