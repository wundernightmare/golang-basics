package pgx

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/caarlos0/env/v11"
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
//	TASKS_DB_NAME               [name]                database name                             (default "app")
//	TASKS_DB_SSLMODE            [sslmode]             disable|require|verify-full|…             (default "disable")
//	TASKS_DB_MAX_CONNS          [max_conns]           pool max connections                      (default 10)
//	TASKS_DB_MIN_CONNS          [min_conns]           pool min (warm) connections               (default 0)
//	TASKS_DB_MAX_CONN_LIFETIME  [max_conn_lifetime]   recycle a conn after this age             (default "1h")
//	TASKS_DB_MAX_CONN_IDLE      [max_conn_idle]       close an idle conn after this             (default "30m")
//	TASKS_DB_CONNECT_TIMEOUT    [connect_timeout]     per-connection dial timeout, boot ping    (default "5s")
//	TASKS_DB_STATEMENT_TIMEOUT  [statement_timeout]   server-side cap on one statement          (default "30s")
//	TASKS_DB_LOCK_TIMEOUT       [lock_timeout]        cap on waiting for a row/table lock       (default "5s")
//	TASKS_DB_IDLE_IN_TX_TIMEOUT [idle_in_transaction_session_timeout]
//	                                                  kill a session idle inside a transaction  (default "60s")
//
// The three session timeouts are sent as run-time parameters on every
// connection (see [New]); a zero value leaves the server's setting alone.
// They are the database's own guard rails: a runaway query, a migration
// queued behind a long lock, a handler that forgot to commit — each is ended
// by Postgres instead of holding a connection (and its locks) forever.
type Config struct {
	URL string `env:"DATABASE_URL" yaml:"url"`

	Host     string `env:"DB_HOST" envDefault:"localhost" yaml:"host"`
	Port     int    `env:"DB_PORT" envDefault:"5432" yaml:"port"`
	User     string `env:"DB_USER" envDefault:"app" yaml:"user"`
	Password string `env:"DB_PASSWORD" envDefault:"app" yaml:"password" secret:"true"`
	Name     string `env:"DB_NAME" envDefault:"app" yaml:"name"`
	SSLMode  string `env:"DB_SSLMODE" envDefault:"disable" yaml:"sslmode"`

	MaxConns        int32         `env:"DB_MAX_CONNS" envDefault:"10" yaml:"max_conns"`
	MinConns        int32         `env:"DB_MIN_CONNS" envDefault:"0" yaml:"min_conns"`
	MaxConnLifetime time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h" yaml:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `env:"DB_MAX_CONN_IDLE" envDefault:"30m" yaml:"max_conn_idle"`
	ConnectTimeout  time.Duration `env:"DB_CONNECT_TIMEOUT" envDefault:"5s" yaml:"connect_timeout"`

	StatementTimeout                time.Duration `env:"DB_STATEMENT_TIMEOUT" envDefault:"30s" yaml:"statement_timeout"`
	LockTimeout                     time.Duration `env:"DB_LOCK_TIMEOUT" envDefault:"5s" yaml:"lock_timeout"`
	IdleInTransactionSessionTimeout time.Duration `env:"DB_IDLE_IN_TX_TIMEOUT" envDefault:"60s" yaml:"idle_in_transaction_session_timeout"`
}

// defaultConnectTimeout bounds the boot ping when a hand-built Config leaves
// ConnectTimeout at zero (a zero timeout would expire the ping immediately).
const defaultConnectTimeout = 5 * time.Second

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field is defaulted, so callers can rely on a
// usable config even when nothing is set.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("pgx: parse config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}

// DSN returns the connection string this config resolves to: the explicit URL
// when set, otherwise one assembled from the discrete fields. The password is
// included (it is a connection string, not a log line) — never log the result.
func (c Config) DSN() string {
	if c.URL != "" {
		return c.URL
	}
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   c.Host + ":" + strconv.Itoa(c.Port),
		Path:   "/" + c.Name,
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
