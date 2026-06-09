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
// one process tree without key collisions.
//
// Two ways to point at a database, in precedence order:
//
//  1. A full DSN/URL in <PREFIX>DATABASE_URL (e.g. "postgres://app:app@db:5432/app?sslmode=disable").
//     When set, the discrete Host/Port/User/… fields are ignored.
//  2. The discrete fields below, assembled into a DSN.
//
// Pool sizing and timeouts always apply on top of whichever source is used.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_DATABASE_URL          full DSN; overrides the discrete fields  (default "")
//	TASKS_DB_HOST               host                                     (default "localhost")
//	TASKS_DB_PORT               port                                     (default 5432)
//	TASKS_DB_USER               user                                     (default "app")
//	TASKS_DB_PASSWORD           password                                 (default "app")
//	TASKS_DB_NAME               database name                            (default "app")
//	TASKS_DB_SSLMODE            disable|require|verify-full|…            (default "disable")
//	TASKS_DB_MAX_CONNS          pool max connections                     (default 10)
//	TASKS_DB_MIN_CONNS          pool min (warm) connections              (default 0)
//	TASKS_DB_MAX_CONN_LIFETIME  recycle a conn after this age            (default "1h")
//	TASKS_DB_MAX_CONN_IDLE      close an idle conn after this            (default "30m")
//	TASKS_DB_CONNECT_TIMEOUT    per-connection dial timeout              (default "5s")
type Config struct {
	URL string `env:"DATABASE_URL"`

	Host     string `env:"DB_HOST" envDefault:"localhost"`
	Port     int    `env:"DB_PORT" envDefault:"5432"`
	User     string `env:"DB_USER" envDefault:"app"`
	Password string `env:"DB_PASSWORD" envDefault:"app"`
	Name     string `env:"DB_NAME" envDefault:"app"`
	SSLMode  string `env:"DB_SSLMODE" envDefault:"disable"`

	MaxConns        int32         `env:"DB_MAX_CONNS" envDefault:"10"`
	MinConns        int32         `env:"DB_MIN_CONNS" envDefault:"0"`
	MaxConnLifetime time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTime time.Duration `env:"DB_MAX_CONN_IDLE" envDefault:"30m"`
	ConnectTimeout  time.Duration `env:"DB_CONNECT_TIMEOUT" envDefault:"5s"`
}

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
