package valkey

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	valkeygo "github.com/valkey-io/valkey-go"
)

// Config is the Valkey client configuration, populated from the environment
// with a per-service prefix (e.g. "TASKS_").
//
// Two ways to point at a server, in precedence order:
//
//  1. A connection URL in <PREFIX>VALKEY_URL (e.g. "valkey://:pw@host:6379/0"
//     or the redis:// scheme). When set, the discrete fields are ignored.
//  2. The discrete Addr/Password/DB fields.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_VALKEY_URL           connection URL; overrides discrete fields  (default "")
//	TASKS_VALKEY_ADDR          host:port                                  (default "localhost:6379")
//	TASKS_VALKEY_PASSWORD      auth password                              (default "")
//	TASKS_VALKEY_DB            logical database number                    (default 0)
//	TASKS_VALKEY_DIAL_TIMEOUT  connection dial timeout                    (default "5s")
type Config struct {
	URL         string        `env:"VALKEY_URL"`
	Addr        string        `env:"VALKEY_ADDR" envDefault:"localhost:6379"`
	Password    string        `env:"VALKEY_PASSWORD"`
	DB          int           `env:"VALKEY_DB" envDefault:"0"`
	DialTimeout time.Duration `env:"VALKEY_DIAL_TIMEOUT" envDefault:"5s"`
}

// LoadConfig parses a [Config] from the environment using the given key prefix
// (use "" for no prefix). Every field is defaulted.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("valkey: parse config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}

// clientOption translates the Config into a valkey-go ClientOption, preferring
// the URL form when present.
func (c Config) clientOption() (valkeygo.ClientOption, error) {
	if c.URL != "" {
		opt, err := valkeygo.ParseURL(c.URL)
		if err != nil {
			return valkeygo.ClientOption{}, fmt.Errorf("valkey: parse url: %w", err)
		}
		if c.DialTimeout > 0 {
			opt.Dialer.Timeout = c.DialTimeout
		}
		return opt, nil
	}
	opt := valkeygo.ClientOption{
		InitAddress: []string{c.Addr},
		Password:    c.Password,
		SelectDB:    c.DB,
	}
	opt.Dialer.Timeout = c.DialTimeout
	return opt, nil
}
