package valkey

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	valkeygo "github.com/valkey-io/valkey-go"
)

// Config is the Valkey client configuration, populated from the environment
// with a per-service prefix (e.g. "TASKS_"), or from YAML when a service
// embeds it in its own config (field `Valkey valkey.Config` tagged yaml:"valkey", no
// envPrefix: the keys already carry VALKEY_).
//
// Two ways to point at a server, in precedence order:
//
//  1. A connection URL in <PREFIX>VALKEY_URL (e.g. "valkey://:pw@host:6379/0"
//     or the redis:// scheme). When set, the discrete fields are ignored.
//  2. The discrete Addr/Password/DB fields.
//
// Keys (with the "TASKS_" prefix as an example):
//
//	TASKS_VALKEY_URL           [url]           connection URL; overrides discrete fields  (default "")
//	TASKS_VALKEY_ADDR          [addr]          host:port                                  (default "localhost:6379")
//	TASKS_VALKEY_PASSWORD      [password]      auth password                              (default "")
//	TASKS_VALKEY_DB            [db]            logical database number                    (default 0)
//	TASKS_VALKEY_DIAL_TIMEOUT  [dial_timeout]  connection dial timeout                    (default "5s")
//	TASKS_VALKEY_OP_TIMEOUT    [op_timeout]    per-command deadline                       (default "500ms")
type Config struct {
	URL         string        `env:"VALKEY_URL" yaml:"url"`
	Addr        string        `env:"VALKEY_ADDR" envDefault:"localhost:6379" yaml:"addr"`
	Password    string        `env:"VALKEY_PASSWORD" yaml:"password" secret:"true"`
	DB          int           `env:"VALKEY_DB" envDefault:"0" yaml:"db"`
	DialTimeout time.Duration `env:"VALKEY_DIAL_TIMEOUT" envDefault:"5s" yaml:"dial_timeout"`
	// OpTimeout bounds every command (get/set/del). Without it a cache that
	// refuses connections makes valkey-go retry forever, and a request whose
	// context has no deadline hangs with it — found by the chaos suite. A
	// cache is optional: past this, the caller treats it as a miss.
	OpTimeout time.Duration `env:"VALKEY_OP_TIMEOUT" envDefault:"500ms" yaml:"op_timeout"`
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
