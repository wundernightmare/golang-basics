package httpx

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the shared HTTP-server configuration, populated from the
// environment. Services load it with a prefix (e.g. "PING_") so several
// binaries can coexist in one process tree without key collisions; an empty
// prefix reads the bare keys.
//
// Two listeners: the API listener (Addr) carries the service's own routes and
// nothing else; the admin listener (AdminAddr) carries everything operational
// — /healthz, /livez, /readyz, /metrics, /version, /admin/config,
// /admin/log-level and /debug/pprof — so an ingress that only routes to the
// API port never exposes them. A pure worker sets Addr to "" and gets only the
// admin listener.
//
// Keys (with the "PING_" prefix as an example):
//
//	PING_SERVICE_NAME            service name for logs, build_info, spans (default: executable name)
//	PING_HTTP_ADDR               API listen address                       (default ":8080")
//	PING_ADMIN_ADDR              admin listen address                     (default ":9080")
//	PING_ADMIN_TOKEN             bearer token for /admin/* and /debug/*   (default "": see ADMIN_INSECURE)
//	PING_ADMIN_INSECURE          allow an empty ADMIN_TOKEN on a non-loopback admin address (default false)
//	PING_DEBUG_TOKEN             X-Debug-Token value that turns on debug logging for one request (default "": off)
//	PING_HTTP_READ_HEADER_TIMEOUT time to read the request headers        (default "5s")
//	PING_HTTP_READ_TIMEOUT       time to read the whole request            (default "30s")
//	PING_HTTP_WRITE_TIMEOUT      time to write the response                (default "30s")
//	PING_HTTP_IDLE_TIMEOUT       keep-alive idle limit                     (default "120s")
//	PING_HTTP_REQUEST_TIMEOUT    per-request context deadline (0 = none)   (default "30s")
//	PING_HTTP_MAX_HEADER_BYTES   request header limit                      (default 1048576)
//	PING_HTTP_MAX_BODY_BYTES     request body limit; larger bodies → 413   (default 1048576)
//	PING_HTTP_SHUTDOWN_DELAY     wait after readiness flips before closing (default "2s")
//	PING_HTTP_SHUTDOWN_TIMEOUT   graceful-shutdown budget                  (default "10s")
//	PING_HTTP_SLOW_REQUEST       log a request at warn above this latency  (default "1s", 0 = off)
//	PING_HTTP_TRUSTED_PROXIES    CIDRs whose X-Forwarded-For / X-Request-Id are honoured (default: none)
//	PING_HEALTH_CHECK_INTERVAL   how often readiness checks run in the background (default "10s")
//	PING_HEALTH_CHECK_TIMEOUT    per-check timeout                         (default "3s")
//	PING_LOG_LEVEL               debug|info|warn|error                     (default "info")
//	PING_LOG_LEVEL_MAX_TTL       cap on how long PUT /admin/log-level lasts (default "24h")
//	PING_LOG_FORMAT              json|text                                 (default "json")
//	PING_LOG_SAMPLE_INITIAL      per second, per message: pass the first N (default 100, 0 = no sampling)
//	PING_LOG_SAMPLE_THEREAFTER   … then every M-th                         (default 100)
//
// The two tokens are tagged secret so GET /admin/config never shows them (see
// [Redact]); a service that embeds them in its own config struct should do
// the same. [Config.Validate] rejects an empty AdminToken on an admin address
// that is not loopback unless AdminInsecure is set: pprof, the effective
// config and the log-level switch are not things to leave open on a pod IP.
type Config struct {
	Service       string `env:"SERVICE_NAME" yaml:"service_name"`
	Addr          string `env:"HTTP_ADDR" envDefault:":8080" yaml:"http_addr"`
	AdminAddr     string `env:"ADMIN_ADDR" envDefault:":9080" yaml:"admin_addr"`
	AdminToken    string `env:"ADMIN_TOKEN" secret:"true" yaml:"admin_token"`
	AdminInsecure bool   `env:"ADMIN_INSECURE" envDefault:"false" yaml:"admin_insecure"`
	DebugToken    string `env:"DEBUG_TOKEN" secret:"true" yaml:"debug_token"`

	ReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" envDefault:"5s" yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"30s" yaml:"read_timeout"`
	WriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"30s" yaml:"write_timeout"`
	IdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT" envDefault:"120s" yaml:"idle_timeout"`
	RequestTimeout    time.Duration `env:"HTTP_REQUEST_TIMEOUT" envDefault:"30s" yaml:"request_timeout"`
	MaxHeaderBytes    int           `env:"HTTP_MAX_HEADER_BYTES" envDefault:"1048576" yaml:"max_header_bytes"`
	MaxBodyBytes      int64         `env:"HTTP_MAX_BODY_BYTES" envDefault:"1048576" yaml:"max_body_bytes"`

	ShutdownDelay   time.Duration `env:"HTTP_SHUTDOWN_DELAY" envDefault:"2s" yaml:"shutdown_delay"`
	ShutdownTimeout time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s" yaml:"shutdown_timeout"`
	SlowRequest     time.Duration `env:"HTTP_SLOW_REQUEST" envDefault:"1s" yaml:"slow_request"`
	TrustedProxies  []string      `env:"HTTP_TRUSTED_PROXIES" envSeparator:"," yaml:"trusted_proxies"`

	HealthInterval time.Duration `env:"HEALTH_CHECK_INTERVAL" envDefault:"10s" yaml:"health_check_interval"`
	HealthTimeout  time.Duration `env:"HEALTH_CHECK_TIMEOUT" envDefault:"3s" yaml:"health_check_timeout"`

	LogLevel            string        `env:"LOG_LEVEL" envDefault:"info" yaml:"log_level"`
	LogLevelMaxTTL      time.Duration `env:"LOG_LEVEL_MAX_TTL" envDefault:"24h" yaml:"log_level_max_ttl"`
	LogFormat           string        `env:"LOG_FORMAT" envDefault:"json" yaml:"log_format"`
	LogSampleInitial    int           `env:"LOG_SAMPLE_INITIAL" envDefault:"100" yaml:"log_sample_initial"`
	LogSampleThereafter int           `env:"LOG_SAMPLE_THEREAFTER" envDefault:"100" yaml:"log_sample_thereafter"`
}

// Defaults applied by [Config.withDefaults] when a service builds a Config by
// hand and leaves a field zero. They mirror the envDefault tags above.
const (
	defaultLogLevelMaxTTL    = 24 * time.Hour
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 30 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 120 * time.Second
	defaultMaxHeaderBytes    = 1 << 20
	defaultMaxBodyBytes      = 1 << 20
	defaultShutdownTimeout   = 10 * time.Second
)

// LoadConfig parses a [Config] from the environment using the given key
// prefix (use "" for no prefix), validates it and returns it fully defaulted,
// so callers can rely on every field being set.
func LoadConfig(prefix string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix}); err != nil {
		return Config{}, fmt.Errorf("httpx: parse config (prefix %q): %w", prefix, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("httpx: config (prefix %q): %w", prefix, err)
	}
	return cfg, nil
}

// withDefaults fills the zero fields a hand-built Config may leave, so a
// service that projects its own struct into [Config] gets the same server
// timeouts and limits as one that used [LoadConfig]. Negative durations mean
// "off" where the field allows it and are left alone.
func (c *Config) withDefaults() {
	if c.Service == "" {
		c.Service = defaultServiceName()
	}
	if c.LogLevelMaxTTL == 0 {
		c.LogLevelMaxTTL = defaultLogLevelMaxTTL
	}
	if c.ReadHeaderTimeout == 0 {
		c.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxHeaderBytes == 0 {
		c.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
	if c.HealthInterval == 0 {
		c.HealthInterval = defaultHealthInterval
	}
	if c.HealthTimeout == 0 {
		c.HealthTimeout = defaultHealthTimeout
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogFormat == "" {
		c.LogFormat = "json"
	}
}

// Validate applies the defaults and rejects a config the server could not
// run safely with: a typo in a level or format (which would otherwise fall
// back silently), non-positive budgets, the two listeners on one address, a
// readiness check that outlives its interval, an unparsable proxy CIDR, and
// an unauthenticated admin listener on a routable address (see AdminInsecure).
func (c *Config) Validate() error {
	c.withDefaults()
	var errs []error
	if _, err := parseLevelStrict(c.LogLevel); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	switch strings.ToLower(c.LogFormat) {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("LOG_FORMAT: unknown format %q (want json|text)", c.LogFormat))
	}
	if c.Addr == "" && c.AdminAddr == "" {
		errs = append(errs, errors.New("neither HTTP_ADDR nor ADMIN_ADDR is set"))
	}
	if c.Addr != "" && c.Addr == c.AdminAddr && !strings.HasSuffix(c.Addr, ":0") { // ":0" binds two ephemeral ports
		errs = append(errs, fmt.Errorf("HTTP_ADDR and ADMIN_ADDR are both %q", c.Addr))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("HTTP_SHUTDOWN_TIMEOUT must be positive"))
	}
	if c.ShutdownDelay < 0 {
		errs = append(errs, errors.New("HTTP_SHUTDOWN_DELAY must not be negative"))
	}
	if c.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("HTTP_MAX_BODY_BYTES must be positive"))
	}
	if c.MaxHeaderBytes <= 0 {
		errs = append(errs, errors.New("HTTP_MAX_HEADER_BYTES must be positive"))
	}
	if c.HealthTimeout >= c.HealthInterval {
		errs = append(errs, fmt.Errorf("HEALTH_CHECK_TIMEOUT (%s) must be shorter than HEALTH_CHECK_INTERVAL (%s)", c.HealthTimeout, c.HealthInterval))
	}
	if c.LogLevelMaxTTL <= 0 {
		errs = append(errs, errors.New("LOG_LEVEL_MAX_TTL must be positive"))
	}
	if c.LogSampleInitial < 0 || c.LogSampleThereafter < 0 {
		errs = append(errs, errors.New("LOG_SAMPLE_INITIAL / LOG_SAMPLE_THEREAFTER must not be negative"))
	}
	if _, err := parsePrefixes(c.TrustedProxies); err != nil {
		errs = append(errs, fmt.Errorf("HTTP_TRUSTED_PROXIES: %w", err))
	}
	if c.AdminAddr != "" && c.AdminToken == "" && !c.AdminInsecure && !isLoopback(c.AdminAddr) {
		errs = append(errs, fmt.Errorf("ADMIN_TOKEN is empty and ADMIN_ADDR %q is not loopback: set a token, or ADMIN_INSECURE=true for local development", c.AdminAddr))
	}
	return errors.Join(errs...)
}

// LogConfig projects the logging fields for [NewLogger].
func (c Config) LogConfig() LogConfig {
	return LogConfig{
		Service:          c.Service,
		Level:            c.LogLevel,
		Format:           c.LogFormat,
		SampleInitial:    c.LogSampleInitial,
		SampleThereafter: c.LogSampleThereafter,
	}
}

// parsePrefixes parses CIDRs (a bare IP is accepted as a /32 or /128).
func parsePrefixes(cidrs []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, raw := range cidrs {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if p, err := netip.ParsePrefix(s); err == nil {
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("%q is not a CIDR or IP", s)
		}
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// isLoopback reports whether addr ("host:port") binds only to a loopback
// interface. An empty host (":9080") binds every interface.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}
