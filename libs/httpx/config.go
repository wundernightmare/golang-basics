package httpx

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tracehubmmp/golang-basics/libs/secret"
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
//	PING_HTTP_REQUEST_TIMEOUT    per-request context deadline (0 = none; X-Request-Timeout-Ms may shorten it) (default "30s")
//	PING_HTTP_REQUEST_TIMEOUT_MIN floor for a deadline shortened by X-Request-Timeout-Ms (default "1s", 0 = none)
//	PING_HTTP_MAX_HEADER_BYTES   request header limit                      (default 1048576)
//	PING_HTTP_MAX_BODY_BYTES     request body limit; larger bodies → 413   (default 1048576)
//	PING_HTTP_RATE_LIMIT         requests per second the API accepts, all clients together; over it → 429 (default 0 = no limit)
//	PING_HTTP_RATE_BURST         depth of that token bucket                (default 0 = ceil(rate))
//	PING_HTTP_RATE_LIMIT_PER_CLIENT  requests per second per client address (default 0 = no limit)
//	PING_HTTP_RATE_BURST_PER_CLIENT  depth of a client's bucket            (default 0 = ceil(rate))
//	PING_HTTP_RATE_CLIENTS_MAX   client buckets kept; when all are in use a new client is shed (default 10000)
//	PING_HTTP_SHUTDOWN_DELAY     wait after readiness flips before closing (default "2s")
//	PING_HTTP_SHUTDOWN_TIMEOUT   graceful-shutdown budget                  (default "10s")
//	PING_HTTP_SLOW_REQUEST       log a request at warn above this latency  (default "1s", 0 = off)
//	PING_HTTP_TRUSTED_PROXIES    CIDRs whose X-Forwarded-For / X-Request-Id are honoured (default: none)
//	PING_HEALTH_CHECK_INTERVAL   how often readiness checks run in the background (default "10s")
//	PING_HEALTH_CHECK_TIMEOUT    per-check timeout                         (default "3s")
//	PING_LOG_LEVEL               debug|info|warn|error                     (default "info")
//	PING_LOG_LEVEL_MAX_TTL       cap on how long PUT /admin/log-level lasts (default "24h")
//	PING_LOG_FORMAT              json|text|ecs (ecs: JSON with Elastic Common Schema field names) (default "json")
//	PING_LOG_REDACT_KEYS         comma-separated attribute keys logged as "[redacted]", on top of the built-in secret names (default "")
//	PING_LOG_SAMPLE_INITIAL      per second, per message: pass the first N (default 100, 0 = no sampling)
//	PING_LOG_SAMPLE_THEREAFTER   … then every M-th                         (default 100)
//	PING_RUNTIME_MEMORY_LIMIT_RATIO   Go memory limit as a share of the container's memory limit; negative = leave it alone (default 0.85)
//	PING_RUNTIME_MEMORY_LIMIT_REFRESH how often the container limit is re-read; negative = only at start (default "30s")
//	PING_RUNTIME_TRACEBACK       none|single|all|system|crash: goroutines printed when the process dies (default "all")
//	PING_DIAG_DUMP_DIR           directory (an emptyDir) for crash reports and artifacts not uploaded yet; "" = no crash capture (default "")
//	PING_DIAG_KEY_PREFIX         first segment of the object keys in the dump sink (default "diagnostics")
//	PING_DIAG_MEMORY_THRESHOLD   share of the container's memory limit at which profiles are captured; negative = never (default 0.9)
//	PING_DIAG_MEMORY_COOLDOWN    shortest time between two memory captures (default "10m")
//	PING_DIAG_CHECK_INTERVAL     how often container memory is looked at     (default "5s")
//	PING_DIAG_UPLOAD_TIMEOUT     one upload to the dump sink                 (default "15s")
//	PING_DIAG_SPOOL_RETRY        how often the dump directory is re-sent     (default "1m")
//	PING_DIAG_SPOOL_MAX_FILES    files kept in the dump directory            (default 20)
//	PING_DIAG_CRASH_MAX_BYTES    longest crash report shipped                (default 8388608)
//	PING_DIAG_FLIGHT_RECORDER    keep the last seconds of the execution trace in memory and ship a snapshot on an anomaly (default false)
//	PING_DIAG_FLIGHT_MIN_AGE     how far back the window reaches             (default "10s")
//	PING_DIAG_FLIGHT_MAX_BYTES   upper bound of the window in memory         (default 8388608)
//	PING_DIAG_FLIGHT_SLOW_REQUEST  a request this slow triggers a snapshot; 0 = requests never do (default "0s")
//	PING_DIAG_FLIGHT_COOLDOWN    shortest time between two snapshots         (default "5m")
//
// The DIAG_ keys do nothing until the service installs a sink
// ([Server.SetDumpSink]) or sets DIAG_DUMP_DIR (diagnostics.go).
//
// The RUNTIME_ keys are applied by [Server.Run]; a GOMEMLIMIT in the
// environment overrides the ratio. GOMAXPROCS has no key: the runtime follows
// the container's CPU limit by itself (docs/RUNTIME.md).
//
// The two tokens are tagged secret so GET /admin/config never shows them (see
// [Redact]); a service that embeds them in its own config struct should do
// the same. [Config.Validate] rejects an empty AdminToken on an admin address
// that is not loopback unless AdminInsecure is set: pprof, the effective
// config and the log-level switch are not things to leave open on a pod IP.
type Config struct {
	Service       string        `env:"SERVICE_NAME" yaml:"service_name"`
	Addr          string        `env:"HTTP_ADDR" envDefault:":8080" yaml:"http_addr"`
	AdminAddr     string        `env:"ADMIN_ADDR" envDefault:":9080" yaml:"admin_addr"`
	AdminToken    secret.String `env:"ADMIN_TOKEN" yaml:"admin_token"`
	AdminInsecure bool          `env:"ADMIN_INSECURE" envDefault:"false" yaml:"admin_insecure"`
	DebugToken    secret.String `env:"DEBUG_TOKEN" yaml:"debug_token"`

	ReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" envDefault:"5s" yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"30s" yaml:"read_timeout"`
	WriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"30s" yaml:"write_timeout"`
	IdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT" envDefault:"120s" yaml:"idle_timeout"`
	RequestTimeout    time.Duration `env:"HTTP_REQUEST_TIMEOUT" envDefault:"30s" yaml:"request_timeout"`
	// RequestTimeoutMin is the shortest deadline X-Request-Timeout-Ms can
	// ask for: a smaller value is raised to it, so a 504 always means the
	// server needed longer than this, never that a caller asked for 1 ms.
	// Without it any client could fill the 5xx-rate alert with timeouts it
	// requested itself. 0 = no floor.
	RequestTimeoutMin time.Duration `env:"HTTP_REQUEST_TIMEOUT_MIN" envDefault:"1s" yaml:"request_timeout_min"`
	MaxHeaderBytes    int           `env:"HTTP_MAX_HEADER_BYTES" envDefault:"1048576" yaml:"max_header_bytes"`
	MaxBodyBytes      int64         `env:"HTTP_MAX_BODY_BYTES" envDefault:"1048576" yaml:"max_body_bytes"`
	// The server's own inbound rate limit (ratelimit.go): the last line of
	// defence, behind whatever the ingress does. 0 = off.
	RateLimit          float64 `env:"HTTP_RATE_LIMIT" envDefault:"0" yaml:"rate_limit"`
	RateBurst          int     `env:"HTTP_RATE_BURST" envDefault:"0" yaml:"rate_burst"`
	RateLimitPerClient float64 `env:"HTTP_RATE_LIMIT_PER_CLIENT" envDefault:"0" yaml:"rate_limit_per_client"`
	RateBurstPerClient int     `env:"HTTP_RATE_BURST_PER_CLIENT" envDefault:"0" yaml:"rate_burst_per_client"`
	RateClientsMax     int     `env:"HTTP_RATE_CLIENTS_MAX" envDefault:"10000" yaml:"rate_clients_max"`

	ShutdownDelay   time.Duration `env:"HTTP_SHUTDOWN_DELAY" envDefault:"2s" yaml:"shutdown_delay"`
	ShutdownTimeout time.Duration `env:"HTTP_SHUTDOWN_TIMEOUT" envDefault:"10s" yaml:"shutdown_timeout"`
	SlowRequest     time.Duration `env:"HTTP_SLOW_REQUEST" envDefault:"1s" yaml:"slow_request"`
	TrustedProxies  []string      `env:"HTTP_TRUSTED_PROXIES" envSeparator:"," yaml:"trusted_proxies"`

	HealthInterval time.Duration `env:"HEALTH_CHECK_INTERVAL" envDefault:"10s" yaml:"health_check_interval"`
	HealthTimeout  time.Duration `env:"HEALTH_CHECK_TIMEOUT" envDefault:"3s" yaml:"health_check_timeout"`

	LogLevel            string        `env:"LOG_LEVEL" envDefault:"info" yaml:"log_level"`
	LogLevelMaxTTL      time.Duration `env:"LOG_LEVEL_MAX_TTL" envDefault:"24h" yaml:"log_level_max_ttl"`
	LogFormat           string        `env:"LOG_FORMAT" envDefault:"json" yaml:"log_format"`
	LogRedactKeys       []string      `env:"LOG_REDACT_KEYS" yaml:"log_redact_keys"`
	LogSampleInitial    int           `env:"LOG_SAMPLE_INITIAL" envDefault:"100" yaml:"log_sample_initial"`
	LogSampleThereafter int           `env:"LOG_SAMPLE_THEREAFTER" envDefault:"100" yaml:"log_sample_thereafter"`

	// RuntimeMemoryLimitRatio is the share of the container's memory limit
	// handed to the garbage collector as its soft limit (GOMEMLIMIT). The
	// rest is headroom for what the collector does not count: goroutine
	// stacks in flight, memory held by C libraries, the kernel's accounting
	// of page cache and sockets. Negative leaves the runtime alone.
	RuntimeMemoryLimitRatio   float64       `env:"RUNTIME_MEMORY_LIMIT_RATIO" envDefault:"0.85" yaml:"runtime_memory_limit_ratio"`
	RuntimeMemoryLimitRefresh time.Duration `env:"RUNTIME_MEMORY_LIMIT_REFRESH" envDefault:"30s" yaml:"runtime_memory_limit_refresh"`
	RuntimeTraceback          string        `env:"RUNTIME_TRACEBACK" envDefault:"all" yaml:"runtime_traceback"`

	// Post-mortem diagnostics (diagnostics.go): crash reports and
	// memory-pressure profiles, shipped to the sink of [Server.SetDumpSink].
	DiagDumpDir         string        `env:"DIAG_DUMP_DIR" yaml:"diag_dump_dir"`
	DiagKeyPrefix       string        `env:"DIAG_KEY_PREFIX" envDefault:"diagnostics" yaml:"diag_key_prefix"`
	DiagMemoryThreshold float64       `env:"DIAG_MEMORY_THRESHOLD" envDefault:"0.9" yaml:"diag_memory_threshold"`
	DiagMemoryCooldown  time.Duration `env:"DIAG_MEMORY_COOLDOWN" envDefault:"10m" yaml:"diag_memory_cooldown"`
	DiagCheckInterval   time.Duration `env:"DIAG_CHECK_INTERVAL" envDefault:"5s" yaml:"diag_check_interval"`
	DiagUploadTimeout   time.Duration `env:"DIAG_UPLOAD_TIMEOUT" envDefault:"15s" yaml:"diag_upload_timeout"`
	DiagSpoolRetry      time.Duration `env:"DIAG_SPOOL_RETRY" envDefault:"1m" yaml:"diag_spool_retry"`
	DiagSpoolMaxFiles   int           `env:"DIAG_SPOOL_MAX_FILES" envDefault:"20" yaml:"diag_spool_max_files"`
	DiagCrashMaxBytes   int64         `env:"DIAG_CRASH_MAX_BYTES" envDefault:"8388608" yaml:"diag_crash_max_bytes"`

	DiagFlightRecorder    bool          `env:"DIAG_FLIGHT_RECORDER" envDefault:"false" yaml:"diag_flight_recorder"`
	DiagFlightMinAge      time.Duration `env:"DIAG_FLIGHT_MIN_AGE" envDefault:"10s" yaml:"diag_flight_min_age"`
	DiagFlightMaxBytes    int64         `env:"DIAG_FLIGHT_MAX_BYTES" envDefault:"8388608" yaml:"diag_flight_max_bytes"`
	DiagFlightSlowRequest time.Duration `env:"DIAG_FLIGHT_SLOW_REQUEST" envDefault:"0s" yaml:"diag_flight_slow_request"`
	DiagFlightCooldown    time.Duration `env:"DIAG_FLIGHT_COOLDOWN" envDefault:"5m" yaml:"diag_flight_cooldown"`
}

// Defaults applied by [Config.withDefaults] when a service builds a Config by
// hand and leaves a field zero. They mirror the envDefault tags above.
const (
	defaultLogLevelMaxTTL     = 24 * time.Hour
	defaultReadHeaderTimeout  = 5 * time.Second
	defaultReadTimeout        = 30 * time.Second
	defaultWriteTimeout       = 30 * time.Second
	defaultIdleTimeout        = 120 * time.Second
	defaultMaxHeaderBytes     = 1 << 20
	defaultMaxBodyBytes       = 1 << 20
	defaultDiagFlightMinAge   = 10 * time.Second
	defaultDiagFlightMaxBytes = 8 << 20
	defaultRateClientsMax     = 10_000
	defaultShutdownTimeout    = 10 * time.Second
)

// LoadConfig parses a [Config] from the environment using the given key
// prefix (use "" for no prefix), validates it and returns it fully defaulted,
// so callers can rely on every field being set.
func LoadConfig(prefix string) (Config, error) { return LoadConfigFrom(prefix, nil) }

// LoadConfigFrom is [LoadConfig] reading environ instead of the process
// environment (nil means the process environment). Tests pass a map, so they
// need no t.Setenv and can run in parallel.
func LoadConfigFrom(prefix string, environ map[string]string) (Config, error) {
	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: prefix, Environment: environ}); err != nil {
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

	if c.RateClientsMax == 0 {
		c.RateClientsMax = defaultRateClientsMax
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

	if c.RuntimeMemoryLimitRatio == 0 {
		c.RuntimeMemoryLimitRatio = defaultMemoryLimitRatio
	}

	if c.RuntimeMemoryLimitRefresh == 0 {
		c.RuntimeMemoryLimitRefresh = defaultMemoryLimitRefresh
	}

	if c.RuntimeTraceback == "" {
		c.RuntimeTraceback = defaultTraceback
	}

	c.withDiagDefaults()
}

func (c *Config) withDiagDefaults() {
	if c.DiagKeyPrefix == "" {
		c.DiagKeyPrefix = defaultDiagKeyPrefix
	}

	if c.DiagMemoryThreshold == 0 {
		c.DiagMemoryThreshold = defaultDiagMemoryThreshold
	}

	if c.DiagMemoryCooldown == 0 {
		c.DiagMemoryCooldown = defaultDiagMemoryCooldown
	}

	if c.DiagCheckInterval == 0 {
		c.DiagCheckInterval = defaultDiagCheckInterval
	}

	if c.DiagUploadTimeout == 0 {
		c.DiagUploadTimeout = defaultDiagUploadTimeout
	}

	if c.DiagSpoolRetry == 0 {
		c.DiagSpoolRetry = defaultDiagSpoolRetry
	}

	if c.DiagSpoolMaxFiles == 0 {
		c.DiagSpoolMaxFiles = defaultDiagSpoolMaxFiles
	}

	if c.DiagFlightMinAge == 0 {
		c.DiagFlightMinAge = defaultDiagFlightMinAge
	}

	if c.DiagFlightMaxBytes == 0 {
		c.DiagFlightMaxBytes = defaultDiagFlightMaxBytes
	}

	if c.DiagCrashMaxBytes == 0 {
		c.DiagCrashMaxBytes = defaultDiagCrashMaxBytes
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
	case "json", "text", "ecs":
	default:
		errs = append(errs, fmt.Errorf("LOG_FORMAT: unknown format %q (want json|text|ecs)", c.LogFormat))
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

	if c.RequestTimeoutMin < 0 {
		errs = append(errs, errors.New("HTTP_REQUEST_TIMEOUT_MIN must not be negative"))
	}

	if c.RequestTimeout > 0 && c.RequestTimeoutMin > c.RequestTimeout {
		errs = append(errs, fmt.Errorf("HTTP_REQUEST_TIMEOUT_MIN (%s) must not exceed HTTP_REQUEST_TIMEOUT (%s)", c.RequestTimeoutMin, c.RequestTimeout))
	}

	if c.ShutdownDelay < 0 {
		errs = append(errs, errors.New("HTTP_SHUTDOWN_DELAY must not be negative"))
	}

	if c.RateLimit < 0 || c.RateLimitPerClient < 0 || c.RateBurst < 0 || c.RateBurstPerClient < 0 || c.RateClientsMax < 0 {
		errs = append(errs, errors.New("HTTP_RATE_* values must not be negative"))
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

	errs = append(errs, c.validateRuntime()...)

	if _, err := parsePrefixes(c.TrustedProxies); err != nil {
		errs = append(errs, fmt.Errorf("HTTP_TRUSTED_PROXIES: %w", err))
	}

	if c.AdminAddr != "" && c.AdminToken.IsZero() && !c.AdminInsecure && !isLoopback(c.AdminAddr) {
		errs = append(errs, fmt.Errorf("ADMIN_TOKEN is empty and ADMIN_ADDR %q is not loopback: set a token, or ADMIN_INSECURE=true for local development", c.AdminAddr))
	}

	return errors.Join(errs...)
}

// validateRuntime checks the RUNTIME_ and DIAG_ keys.
func (c *Config) validateRuntime() []error {
	var errs []error

	if c.RuntimeMemoryLimitRatio > 1 {
		errs = append(errs, fmt.Errorf("RUNTIME_MEMORY_LIMIT_RATIO must be at most 1 (negative = off), got %v", c.RuntimeMemoryLimitRatio))
	}

	if c.DiagMemoryThreshold > 1 {
		errs = append(errs, fmt.Errorf("DIAG_MEMORY_THRESHOLD must be at most 1 (negative = off), got %v", c.DiagMemoryThreshold))
	}

	if c.DiagMemoryCooldown < 0 || c.DiagCheckInterval <= 0 || c.DiagUploadTimeout <= 0 || c.DiagSpoolRetry <= 0 {
		errs = append(errs, errors.New("DIAG_CHECK_INTERVAL, DIAG_UPLOAD_TIMEOUT and DIAG_SPOOL_RETRY must be positive, DIAG_MEMORY_COOLDOWN not negative"))
	}

	if c.DiagSpoolMaxFiles < 1 || c.DiagCrashMaxBytes < 1 {
		errs = append(errs, errors.New("DIAG_SPOOL_MAX_FILES and DIAG_CRASH_MAX_BYTES must be positive"))
	}

	if c.DiagFlightRecorder && (c.DiagFlightMinAge < 0 || c.DiagFlightMaxBytes < 0 || c.DiagFlightSlowRequest < 0 || c.DiagFlightCooldown < 0) {
		errs = append(errs, errors.New("DIAG_FLIGHT_* durations and sizes must not be negative"))
	}

	if strings.Trim(c.DiagKeyPrefix, "/") == "" {
		errs = append(errs, errors.New("DIAG_KEY_PREFIX must not be empty"))
	}

	if !validTraceback(c.RuntimeTraceback) {
		errs = append(errs, fmt.Errorf("RUNTIME_TRACEBACK: unknown level %q (want none|single|all|system|crash)", c.RuntimeTraceback))
	}

	return errs
}

// LogConfig projects the logging fields for [NewLogger].
func (c Config) LogConfig() LogConfig {
	return LogConfig{
		Service:          c.Service,
		Level:            c.LogLevel,
		Format:           c.LogFormat,
		RedactKeys:       c.LogRedactKeys,
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
