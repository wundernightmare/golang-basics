package grpcx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

// maxAttemptsLimit is grpc-go's own cap on attempts per call (its
// WithMaxCallAttempts default): a larger retry_max_attempts would be clamped
// silently, so it is rejected instead.
const maxAttemptsLimit = 5

// minKeepaliveTime is the shortest ping interval grpc-go accepts (it raises a
// shorter one silently, so it is rejected instead).
const minKeepaliveTime = 10 * time.Second

// Load-balancing policies.
const (
	// RoundRobin spreads calls over every address the target resolves to.
	RoundRobin = "round_robin"
	// PickFirst uses the first address that connects.
	PickFirst = "pick_first"
)

// Config is the declarative configuration of a [Client].
//
// Zero means what each field's comment says — usually "disabled" — and is
// never silently replaced by a default. Start from [DefaultConfig] /
// [DefaultTarget] (or use [LoadConfig], which overlays the YAML on those
// defaults) and change what differs.
type Config struct {
	// Targets declares one connection and policy set per logical target.
	Targets []TargetConfig `yaml:"targets"`
}

// TargetConfig is the connection and policy of one logical target.
type TargetConfig struct {
	// Name identifies the target in [Client.Conn] and in metric labels.
	// Required, unique.
	Name string `yaml:"name"`
	// Address is the gRPC target URI, e.g. "dns:///billing.internal:443".
	// Required.
	Address string `yaml:"address"`
	// Insecure connects without TLS (default false: TLS with the system
	// roots; other credentials go through [WithDialOptions]).
	Insecure bool `yaml:"insecure"`

	// Timeout is the deadline of a call that has none of its own, retries
	// included; a caller's shorter deadline wins (default 5s; 0 = none — only
	// the caller's context).
	Timeout time.Duration `yaml:"timeout"`
	// WaitForReady queues calls while the connection is down instead of
	// failing them at once with Unavailable (default false).
	WaitForReady bool `yaml:"wait_for_ready"`
	// LoadBalancing is "round_robin" (default) or "pick_first".
	LoadBalancing string `yaml:"load_balancing"`

	// KeepaliveTime pings a connection that has been silent this long
	// (default 0 = off). Not below 10s, and agree it with the server: a gRPC
	// server closes a connection that pings more often than its enforcement
	// policy allows — every 5 minutes by default.
	KeepaliveTime time.Duration `yaml:"keepalive_time"`
	// KeepaliveTimeout closes the connection when a ping is not answered in
	// time (default 20s; must be > 0 with keepalive_time).
	KeepaliveTimeout time.Duration `yaml:"keepalive_timeout"`

	// RetryMaxAttempts is the total attempts of a call, the first included
	// (default 3; at most 5; 0 or 1 = no retries).
	RetryMaxAttempts int `yaml:"retry_max_attempts"`
	// RetryBaseDelay is the first backoff; it doubles per retry, ±20 % jitter
	// (default 100ms; must be > 0 with retries).
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	// RetryMaxDelay caps one backoff (default 5s; must be ≥ RetryBaseDelay).
	RetryMaxDelay time.Duration `yaml:"retry_max_delay"`
	// RetryCodes are the status codes retried, by name (default
	// [UNAVAILABLE]; must not be empty with retries).
	RetryCodes []string `yaml:"retry_codes"`
	// RetryMethods limits retries to these services ("pkg.Service") or
	// methods ("pkg.Service/Method"); empty (the default) = every method.
	// gRPC does not know which methods are idempotent: list them when a
	// retried code can mean "partly done".
	RetryMethods []string `yaml:"retry_methods"`
	// RetryThrottleMaxTokens is the size of gRPC's retry-throttling bucket: a
	// failed call takes a token, a successful one returns
	// RetryThrottleTokenRatio, and retries stop while the bucket is at most
	// half full (default 10; at most 1000; 0 = no throttling).
	RetryThrottleMaxTokens int `yaml:"retry_throttle_max_tokens"`
	// RetryThrottleTokenRatio is the tokens a successful call returns
	// (default 0.1; must be > 0 with throttling).
	RetryThrottleTokenRatio float64 `yaml:"retry_throttle_token_ratio"`

	// The circuit breaker: breaker_failure_ratio (default 0.5; 0 = none),
	// breaker_min_requests (20), breaker_window (10s), breaker_open_timeout
	// (30s), breaker_half_open_probes (1).
	resilience.BreakerConfig `yaml:",inline"`
}

// DefaultConfig returns a config with the given targets, each built with
// [DefaultTarget].
func DefaultConfig(targets ...TargetConfig) Config {
	return Config{Targets: targets}
}

// DefaultTarget returns the defaults documented on [TargetConfig].
func DefaultTarget(name, address string) TargetConfig {
	return TargetConfig{
		Name:                    name,
		Address:                 address,
		Timeout:                 5 * time.Second,
		LoadBalancing:           RoundRobin,
		KeepaliveTimeout:        20 * time.Second,
		RetryMaxAttempts:        3,
		RetryBaseDelay:          100 * time.Millisecond,
		RetryMaxDelay:           5 * time.Second,
		RetryCodes:              []string{"UNAVAILABLE"},
		RetryThrottleMaxTokens:  10,
		RetryThrottleTokenRatio: 0.1,
		BreakerConfig:           resilience.DefaultBreakerConfig(),
	}
}

// LoadConfig parses YAML (each target over [DefaultTarget]), rejects unknown
// keys and validates the result.
func LoadConfig(data []byte) (Config, error) {
	var c Config
	if err := strictDecode(data, &c); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("grpcx: parse config: %w", err)
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}

	return c, nil
}

// UnmarshalYAML decodes a target over [DefaultTarget], so omitted keys keep
// their defaults. yaml.Node.Decode forgets KnownFields, hence the re-encode.
func (t *TargetConfig) UnmarshalYAML(n *yaml.Node) error {
	type plain TargetConfig

	raw, err := yaml.Marshal(n)
	if err != nil {
		return err //nolint:wrapcheck // a yaml error about the node, as is
	}

	v := plain(DefaultTarget("", ""))
	if err := strictDecode(raw, &v); err != nil {
		return fmt.Errorf("target at line %d: %w", n.Line, err)
	}

	*t = TargetConfig(v)

	return nil
}

func strictDecode(data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	return dec.Decode(v) //nolint:wrapcheck // wrapped by the callers
}

// Validate reports every problem in c at once (joined), or nil.
func (c *Config) Validate() error {
	var errs []error

	seen := make(map[string]bool, len(c.Targets))
	for i := range c.Targets {
		t := &c.Targets[i]
		if t.Name == "" {
			errs = append(errs, fmt.Errorf("target #%d: name is empty", i))
		} else if seen[t.Name] {
			errs = append(errs, fmt.Errorf("target %q: declared twice", t.Name))
		}

		seen[t.Name] = true
		for _, err := range t.validate() {
			errs = append(errs, fmt.Errorf("target %q: %w", t.Name, err))
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("grpcx: invalid config: %w", errors.Join(errs...))
}

func (t *TargetConfig) validate() []error {
	var errs []error

	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if t.Address == "" {
		bad("address is empty")
	}

	if t.Timeout < 0 {
		bad("timeout must not be negative")
	}

	if t.LoadBalancing != RoundRobin && t.LoadBalancing != PickFirst {
		bad("load_balancing must be %q or %q", RoundRobin, PickFirst)
	}

	// if / else, not a switch: a case expression has no block in Go's cover
	// profile, and mutation testing would report these as not covered.
	if t.KeepaliveTime < 0 || t.KeepaliveTimeout < 0 {
		bad("keepalive durations must not be negative")
	} else if t.KeepaliveTime > 0 {
		if t.KeepaliveTime < minKeepaliveTime {
			bad("keepalive_time must be 0 or ≥ %s", minKeepaliveTime)
		}

		if t.KeepaliveTimeout == 0 {
			bad("keepalive_timeout must be > 0 with keepalive_time")
		}
	}

	errs = append(errs, t.validateRetry()...)
	errs = append(errs, t.BreakerConfig.Validate()...) //nolint:staticcheck // QF1008: the embedded name says whose Validate this is, next to validateRetry

	return errs
}

func (t *TargetConfig) validateRetry() []error {
	var errs []error

	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if t.RetryMaxAttempts < 0 || t.RetryMaxAttempts > maxAttemptsLimit {
		bad("retry_max_attempts must be in [0, %d]", maxAttemptsLimit)
	}

	if t.RetryThrottleMaxTokens < 0 || t.RetryThrottleMaxTokens > 1000 {
		bad("retry_throttle_max_tokens must be in [0, 1000]")
	}

	if t.RetryThrottleMaxTokens > 0 && t.RetryThrottleTokenRatio <= 0 {
		bad("retry_throttle_token_ratio must be > 0 with throttling")
	}

	for _, m := range t.RetryMethods {
		if _, _, ok := splitMethod(m); !ok {
			bad("retry_methods: %q is not pkg.Service or pkg.Service/Method", m)
		}
	}

	if !t.retries() {
		return errs
	}

	if t.RetryBaseDelay <= 0 || t.RetryMaxDelay < t.RetryBaseDelay {
		bad("retry delays must satisfy 0 < retry_base_delay ≤ retry_max_delay")
	}

	if len(t.RetryCodes) == 0 {
		bad("retry_codes must not be empty with retries")
	}

	for _, name := range t.RetryCodes {
		if code, ok := parseCode(name); !ok || code == codes.OK {
			bad("retry_codes: %q is not a retryable gRPC status code name", name)
		}
	}

	return errs
}

// keepalive is the target's keepalive pings; ok is false when they are off.
func (t *TargetConfig) keepalive() (params keepalive.ClientParameters, ok bool) {
	if t.KeepaliveTime <= 0 {
		return keepalive.ClientParameters{}, false
	}

	return keepalive.ClientParameters{Time: t.KeepaliveTime, Timeout: t.KeepaliveTimeout}, true
}

// retries reports whether the target retries at all.
func (t *TargetConfig) retries() bool { return t.RetryMaxAttempts > 1 }

// splitMethod splits "pkg.Service" or "pkg.Service/Method" (a leading slash
// is tolerated: it is how gRPC itself writes a full method).
func splitMethod(s string) (service, method string, ok bool) {
	service, method, _ = strings.Cut(strings.TrimPrefix(s, "/"), "/")

	return service, method, service != "" && !strings.Contains(method, "/")
}

// parseCode reads a status code by its canonical name ("UNAVAILABLE").
func parseCode(name string) (codes.Code, bool) {
	var c codes.Code
	if err := c.UnmarshalJSON([]byte(`"` + name + `"`)); err != nil {
		return 0, false
	}

	return c, true
}
