package resilient

import (
	"errors"
	"fmt"
	"math"

	"github.com/wundernightmare/golang-basics/libs/secret"
)

// ErrConfig is in every error a config is refused with: by
// [Config.Validate], by [ParseYAML] (a document that does not parse included)
// and by [New].
var ErrConfig = errors.New("resilient: invalid config")

// Validate reports every problem in c at once (joined), or nil. It reads the
// ca_file of each target that names one.
func (c *Config) Validate() error {
	var errs []error

	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.MaxIdleConns < 0 || c.MaxIdleConnsPerHost < 0 || c.MaxConnsPerHost < 0 {
		bad("connection pool sizes must not be negative")
	}

	if c.IdleConnTimeout < 0 || c.ResponseHeaderTimeout < 0 || c.HTTP2PingInterval < 0 || c.HTTP2PingTimeout < 0 {
		bad("transport timeouts must not be negative")
	}

	if c.MaxRedirects < 0 {
		bad("max_redirects must not be negative")
	}

	if len(c.Targets) == 0 {
		bad("targets: at least one is required")
	}

	seen := make(map[string]bool, len(c.Targets))
	for i := range c.Targets {
		t := &c.Targets[i]
		if t.Name == "" {
			bad("target #%d: name is empty", i)
		} else if seen[t.Name] {
			bad("target %q: declared twice", t.Name)
		}

		seen[t.Name] = true
		for _, err := range t.validate() {
			bad("target %q: %w", t.Name, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("%w: %w", ErrConfig, errors.Join(errs...))
}

func (t *TargetConfig) validate() []error {
	var errs []error

	bad := func(msg string) { errs = append(errs, errors.New(msg)) }

	if t.CAFile != "" {
		if _, err := secret.CertPool(t.CAFile); err != nil {
			errs = append(errs, fmt.Errorf("ca_file: %w", err))
		}
	}

	if t.Timeout < 0 || t.MaxConcurrentWait < 0 {
		bad("timeouts must not be negative")
	}

	// Written so that NaN fails: every comparison with it is false, and a
	// limiter or a budget built from one never limits.
	if !(t.RateLimit >= 0) || math.IsInf(t.RateLimit, 1) || t.RateBurst < 0 {
		bad("rate_limit must be a finite number, and neither it nor rate_burst negative")
	}

	if t.MaxConcurrent < 0 {
		bad("max_concurrent must not be negative")
	}

	for _, err := range problems(t.Breaker.Validate()) {
		bad("breaker: " + err.Error())
	}

	if t.RetryMaxAttempts < 0 {
		bad("retry_max_attempts must not be negative")
	}

	if t.RetryBaseDelay < 0 || t.RetryMaxDelay < t.RetryBaseDelay {
		bad("retry delays must satisfy 0 ≤ retry_base_delay ≤ retry_max_delay")
	}

	if !(t.RetryBudgetRatio >= 0 && t.RetryBudgetRatio <= 1) {
		bad("retry_budget_ratio must be in [0, 1]")
	}

	if t.RetryBudgetMinRetries < 0 || t.RetryBudgetWindow < 0 {
		bad("retry budget values must not be negative")
	}

	return errs
}

// problems is the list an errors.Join error was made of, so each gets a line
// that names its target; any other error is a list of one, nil of none.
func problems(err error) []error {
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return joined.Unwrap()
	}

	if err == nil {
		return nil
	}

	return []error{err}
}
