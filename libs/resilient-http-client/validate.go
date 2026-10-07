package resilient

import (
	"errors"
	"fmt"
)

// Validate reports every problem in c at once (joined), or nil.
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
	return fmt.Errorf("resilient: invalid config: %w", errors.Join(errs...))
}

func (t *TargetConfig) validate() []error {
	var errs []error
	bad := func(msg string) { errs = append(errs, errors.New(msg)) }

	if t.Timeout < 0 || t.MaxConcurrentWait < 0 {
		bad("timeouts must not be negative")
	}
	if t.RateLimit < 0 || t.RateBurst < 0 {
		bad("rate_limit and rate_burst must not be negative")
	}
	if t.MaxConcurrent < 0 {
		bad("max_concurrent must not be negative")
	}

	switch {
	case t.BreakerFailureRatio < 0 || t.BreakerFailureRatio > 1:
		bad("breaker_failure_ratio must be in [0, 1]")
	case t.BreakerFailureRatio > 0:
		if t.BreakerMinRequests < 1 {
			bad("breaker_min_requests must be ≥ 1")
		}
		if t.BreakerWindow <= 0 || t.BreakerOpenTimeout <= 0 {
			bad("breaker_window and breaker_open_timeout must be > 0")
		}
		if t.BreakerHalfOpenProbes < 1 {
			bad("breaker_half_open_probes must be ≥ 1")
		}
	}

	if t.RetryMaxAttempts < 0 {
		bad("retry_max_attempts must not be negative")
	}
	if t.RetryBaseDelay < 0 || t.RetryMaxDelay < t.RetryBaseDelay {
		bad("retry delays must satisfy 0 ≤ retry_base_delay ≤ retry_max_delay")
	}
	if t.RetryBudgetRatio < 0 || t.RetryBudgetRatio > 1 {
		bad("retry_budget_ratio must be in [0, 1]")
	}
	if t.RetryBudgetMinRetries < 0 || t.RetryBudgetWindow < 0 {
		bad("retry budget values must not be negative")
	}
	return errs
}
