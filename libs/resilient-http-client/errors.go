package resilient

import (
	"errors"
	"fmt"
	"time"
)

// Kind classifies an [OutboundError]. A Kind is itself an error, so
// errors.Is(err, resilient.KindTimeout) works on any error wrapping an
// [OutboundError].
type Kind int

// The error kinds.
const (
	// KindTimeout: the per-attempt timeout expired (the caller's context did
	// not). Counts as a breaker failure; retryable.
	KindTimeout Kind = iota + 1
	// KindConnection: dial, TLS, reset or another transport failure. Counts
	// as a breaker failure; retryable.
	KindConnection
	// KindStatus: the dependency answered with status ≥ 400 (see
	// StatusCode). 5xx counts as a breaker failure; 408, 425, 429 and 5xx
	// other than 501/505 are retryable.
	KindStatus
	// KindCircuitOpen: rejected locally by the target's circuit breaker.
	KindCircuitOpen
	// KindRateLimited: rejected locally — no rate-limiter token before the
	// attempt timeout.
	KindRateLimited
	// KindBulkheadFull: rejected locally — no concurrency slot within
	// max_concurrent_wait.
	KindBulkheadFull
	// KindShutdown: rejected locally — the client is shutting down.
	KindShutdown
	// KindCanceled: the caller's context was cancelled or hit its deadline.
	KindCanceled
	// KindRedirect: a redirect was refused (another host, or too many).
	KindRedirect
	// KindInvalid: the request cannot be sent (unknown target, missing or
	// non-http(s) URL). A caller bug; never retried, not counted in metrics.
	KindInvalid
)

var kindNames = map[Kind]string{
	KindTimeout:      "timeout",
	KindConnection:   "connection",
	KindStatus:       "status",
	KindCircuitOpen:  "circuit_open",
	KindRateLimited:  "rate_limited",
	KindBulkheadFull: "bulkhead_full",
	KindShutdown:     "shutdown",
	KindCanceled:     "canceled",
	KindRedirect:     "redirect",
	KindInvalid:      "invalid",
}

// String returns the kind's name.
func (k Kind) String() string {
	if s, ok := kindNames[k]; ok {
		return s
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

// Error makes a Kind usable as an errors.Is target.
func (k Kind) Error() string { return "resilient: " + k.String() }

// OutboundError is the error of every failed send.
type OutboundError struct {
	// Kind classifies the failure.
	Kind Kind
	// Target is the logical target of the request.
	Target string
	// StatusCode is the response status for KindStatus (else 0).
	StatusCode int
	// RetryAfter is the response's Retry-After, parsed (else 0).
	RetryAfter time.Duration
	// Body is the start (at most 4 KiB) of a KindStatus response body, for
	// diagnostics. It is never part of Error().
	Body []byte
	// Err is the underlying cause, if any.
	Err error
}

// Error implements error.
func (e *OutboundError) Error() string {
	msg := "resilient: " + e.Target + ": " + e.Kind.String()
	if e.StatusCode != 0 {
		msg += fmt.Sprintf(" %d", e.StatusCode)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns the cause.
func (e *OutboundError) Unwrap() error { return e.Err }

// Is matches a [Kind]: errors.Is(err, resilient.KindTimeout).
func (e *OutboundError) Is(target error) bool {
	k, ok := target.(Kind)
	return ok && k == e.Kind
}

// Retryable reports whether err is an [OutboundError] that
// [Client.SendWithRetry] would retry (for an idempotent request).
func Retryable(err error) bool {
	var oe *OutboundError
	return errors.As(err, &oe) && retryable(oe)
}
