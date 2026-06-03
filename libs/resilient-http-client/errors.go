package resilient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
)

// ErrorKind classifies an [OutboundError] as retry-eligible or terminal.
type ErrorKind uint8

const (
	// KindTransient marks a temporary failure: the caller should re-queue the
	// job and try again later (network timeout, 5xx, 429, circuit open, rate
	// limited, or shutting down).
	KindTransient ErrorKind = iota
	// KindFatal marks a permanent failure: retrying will not help (4xx other
	// than 429, TLS certificate error, malformed request).
	KindFatal
)

// OutboundError is the error returned by every send method. The Transient/Fatal
// split lets workers decide whether to re-queue (transient) or drop (fatal) a
// message, mirroring the Rust crate's OutboundError.
type OutboundError struct {
	// Kind is the retry classification.
	Kind ErrorKind
	// Msg is a short human-readable description.
	Msg string
	// Err is the wrapped underlying cause, if any. Exposed via [errors.Unwrap].
	Err error
}

// Error implements the error interface.
func (e *OutboundError) Error() string {
	prefix := "transient"
	if e.Kind == KindFatal {
		prefix = "fatal"
	}
	if e.Err != nil {
		return prefix + ": " + e.Msg + ": " + e.Err.Error()
	}
	return prefix + ": " + e.Msg
}

// Unwrap exposes the wrapped cause for [errors.Is] / [errors.As].
func (e *OutboundError) Unwrap() error { return e.Err }

// Transient reports whether this error is retry-eligible.
func (e *OutboundError) Transient() bool { return e.Kind == KindTransient }

// Fatal reports whether this error is terminal.
func (e *OutboundError) Fatal() bool { return e.Kind == KindFatal }

// transient builds a transient error wrapping cause (which may be nil).
func transient(msg string, cause error) *OutboundError {
	return &OutboundError{Kind: KindTransient, Msg: msg, Err: cause}
}

// fatal builds a fatal error wrapping cause (which may be nil).
func fatal(msg string, cause error) *OutboundError {
	return &OutboundError{Kind: KindFatal, Msg: msg, Err: cause}
}

// IsTransient reports whether err is (or wraps) a transient [OutboundError].
// A nil error is not transient.
func IsTransient(err error) bool {
	var oe *OutboundError
	return errors.As(err, &oe) && oe.Kind == KindTransient
}

// IsFatal reports whether err is (or wraps) a fatal [OutboundError].
func IsFatal(err error) bool {
	var oe *OutboundError
	return errors.As(err, &oe) && oe.Kind == KindFatal
}

// classifyTransport maps a transport-level (non-HTTP-status) error from the
// underlying http.Client into a metric label and an [OutboundError].
//
// Timeouts and connection failures are transient (worth retrying); TLS
// certificate problems are fatal (the peer identity will not change on retry).
func classifyTransport(err error) (label string, oe *OutboundError) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout", transient("request timed out", err)
	case errors.Is(err, context.Canceled):
		return "canceled", transient("request canceled", err)
	}

	// TLS / certificate validation failures are not going to fix themselves.
	var (
		certInvalid x509.CertificateInvalidError
		unknownAuth x509.UnknownAuthorityError
		hostErr     x509.HostnameError
		recordErr   tls.RecordHeaderError
	)
	if errors.As(err, &certInvalid) || errors.As(err, &unknownAuth) ||
		errors.As(err, &hostErr) || errors.As(err, &recordErr) {
		return "tls_error", fatal("TLS certificate error", err)
	}

	// net.Error timeouts (e.g. dial/read deadline) are transient.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout", transient("network timeout", err)
	}

	// DNS failures: a lookup miss is usually transient (resolver blip / transient
	// outage) — let the caller's retry policy decide.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_error", transient("dns lookup failed", err)
	}

	// Generic connection-level failure (refused, reset, unreachable).
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return "connect_error", transient("connection error", err)
	}

	return "transport_error", transient("transport error", err)
}
