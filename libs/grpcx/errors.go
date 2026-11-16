package grpcx

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tracehubmmp/golang-basics/libs/resilience"
)

var (
	// ErrCircuitOpen is the cause of a call rejected locally by the target's
	// circuit breaker; such an error is resilience.ErrOpen too. It is also a
	// gRPC status, code Unavailable, so code that only looks at the status
	// treats it as "the dependency is down" — which is what it means.
	ErrCircuitOpen = errors.New("circuit open")
	// ErrUnknownTarget is returned for a target name the config does not
	// declare.
	ErrUnknownTarget = errors.New("unknown target")
	// ErrClosed is returned by [Client.Conn] after [Client.Close], and is the
	// cause of a call started after it on a connection taken earlier (a gRPC
	// status as well, code Canceled — what gRPC itself answers on a closed
	// connection).
	ErrClosed = errors.New("client is closed")
	// ErrConfig is in every error a config is refused with: by
	// [Config.Validate], by [ParseYAML] (a document that does not parse
	// included) and by [New].
	ErrConfig = errors.New("grpcx: invalid config")
	// ErrLocalRejection is the cause of an error made by [LocalRejection].
	ErrLocalRejection = errors.New("rejected locally")
)

// rejection is a call the circuit breaker refused.
type rejection struct{ target string }

func (e *rejection) Error() string { return "grpcx: " + e.target + ": " + ErrCircuitOpen.Error() }

func (e *rejection) Unwrap() []error { return []error{ErrCircuitOpen, resilience.ErrOpen} }

// GRPCStatus makes status.FromError and status.Code see Unavailable.
func (e *rejection) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}

// closedError is a call started after [Client.Close].
type closedError struct{ target string }

func (e *closedError) Error() string { return "grpcx: " + e.target + ": " + ErrClosed.Error() }

func (e *closedError) Unwrap() error { return ErrClosed }

func (e *closedError) GRPCStatus() *status.Status {
	return status.New(codes.Canceled, e.Error())
}

// LocalRejection is the error for an interceptor of the caller's (see
// [WithDialOptions]) that refuses a call itself, before the dependency saw
// it: a rate limiter, a bulkhead. The circuit breaker ignores such a call —
// it says nothing about the dependency, where any other error that is not a
// gRPC status counts as a failure (Unknown), and a status counts as the
// dependency's answer. The error is a gRPC status with the given code and
// message (the code is the call's outcome label) and
// errors.Is(err, ErrLocalRejection).
func LocalRejection(code codes.Code, msg string) error {
	return &localRejection{status: status.New(code, msg)}
}

type localRejection struct{ status *status.Status }

func (e *localRejection) Error() string { return e.status.Err().Error() }

func (e *localRejection) Unwrap() error { return ErrLocalRejection }

func (e *localRejection) GRPCStatus() *status.Status { return e.status }
