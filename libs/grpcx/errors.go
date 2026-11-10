package grpcx

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrCircuitOpen is the cause of a call rejected locally by the target's
	// circuit breaker. The error is also a gRPC status, code Unavailable, so
	// code that only looks at the status treats it as "the dependency is
	// down" — which is what it means.
	ErrCircuitOpen = errors.New("circuit open")
	// ErrUnknownTarget is returned for a target name the config does not
	// declare.
	ErrUnknownTarget = errors.New("unknown target")
	// ErrClosed is returned by [Client.Conn] after [Client.Close].
	ErrClosed = errors.New("client is closed")
)

// rejection is a call the circuit breaker refused.
type rejection struct{ target string }

func (e *rejection) Error() string { return "grpcx: " + e.target + ": " + ErrCircuitOpen.Error() }

func (e *rejection) Unwrap() error { return ErrCircuitOpen }

// GRPCStatus makes status.FromError and status.Code see Unavailable.
func (e *rejection) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}
