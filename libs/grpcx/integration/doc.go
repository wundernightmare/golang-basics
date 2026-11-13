// Package integration is the integration suite of libs/grpcx: the gRPC wiring
// against real servers, over TLS and cleartext, through a network that fails.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client, Toxiproxy, the OTLP
// protocol stubs — stays out of grpcx's go.mod.
//
// See README.md for the stack and what each test file owns.
package integration
