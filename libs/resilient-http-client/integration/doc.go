// Package integration is the integration suite of libs/resilient-http-client:
// the client against real servers, over real protocols, through a network
// that fails.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client, Toxiproxy, an HTTP/3
// implementation — stays out of the client's go.mod: importing the client
// pulls none of it.
//
// See README.md for the layers, the stack and what each test file owns.
package integration
