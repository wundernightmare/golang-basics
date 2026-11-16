// Package integration is the integration suite of libs/valkey: the cache
// against a real Valkey, and behind a proxy that takes the server away.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client, Toxiproxy — stays out of
// the go.mod of libs/valkey.
//
// See README.md for what the suite owns.
package integration
