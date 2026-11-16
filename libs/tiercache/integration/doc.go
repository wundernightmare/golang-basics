// Package integration is the integration suite of libs/tiercache: the cache
// against a real Valkey through libs/valkey, healthy and failing.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — libs/valkey and valkey-go, testcontainers, the Docker client,
// Toxiproxy — stays out of tiercache's go.mod.
//
// See README.md for what each test file owns.
package integration
