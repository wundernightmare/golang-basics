// Package integration is the integration suite of libs/s3: the client
// against a real S3-compatible store (RustFS), and behind a proxy that takes
// the store away.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client, Toxiproxy — stays out of
// the go.mod of libs/s3.
//
// See README.md for what the suite owns.
package integration
