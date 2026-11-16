// Package integration is the integration suite of libs/pgx: the pool, the
// unit of work and Migrate against a real PostgreSQL, behind PgBouncer in
// both pooling modes, and through Toxiproxy when the network fails.
//
// It is a module of its own, next to the code it tests, so that what the
// suite needs — testcontainers, the Docker client, Toxiproxy — stays out of
// pgx's go.mod.
//
// See README.md for the stack and what each test owns.
package integration
