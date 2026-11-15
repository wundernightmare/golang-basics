// Package migrations embeds the tasks service's schema, applied on boot by
// libs/pgx.Migrate (versioned, one transaction per file, serialised across
// replicas). Files are NNN_snake_name.sql and forward-only: never edit one
// that has shipped — add the next number. safety_test.go rejects DDL that
// breaks the release still serving, can fail on existing rows or locks a
// table in use (its rules table is the list, each rule with its reason)
// unless the file carries "-- migration-safety: reviewed".
package migrations

import "embed"

// FS holds every migration at its root.
//
//go:embed *.sql
var FS embed.FS
