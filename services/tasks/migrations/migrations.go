// Package migrations embeds the tasks service's schema, applied on boot by
// libs/pgx.Migrate (versioned, one transaction per file, serialised across
// replicas). Files are NNN_snake_name.sql and forward-only: never edit one
// that has shipped — add the next number.
package migrations

import "embed"

// FS holds every migration at its root.
//
//go:embed *.sql
var FS embed.FS
