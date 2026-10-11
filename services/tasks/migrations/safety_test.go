package migrations_test

// Migration safety gate: a review-time lint over the embedded migrations.
//
// A migration runs while the previous release still serves traffic, so DDL
// that breaks the old code or locks a busy table for the length of a build is
// flagged. A file that has been read and judged safe opts out with a line
// `-- migration-safety: reviewed` — coarse on purpose: a reviewer who sees it
// on a PR knows to read the SQL.

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/pgx"
	"github.com/tracehubmmp/golang-basics/services/tasks/migrations"
)

const reviewed = "-- migration-safety: reviewed"

type rule struct {
	name string
	// match reports whether one statement (comments stripped, single line,
	// upper case) breaks the rule; created holds the tables the same file
	// creates.
	match func(stmt string, created map[string]bool) bool
	why   string
}

var (
	// alterTableHead is "ALTER TABLE [IF EXISTS] [ONLY] name [*] "; what
	// follows is the comma-separated action list.
	alterTableHead = regexp.MustCompile(`^ALTER TABLE (IF EXISTS )?(ONLY )?("[^"]+"|[\w.]+)( \*)? `)
	// In each action the COLUMN keyword is optional (DROP c, RENAME c TO d,
	// ALTER c TYPE t are all valid), so the rules name what must not follow
	// instead of requiring the keyword.
	dropColumn   = regexp.MustCompile(`^DROP (COLUMN )?(IF EXISTS )?("[^"]+"|\w+)`)
	renameColumn = regexp.MustCompile(`^RENAME (COLUMN )?("[^"]+"|\w+) TO `)
	renameTable  = regexp.MustCompile(`^RENAME TO `)
	alterColType = regexp.MustCompile(`^ALTER (COLUMN )?("[^"]+"|\w+) (SET DATA )?TYPE\b`)
	setNotNull   = regexp.MustCompile(`^ALTER (COLUMN )?("[^"]+"|\w+) SET NOT NULL\b`)
	createIndex  = regexp.MustCompile(`^CREATE (UNIQUE )?INDEX\b.*?\bON (ONLY )?"?(\w+)"?`)
	createTable  = regexp.MustCompile(`^CREATE TABLE (IF NOT EXISTS )?"?(\w+)"?`)
	spaces       = regexp.MustCompile(`\s+`)
	lineComment  = regexp.MustCompile(`--[^\n]*`)
)

// notColumn lists the words after DROP / RENAME / ALTER that name something
// other than a column (ALTER TABLE t DROP CONSTRAINT c is not a column drop).
var notColumn = map[string]bool{"CONSTRAINT": true, "TO": true}

// actions returns the action list of an ALTER TABLE statement, split at the
// top-level commas (a comma inside parentheses belongs to its action), or nil
// for any other statement.
func actions(stmt string) []string {
	head := alterTableHead.FindString(stmt)
	if head == "" {
		return nil
	}
	var out []string
	depth, start := 0, len(head)
	for i := start; i < len(stmt); i++ {
		switch stmt[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(stmt[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(stmt[start:]))
}

// anyAction reports whether one of stmt's ALTER TABLE actions matches re with
// a column name (submatch col) that is not a keyword from notColumn.
func anyAction(stmt string, re *regexp.Regexp, col int) bool {
	for _, a := range actions(stmt) {
		if m := re.FindStringSubmatch(a); m != nil && (col < 0 || !notColumn[m[col]]) {
			return true
		}
	}
	return false
}

var rules = []rule{
	{"DROP COLUMN", func(s string, _ map[string]bool) bool {
		return anyAction(s, dropColumn, 3)
	}, "the running release still reads it — stop using it in one release, drop it in the next"},
	{"RENAME COLUMN", func(s string, _ map[string]bool) bool {
		return anyAction(s, renameColumn, 2)
	}, "the running release reads the old name — add new, dual-write, switch reads, drop old"},
	{"RENAME TABLE", func(s string, _ map[string]bool) bool {
		return anyAction(s, renameTable, -1)
	}, "the running release reads the old name — create the new one (or a view) first, move over, drop the old"},
	{"DROP TABLE", func(s string, _ map[string]bool) bool {
		return strings.HasPrefix(s, "DROP TABLE ")
	}, "the running release still reads it — drop it a release after the code stops using it"},
	{"ALTER COLUMN TYPE without USING", func(s string, _ map[string]bool) bool {
		for _, a := range actions(s) {
			if m := alterColType.FindStringSubmatch(a); m != nil && !notColumn[m[2]] && !strings.Contains(a, " USING ") {
				return true
			}
		}
		return false
	}, "an implicit cast can truncate or fail on existing rows — make it explicit with USING"},
	{"SET NOT NULL", func(s string, _ map[string]bool) bool {
		return anyAction(s, setNotNull, 2)
	}, "fails on existing NULLs and scans the table under a lock — backfill first"},
	{"CREATE INDEX without CONCURRENTLY", func(s string, created map[string]bool) bool {
		m := createIndex.FindStringSubmatch(s)
		// An index on a table this same file creates is harmless (it is empty).
		return m != nil && !strings.Contains(s, " CONCURRENTLY ") && !created[strings.ToLower(m[3])]
	}, "blocks writes for the whole build — libs/pgx.Migrate runs each file in a transaction, " +
		"where CONCURRENTLY is not allowed: build it by hand first (CREATE INDEX CONCURRENTLY IF NOT EXISTS), " +
		"then ship the migration"},
}

// statements splits sql into normalised statements: comments stripped,
// whitespace collapsed, upper case (no dollar-quoting in our files).
func statements(sql string) []string {
	var out []string
	for _, s := range strings.Split(lineComment.ReplaceAllString(sql, ""), ";") {
		if s = strings.TrimSpace(spaces.ReplaceAllString(s, " ")); s != "" {
			out = append(out, strings.ToUpper(s))
		}
	}
	return out
}

// findings lists the rules sql breaks.
func findings(sql string) []string {
	stmts := statements(sql)
	created := map[string]bool{}
	for _, s := range stmts {
		if m := createTable.FindStringSubmatch(s); m != nil {
			created[strings.ToLower(m[2])] = true
		}
	}
	var out []string
	for _, r := range rules {
		for _, s := range stmts {
			if r.match(s, created) {
				out = append(out, r.name)
				break
			}
		}
	}
	return out
}

func TestMigrations_NoRiskyDDLUnlessReviewed(t *testing.T) {
	t.Parallel()
	all, err := pgx.LoadMigrations(migrations.FS)
	require.NoError(t, err)
	require.NotEmpty(t, all)
	var problems []string
	for _, m := range all {
		if strings.Contains(m.SQL, reviewed) {
			continue
		}
		for _, name := range findings(m.SQL) {
			for _, r := range rules {
				if r.name == name {
					problems = append(problems, fmt.Sprintf("%s: %s — %s (or add %q)", m.File, name, r.why, reviewed))
				}
			}
		}
	}
	assert.Empty(t, problems)
}

func TestMigrationSafety_Patterns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		sql  string
		hits []string
	}{
		{"ALTER TABLE t ADD COLUMN c JSONB;", nil},
		{"ALTER TABLE t ADD COLUMN n INT NOT NULL DEFAULT 0;", nil},
		{"ALTER TABLE t DROP COLUMN c;", []string{"DROP COLUMN"}},
		{`ALTER TABLE "t" RENAME COLUMN a TO b;`, []string{"RENAME COLUMN"}},
		{"DROP TABLE t;", []string{"DROP TABLE"}},
		{"ALTER TABLE t ALTER COLUMN c TYPE JSONB;", []string{"ALTER COLUMN TYPE without USING"}},
		{"ALTER TABLE t ALTER COLUMN c TYPE JSONB USING c::jsonb;", nil},
		{"ALTER TABLE t ALTER COLUMN c SET NOT NULL;", []string{"SET NOT NULL"}},
		{"CREATE INDEX i ON t (c);", []string{"CREATE INDEX without CONCURRENTLY"}},
		{"create unique index i\n  on t (c);", []string{"CREATE INDEX without CONCURRENTLY"}},
		{"CREATE INDEX CONCURRENTLY i ON t (c);", nil},
		{"CREATE TABLE t (c INT);\nCREATE INDEX i ON t (c);", nil},
		{"CREATE TABLE IF NOT EXISTS t (c INT);\nCREATE INDEX IF NOT EXISTS i ON t (c);", nil},
		{"-- DROP TABLE t;\nSELECT 1;", nil},
		// COLUMN is optional in Postgres; the short forms are the same DDL.
		{"ALTER TABLE t DROP c;", []string{"DROP COLUMN"}},
		{"ALTER TABLE t DROP IF EXISTS c;", []string{"DROP COLUMN"}},
		{"ALTER TABLE IF EXISTS ONLY public.t DROP COLUMN IF EXISTS c;", []string{"DROP COLUMN"}},
		{"ALTER TABLE t ADD COLUMN x INT, DROP y;", []string{"DROP COLUMN"}},
		{"ALTER TABLE t RENAME a TO b;", []string{"RENAME COLUMN"}},
		{"ALTER TABLE t RENAME TO t2;", []string{"RENAME TABLE"}},
		{"ALTER TABLE t ALTER c TYPE INT;", []string{"ALTER COLUMN TYPE without USING"}},
		{"ALTER TABLE t ALTER c SET DATA TYPE INT;", []string{"ALTER COLUMN TYPE without USING"}},
		{"ALTER TABLE t ALTER c TYPE INT USING c::int;", nil},
		{"ALTER TABLE t ALTER c SET NOT NULL;", []string{"SET NOT NULL"}},
		// Not columns: constraints, defaults, nullability relaxed.
		{"ALTER TABLE t DROP CONSTRAINT c;", nil},
		{"ALTER TABLE t DROP CONSTRAINT IF EXISTS c;", nil},
		{"ALTER TABLE t RENAME CONSTRAINT a TO b;", nil},
		{"ALTER TABLE t ALTER c DROP DEFAULT;", nil},
		{"ALTER TABLE t ALTER COLUMN c DROP NOT NULL;", nil},
		{"ALTER TABLE t ALTER c SET DEFAULT 0;", nil},
		{"ALTER TABLE t ADD CONSTRAINT k CHECK (a IN (1, 2)), ADD COLUMN z INT;", nil},
	} {
		assert.Equal(t, tc.hits, findings(tc.sql), tc.sql)
	}
}
