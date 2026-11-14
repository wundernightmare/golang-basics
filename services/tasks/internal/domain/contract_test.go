package domain_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// titlePatterns returns the title patterns of the published contract, as Go
// regular expressions (\uXXXX → \x{XXXX}; nothing else in them is
// ECMA-specific — a class escape such as \s would be, and is refused).
func titlePatterns(t *testing.T) []*regexp.Regexp {
	t.Helper()

	spec, err := os.ReadFile("../../../../api/openapi3/tasks.openapi.yaml")
	require.NoError(t, err)

	escape := regexp.MustCompile(`\\u([0-9A-Fa-f]{4})`)

	var patterns []*regexp.Regexp

	for line := range strings.SplitSeq(string(spec), "\n") {
		pattern, found := strings.CutPrefix(strings.TrimSpace(line), "pattern: ")
		if !found || !strings.Contains(pattern, `\u007F`) {
			continue // not a title pattern
		}

		portable := escape.ReplaceAllString(pattern, `\x{$1}`)
		require.NotRegexp(t, `\\[sSwWdDbB]`, portable,
			"a class escape means different code points to different regex engines: list them")

		patterns = append(patterns, regexp.MustCompile(portable))
	}

	require.Len(t, patterns, 3, "Task, CreateTaskRequest and UpdateTaskRequest")

	return patterns
}

// The contract promises that a schema-compliant title is accepted
// (Schemathesis checks exactly that, with generated strings). The pattern
// and NormalizeTitle are two statements of one rule, and they drifted on what
// "whitespace" is: the pattern said \s, the server trims Unicode White_Space,
// and a title of one U+00A0 was valid to the generator and blank to the
// server. Here every code point is tried alone and beside a letter.
func TestTitlePatternAcceptsOnlyWhatTheServerAccepts(t *testing.T) {
	t.Parallel()

	patterns := titlePatterns(t)

	for r := rune(0); r <= unicode.MaxRune; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue // surrogates are not characters
		}

		// Every code point up to the end of the CJK punctuation block —
		// which holds all the whitespace and control characters there are —
		// every one Unicode itself calls space or control, and beyond that
		// a sample: the rest are ordinary characters to both sides, and
		// trying each of a million of them takes a minute under -race.
		if r > 0x30FF && !unicode.IsSpace(r) && !unicode.IsControl(r) && r%251 != 0 {
			continue
		}

		for _, title := range []string{string(r), "a" + string(r), string(r) + "a", "a" + string(r) + "a"} {
			_, err := domain.NormalizeTitle(title)

			for _, pattern := range patterns {
				if pattern.MatchString(title) && err != nil {
					require.Failf(t, "the schema accepts a title the server rejects", "%+q: %v", title, err)
				}
			}
		}
	}
}

// And the other direction, where it matters: the pattern refuses nothing an
// ordinary title contains.
func TestTitlePatternAcceptsOrdinaryTitles(t *testing.T) {
	t.Parallel()

	for _, title := range []string{
		"a", "buy milk", "  padded  ", "tab\tinside", "купить молоко", "\u725b\u4e73\u3092\u8cb7\u3046", "non breaking inside", "emoji 🥛",
	} {
		_, err := domain.NormalizeTitle(title)
		require.NoError(t, err, "%q", title)

		for _, pattern := range titlePatterns(t) {
			assert.True(t, pattern.MatchString(title), "%q", title)
		}
	}

	for _, blank := range []string{"", " ", " ", "\u0085", " ", "　", " \t "} {
		for _, pattern := range titlePatterns(t) {
			assert.False(t, pattern.MatchString(blank), "%+q is blank", blank)
		}
	}
}
