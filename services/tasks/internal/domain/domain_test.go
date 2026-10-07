package domain_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

func TestNormalizeTitle(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
		ok             bool
	}{
		{"plain", "ship it", "ship it", true},
		{"trimmed", "  ship it \n", "ship it", true},
		{"tab inside is fine", "a\tb", "a\tb", true},
		{"exactly the limit in runes", strings.Repeat("ж", domain.MaxTitleRunes), strings.Repeat("ж", domain.MaxTitleRunes), true},
		{"empty", "", "", false},
		{"blank", " \t\n ", "", false},
		{"too long", strings.Repeat("x", domain.MaxTitleRunes+1), "", false},
		{"NUL", "a\x00b", "", false},
		{"control character", "a\x07b", "", false},
		{"DEL", "a\x7fb", "", false},
		{"invalid UTF-8", "a\xffb", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := domain.NormalizeTitle(tc.in)
			if !tc.ok {
				require.ErrorIs(t, err, domain.ErrInvalidTitle)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPatch(t *testing.T) {
	_, err := domain.Patch{}.Normalize()
	require.ErrorIs(t, err, domain.ErrEmptyPatch)

	blank := "  "
	_, err = domain.Patch{Title: &blank}.Normalize()
	require.ErrorIs(t, err, domain.ErrInvalidTitle)

	title, done := "  new  ", true
	p, err := domain.Patch{Title: &title, Done: &done}.Normalize()
	require.NoError(t, err)
	assert.Equal(t, "new", *p.Title)
	assert.Equal(t, "  new  ", title, "the caller's string is not modified")

	got := p.Apply(domain.Task{ID: "1", Title: "old", Version: 3})
	assert.Equal(t, domain.Task{ID: "1", Title: "new", Done: true, Version: 3}, got, "Apply does not bump the version: the store does")

	onlyDone := false
	got = domain.Patch{Done: &onlyDone}.Apply(domain.Task{Title: "keep", Done: true})
	assert.Equal(t, "keep", got.Title)
	assert.False(t, got.Done)
}

func TestCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 25, 12, 30, 45, 123456000, time.UTC) // microseconds, as Postgres stores them
	c := domain.CursorAfter(domain.Task{ID: "abc", CreatedAt: at})
	enc := c.Encode()
	assert.NotContains(t, enc, "=", "URL-safe, unpadded: fits in a query string as is")
	assert.NotContains(t, enc, "+")

	got, err := domain.DecodeCursor(enc)
	require.NoError(t, err)
	assert.True(t, at.Equal(got.CreatedAt), "no precision lost")
	assert.Equal(t, "abc", got.ID)

	for _, bad := range []string{"", "!!!", "bm90IGpzb24", "e30"} { // not base64, not JSON, "{}"
		_, err := domain.DecodeCursor(bad)
		require.ErrorIs(t, err, domain.ErrInvalidCursor, bad)
	}
}
