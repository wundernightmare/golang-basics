package domain

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// Page limits.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// Cursor is a keyset position in the (created_at DESC, id DESC) order: the
// last task of the previous page. The next page starts strictly after it,
// so rows inserted or deleted meanwhile never shift a page the way OFFSET
// does, and the query is an index range scan however deep the page.
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// CursorAfter is the cursor that continues after t.
func CursorAfter(t Task) Cursor { return Cursor{CreatedAt: t.CreatedAt, ID: t.ID} }

type cursorWire struct {
	T string `json:"t"` // RFC 3339 with nanoseconds: Postgres keeps microseconds, nothing is lost
	I string `json:"i"`
}

// Encode returns the opaque form handed to clients (URL-safe base64 of a
// small JSON document). Clients must not parse it; the format may change.
func (c Cursor) Encode() string {
	b, _ := json.Marshal(cursorWire{T: c.CreatedAt.UTC().Format(time.RFC3339Nano), I: c.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor produced by [Cursor.Encode]; anything else is
// [ErrInvalidCursor].
func DecodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	var w cursorWire
	if err := json.Unmarshal(b, &w); err != nil {
		return Cursor{}, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	t, err := time.Parse(time.RFC3339Nano, w.T)
	if err != nil || w.I == "" {
		return Cursor{}, fmt.Errorf("%w: malformed position", ErrInvalidCursor)
	}
	return Cursor{CreatedAt: t, ID: w.I}, nil
}

// PageRequest asks for up to Limit tasks after Cursor (nil: the first page).
type PageRequest struct {
	Limit  int
	Cursor *Cursor
}

// Page is one page of tasks, newest first; Next is nil on the last page.
type Page struct {
	Tasks []Task
	Next  *Cursor
}
