// Package domain holds the tasks service's core model, rules and errors — the
// part that knows nothing about HTTP, SQL, Kafka or caching. The store, api
// and event layers all speak in terms of these types.
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// MaxTitleRunes bounds a title, in characters (not bytes).
const MaxTitleRunes = 200

// Errors the store and the rules return. The HTTP layer maps each to a status.
var (
	// ErrNotFound: no task with that id (404).
	ErrNotFound = errors.New("task not found")
	// ErrVersionMismatch: an If-Match precondition named a version the task
	// no longer has — someone else changed it first (412).
	ErrVersionMismatch = errors.New("task version does not match")
	// ErrIdempotencyKeyReused: an Idempotency-Key was sent again with a
	// different request body (422).
	ErrIdempotencyKeyReused = errors.New("idempotency key was already used for a different request")
	// ErrInvalidTitle wraps every title rule violation (400).
	ErrInvalidTitle = errors.New("invalid title")
	// ErrEmptyPatch: a PATCH that changes nothing (400).
	ErrEmptyPatch = errors.New("a patch must set title and/or done")
	// ErrInvalidCursor: a pagination cursor this service did not issue (400).
	ErrInvalidCursor = errors.New("invalid cursor")
)

// Task is a single to-do item — the internal model. What crosses the wire is
// the contract: libs/contracts/tasksapi.Task on HTTP and the
// libs/contracts/events types on Kafka, both generated from api/tsp; the api
// and store layers convert at the boundary.
//
// Version starts at 1 and grows by one with every change; it is the ETag and
// the optimistic-concurrency token. CreatedAt is assigned by the database.
type Task struct {
	ID        string
	Title     string
	Done      bool
	Version   int64
	CreatedAt time.Time
}

// IdempotencyKey identifies a create request for replay: Key is the client's
// Idempotency-Key header, Hash a digest of what the request asked for (so the
// same key with a different body is told apart from a retry).
type IdempotencyKey struct {
	Key  string
	Hash string
}

// Patch is a partial update: nil fields are left alone.
type Patch struct {
	Title *string
	Done  *bool
}

// Normalize validates p and returns it with its title normalized (see
// [NormalizeTitle]).
func (p Patch) Normalize() (Patch, error) {
	if p.Title == nil && p.Done == nil {
		return Patch{}, ErrEmptyPatch
	}
	if p.Title != nil {
		t, err := NormalizeTitle(*p.Title)
		if err != nil {
			return Patch{}, err
		}
		p.Title = &t
	}
	return p, nil
}

// Apply returns t with p applied (the store does the same in SQL; this is the
// in-memory form for fakes and events).
func (p Patch) Apply(t Task) Task {
	if p.Title != nil {
		t.Title = *p.Title
	}
	if p.Done != nil {
		t.Done = *p.Done
	}
	return t
}

// NormalizeTitle applies the title rules the schema cannot fully express and
// returns the title as it will be stored: surrounding whitespace trimmed,
// then non-empty, at most [MaxTitleRunes] characters, valid UTF-8, no NUL
// (PostgreSQL TEXT rejects it — found by Schemathesis as a 500 from the
// store) and no other control characters except tab.
func NormalizeTitle(title string) (string, error) {
	t := strings.TrimSpace(title)
	switch {
	case t == "":
		return "", fmt.Errorf("%w: must not be empty or blank", ErrInvalidTitle)
	case !utf8.ValidString(t):
		return "", fmt.Errorf("%w: must be valid UTF-8", ErrInvalidTitle)
	case utf8.RuneCountInString(t) > MaxTitleRunes:
		return "", fmt.Errorf("%w: must be at most %d characters", ErrInvalidTitle, MaxTitleRunes)
	}
	for _, r := range t {
		if r == 0 {
			return "", fmt.Errorf("%w: must not contain NUL", ErrInvalidTitle)
		}
		if r < 0x20 && r != '\t' || r == 0x7f {
			return "", fmt.Errorf("%w: must not contain control characters", ErrInvalidTitle)
		}
	}
	return t, nil
}
