// Package domain holds the tasks service's core model and errors — the part
// that knows nothing about HTTP, SQL, Kafka or caching. The store, api and
// event layers all speak in terms of these types.
package domain

import (
	"errors"
	"time"
)

// ErrNotFound is returned by the store when a task does not exist. The HTTP
// layer maps it to a 404 problem+json response.
var ErrNotFound = errors.New("task not found")

// ErrEmptyTitle is returned when a create request carries a blank title. The
// HTTP layer maps it to a 400 problem+json response.
var ErrEmptyTitle = errors.New("task title must not be empty")

// Task is a single to-do item.
type Task struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	CreatedAt time.Time `json:"created_at"`
}

// TaskCreatedEvent is the message published to Kafka when a task is created. It
// is the contract services/consumer decodes; keep it backwards-compatible.
type TaskCreatedEvent struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
}
