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

// Task is a single to-do item — the internal model. What crosses the wire is
// the contract: libs/contracts/tasksapi.Task on HTTP and
// libs/contracts/events.TaskCreatedEvent on Kafka, both generated from
// api/tsp; the api layer converts at the boundary.
type Task struct {
	ID        string
	Title     string
	Done      bool
	CreatedAt time.Time
}
