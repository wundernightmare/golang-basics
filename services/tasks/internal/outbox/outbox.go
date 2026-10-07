// Package outbox is the tasks service's transactional outbox: the change and
// the event that describes it are written in ONE database transaction
// ([Enqueue]), and a relay ([Relay]) publishes the pending events to Kafka
// afterwards. A task is therefore never created without its event (the
// broker being down delays the event, it does not lose it), and a request
// never waits on the broker.
//
// Delivery is at least once: a relay that crashes after the broker acked a
// record but before deleting its row publishes it again. Every record
// carries event_id so consumers de-duplicate on it.
//
// Ordering: rows are relayed in insertion (id) order, a batch stops at the
// first failure, and only one relay across all replicas works at a time
// (pg_try_advisory_xact_lock per batch), so events of one task reach its
// partition in commit order.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Event types written by the tasks service.
const (
	TypeTaskCreated = "task.created"
	TypeTaskUpdated = "task.updated"
	TypeTaskDeleted = "task.deleted"
)

// Event is one outbox row to write.
type Event struct {
	ID          string // event id (UUID); NewID mints one
	AggregateID string // the task id: the Kafka record key
	Type        string // TypeTask*
	Payload     []byte // the event JSON (a libs/contracts/events type)
}

// NewID returns a fresh event id.
func NewID() string { return uuid.NewString() }

// Execer is the part of a pgx transaction [Enqueue] needs.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Enqueue writes e to the outbox inside tx — the caller's transaction, the
// one that makes the change e describes. The trace context of ctx (the
// request's span) is stored with the row, so the relay can publish the
// record as part of the request's trace however much later it runs.
func Enqueue(ctx context.Context, tx Execer, e Event) error {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers, err := json.Marshal(carrier)
	if err != nil {
		return fmt.Errorf("outbox: encode headers: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (event_id, aggregate_id, event_type, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.AggregateID, e.Type, e.Payload, headers); err != nil {
		return fmt.Errorf("outbox: enqueue %s %s: %w", e.Type, e.AggregateID, err)
	}
	return nil
}
