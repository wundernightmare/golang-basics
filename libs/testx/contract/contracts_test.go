package contract_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/contracts/events"
	"github.com/wundernightmare/golang-basics/libs/testx/contract"
)

// The generated event types and the schemas they were generated from must
// agree on the wire: what a producer marshals from the type is what the
// schema admits, with the optional fields absent and present. The test lives
// here, not in libs/contracts, so that module stays free of dependencies.
func TestGeneratedEventsMarshalToTheirSchemas(t *testing.T) {
	t.Parallel()

	const (
		id      = "0b6f3c0e-5a0e-4d0b-9c57-3f0c6f0f4a11"
		eventID = "7d0a1c1e-1f65-4e5c-8a51-9f2f3f1f6b22"
	)

	version := 1
	eventIDValue := eventID

	for name, tc := range map[string]struct {
		schema string
		event  any
	}{
		"task.created, optional fields absent": {
			schema: "TaskCreatedEvent",
			event:  events.TaskCreatedEvent{Id: id, Title: "write", CreatedAt: time.Now().UTC()},
		},
		"task.created, optional fields present": {
			schema: "TaskCreatedEvent",
			event: events.TaskCreatedEvent{
				Id: id, Title: "write", CreatedAt: time.Now().UTC(), EventId: &eventIDValue, Version: &version,
			},
		},
		// The zero values of the required fields: a tag with omitempty on
		// one of them would drop it here.
		"task.updated, zero values": {schema: "TaskUpdatedEvent", event: events.TaskUpdatedEvent{}},
		"task.deleted, zero values": {schema: "TaskDeletedEvent", event: events.TaskDeletedEvent{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			doc, err := json.Marshal(tc.event)
			require.NoError(t, err)

			contract.LoadJSONSchema(t, "../../../api/jsonschema/"+tc.schema+".json").Validate(t, doc)
		})
	}
}
