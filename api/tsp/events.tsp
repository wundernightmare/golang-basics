import "@typespec/json-schema";

using TypeSpec.JsonSchema;

/**
 * Events — the Kafka contract between services/tasks (producer) and
 * services/consumer (consumer). JSON on the wire, one schema per event,
 * generated Go types in libs/contracts/events shared by both sides, so the
 * shape can no longer drift as two hand-maintained structs.
 *
 * Compatibility rule: a field may be added as optional; a field may never be
 * removed or change type. `oasdiff`-style breaking-change detection is done
 * on the emitted schemas in CI (`just contracts-check`).
 */
@jsonSchema
namespace TasksEvents;

/** Published to the `tasks.events` topic when a task is created; key = task id. */
// The argument is the schema's $id (and file name): go-jsonschema names the Go
// type after it, so it is the type name, not a kebab-case file name.
@jsonSchema("TaskCreatedEvent")
model TaskCreatedEvent {
  /** Task identifier (UUID), also the record key. */
  id: string;
  /** Task title at creation time. */
  title: string;
  /** Creation time, RFC 3339 UTC. */
  created_at: utcDateTime;
}
